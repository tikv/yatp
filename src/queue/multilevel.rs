// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A multilevel feedback task queue.

use super::{LocalQueue, Pop, TaskCell, TaskInjector};
use crate::runner::{LocalSpawn, Runner, RunnerBuilder};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use crossbeam_epoch::{self, Atomic};
use dashmap::DashMap;
use init_with::InitWith;
use rand::prelude::*;
use std::cell::UnsafeCell;
use std::future::Future;
use std::iter;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{
    AtomicPtr, AtomicU32, AtomicU64, AtomicU8, AtomicUsize,
    Ordering::{Relaxed, SeqCst},
};
use std::sync::Arc;
use std::time::{Duration, Instant};

const LEVEL_NUM: usize = 3;
const CHANCE_RATIO: u32 = 4;
const CLEANUP_OLD_MAP_THRESHOLD: u64 = 100_000;
const ADJUST_POSSIBILITY_THRESHOLD: u64 = 2_000_000;

pub struct MultilevelQueueInjector<T> {
    level_injectors: Arc<[Injector<T>; LEVEL_NUM]>,
    manager: Arc<LevelManager>,
}

impl<T> Clone for MultilevelQueueInjector<T> {
    fn clone(&self) -> Self {
        Self {
            level_injectors: self.level_injectors.clone(),
            manager: self.manager.clone(),
        }
    }
}

/// The extras for the task cells pushed into a multilevel task queue.
///
/// [`Default::default`] can be used to create a [`MultilevelQueueExtras`] for
/// a task cell.
#[derive(Debug, Default, Clone)]
pub struct MultilevelQueueExtras {
    task_id: u64,
    /// The instant when the task cell is pushed to the queue.
    schedule_time: Option<Instant>,
    running_time: Option<Arc<ElapsedTime>>,
    current_level: u8,
    fixed_level: Option<u8>,
}

impl MultilevelQueueExtras {
    pub fn new(task_id: u64, fixed_level: Option<u8>) -> Self {
        Self {
            task_id,
            schedule_time: None,
            running_time: None,
            current_level: 0,
            fixed_level,
        }
    }
}

impl<T> TaskInjector for MultilevelQueueInjector<T>
where
    T: TaskCell<Extras = MultilevelQueueExtras>,
{
    type TaskCell = T;

    fn push(&self, mut task_cell: Self::TaskCell) {
        self.manager.prepare_before_push(&mut task_cell);
        let level = task_cell.mut_extras().current_level as usize;
        self.level_injectors[level].push(task_cell);
    }
}

pub struct MultilevelQueueLocal<T> {
    local_queue: Worker<T>,
    level_injectors: Arc<[Injector<T>; LEVEL_NUM]>,
    stealers: Vec<Stealer<T>>,
    self_index: usize,
    manager: Arc<LevelManager>,
    rng: SmallRng,
}

impl<T> LocalQueue for MultilevelQueueLocal<T>
where
    T: TaskCell<Extras = MultilevelQueueExtras>,
{
    type TaskCell = T;

    fn push(&mut self, mut task_cell: Self::TaskCell) {
        self.manager.prepare_before_push(&mut task_cell);
        self.local_queue.push(task_cell);
    }

    fn pop(&mut self) -> Option<Pop<Self::TaskCell>> {
        fn into_pop<T>(mut t: T, from_local: bool) -> Pop<T>
        where
            T: TaskCell<Extras = MultilevelQueueExtras>,
        {
            let schedule_time = t.mut_extras().schedule_time.unwrap();
            Pop {
                task_cell: t,
                schedule_time,
                from_local,
            }
        }

        if let Some(t) = self.local_queue.pop() {
            return Some(into_pop(t, true));
        }
        let mut need_retry;
        loop {
            need_retry = false;
            let expected_level = if self
                .rng
                .gen_ratio(self.manager.get_level0_possibility(), u32::max_value())
            {
                0
            } else {
                (1..LEVEL_NUM - 1)
                    .find(|_| self.rng.gen_ratio(CHANCE_RATIO, CHANCE_RATIO + 1))
                    .unwrap_or(LEVEL_NUM - 1)
            };
            match self.level_injectors[expected_level].steal_batch_and_pop(&self.local_queue) {
                Steal::Success(t) => return Some(into_pop(t, false)),
                Steal::Retry => need_retry = true,
                _ => {}
            }
            for (i, stealer) in self.stealers.iter().enumerate() {
                if i == self.self_index {
                    continue;
                }
                match stealer.steal_batch_and_pop(&self.local_queue) {
                    Steal::Success(t) => return Some(into_pop(t, false)),
                    Steal::Retry => need_retry = true,
                    _ => {}
                }
            }
            for injector in self
                .level_injectors
                .iter()
                .chain(&*self.level_injectors)
                .skip(expected_level + 1)
                .take(LEVEL_NUM - 1)
            {
                match injector.steal_batch_and_pop(&self.local_queue) {
                    Steal::Success(t) => return Some(into_pop(t, false)),
                    Steal::Retry => need_retry = true,
                    _ => {}
                }
            }
            if !need_retry {
                break None;
            }
        }
    }
}

pub struct MultilevelRunner<R> {
    inner: R,
    manager: Arc<LevelManager>,
}

impl<R, S, T> Runner for MultilevelRunner<R>
where
    R: Runner<Spawn = S>,
    S: LocalSpawn<TaskCell = T>,
    T: TaskCell<Extras = MultilevelQueueExtras>,
{
    type Spawn = R::Spawn;

    fn start(&mut self, spawn: &mut Self::Spawn) {
        self.inner.start(spawn)
    }

    fn handle(
        &mut self,
        spawn: &mut Self::Spawn,
        mut task_cell: <Self::Spawn as LocalSpawn>::TaskCell,
    ) -> bool {
        let extras = task_cell.mut_extras();
        let running_time = extras.running_time.clone();
        let level = extras.current_level;
        let begin = Instant::now();
        let res = self.inner.handle(spawn, task_cell);
        let elapsed = begin.elapsed();
        if let Some(running_time) = running_time {
            running_time.inc_by(elapsed);
        }
        if level == 0 {
            self.manager.level0_elapsed.inc_by(elapsed);
        }
        let current_total = self.manager.total_elapsed.inc_by(elapsed) + elapsed.as_micros() as u64;
        if current_total > ADJUST_POSSIBILITY_THRESHOLD {
            self.manager.maybe_adjust_possibility(current_total);
        }
        res
    }

    fn pause(&mut self, spawn: &mut Self::Spawn) -> bool {
        self.inner.pause(spawn)
    }

    fn resume(&mut self, spawn: &mut Self::Spawn) {
        self.inner.resume(spawn)
    }

    fn end(&mut self, spawn: &mut Self::Spawn) {
        self.inner.end(spawn)
    }
}

struct LevelManager {
    level0_elapsed: ElapsedTime,
    total_elapsed: ElapsedTime,
    task_elapsed_map: TaskElapsedMap,
    level_time_threshold: [Duration; LEVEL_NUM - 1],
    level0_possibility: UnsafeCell<u32>,
    level0_proportion_target: f64,
}

impl LevelManager {
    fn prepare_before_push<T>(&self, task_cell: &mut T)
    where
        T: TaskCell<Extras = MultilevelQueueExtras>,
    {
        let extras = task_cell.mut_extras();
        let task_id = extras.task_id;
        let running_time = extras
            .running_time
            .get_or_insert_with(|| self.task_elapsed_map.get_elapsed(task_id));
        let current_level = match extras.fixed_level {
            Some(level) => level,
            None => {
                let running_time = running_time.get();
                self.level_time_threshold
                    .iter()
                    .enumerate()
                    .find(|(_, &threshold)| running_time < threshold)
                    .map(|(level, _)| level)
                    .unwrap_or(LEVEL_NUM - 1) as u8
            }
        };
        extras.current_level = current_level;
        extras.schedule_time = Some(Instant::now());
    }

    fn get_level0_possibility(&self) -> u32 {
        unsafe { *self.level0_possibility.get() }
    }

    fn maybe_adjust_possibility(&self, mut current_total: u64) {}
}

#[derive(Default, Debug)]
struct ElapsedTime(AtomicU64);

impl ElapsedTime {
    fn get(&self) -> Duration {
        Duration::from_micros(self.0.load(Relaxed))
    }

    fn inc_by(&self, t: Duration) -> u64 {
        self.0.fetch_add(t.as_micros() as u64, Relaxed)
    }
}

#[derive(Default)]
struct TaskElapsedMap {
    counter: AtomicU64,
    new_index: AtomicUsize,
    maps: [DashMap<u64, Arc<ElapsedTime>>; 2],
}

impl TaskElapsedMap {
    pub fn get_elapsed(&self, key: u64) -> Arc<ElapsedTime> {
        let new_index = self.new_index.load(SeqCst);
        let new_map = &self.maps[new_index];
        let old_map = &self.maps[new_index ^ 1];
        if let Some(v) = new_map.get(&key) {
            return v.clone();
        }
        let counter_value = self.counter.fetch_add(1, Relaxed);
        let elapsed = match old_map.remove(&key) {
            Some((_, v)) => {
                new_map.insert(key, v.clone());
                v
            }
            _ => {
                let v = new_map.get_or_insert_with(&key, Default::default);
                v.clone()
            }
        };
        if counter_value > CLEANUP_OLD_MAP_THRESHOLD {
            self.maybe_cleanup(counter_value + 1);
        }
        elapsed
    }

    fn maybe_cleanup(&self, mut counter_value: u64) {
        while counter_value > CLEANUP_OLD_MAP_THRESHOLD {
            match self
                .counter
                .compare_exchange_weak(counter_value, 0, SeqCst, Relaxed)
            {
                Ok(_) => {
                    let old_index = self.new_index.fetch_xor(1, SeqCst);
                    self.maps[old_index].clear();
                }
                Err(v) => counter_value = v,
            }
        }
    }
}

/// Creates a multilevel task queue.
pub fn create<T>(
    local_num: usize,
    level_time_threshold: [Duration; LEVEL_NUM - 1],
    level0_possibility: u32,
    level0_proportion_target: f64,
) -> (MultilevelQueueInjector<T>, Vec<MultilevelQueueLocal<T>>) {
    let level_injectors: Arc<[Injector<T>; LEVEL_NUM]> =
        Arc::new(InitWith::init_with(|| Injector::new()));
    let manager = Arc::new(LevelManager {
        level0_elapsed: Default::default(),
        total_elapsed: Default::default(),
        task_elapsed_map: Default::default(),
        level_time_threshold,
        level0_possibility,
        level0_proportion_target,
    });
    let workers: Vec<_> = iter::repeat_with(Worker::new_lifo)
        .take(local_num)
        .collect();
    let stealers: Vec<_> = workers.iter().map(Worker::stealer).collect();
    let local_queues = workers
        .into_iter()
        .enumerate()
        .map(|(self_index, local_queue)| MultilevelQueueLocal {
            local_queue,
            level_injectors: level_injectors.clone(),
            stealers: stealers.clone(),
            self_index,
            manager: manager.clone(),
            rng: SmallRng::from_rng(thread_rng()).unwrap(),
        })
        .collect();

    (
        MultilevelQueueInjector {
            level_injectors,
            manager,
        },
        local_queues,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_cleanup() {
        use std::thread;
        let map = Arc::new(TaskElapsedMap::default());
        let mut handles = Vec::new();
        for _ in 0..6 {
            let map = map.clone();
            handles.push(thread::spawn(move || {
                for i in 0..u64::max_value() {
                    map.get_elapsed(i);
                }
            }));
        }
        for handle in handles {
            handle.join();
        }
    }
}
