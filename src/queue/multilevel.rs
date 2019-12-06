// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A multilevel feedback task queue.

use super::{LocalQueue, Pop, TaskCell, TaskInjector};
use crate::runner::{LocalSpawn, Runner, RunnerBuilder};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use dashmap::DashMap;
use init_with::InitWith;
use rand::prelude::*;
use std::cell::Cell;
use std::cmp;
use std::iter;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

const LEVEL_NUM: usize = 3;
const CHANCE_RATIO: u32 = 4;
const DEFAULT_CLEANUP_OLD_MAP_INTERVAL: Duration = Duration::from_secs(10);
const ADJUST_CHANCE_THRESHOLD: u64 = 1_000_000;

// The chance valus below are the numerators of fractions with u32::max_value()
// as the denominator.
const INIT_LEVEL0_CHANCE: u32 = 3435973836; // 0.8
const MIN_LEVEL0_CHANCE: u32 = 1 << 31; // 0.5
const MAX_LEVEL0_CHANCE: u32 = 4209067949; // 0.98
const ADJUST_AMOUNT: u32 = (MAX_LEVEL0_CHANCE - MIN_LEVEL0_CHANCE) / 8; // 0.06

/// The injector of a multilevel task queue.
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
    /// Creates a [`MultilevelQueueExtras`] with custom parameters.
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

/// The local queue of a multilevel task queue.
pub struct MultilevelQueueLocal<T> {
    local_queue: Worker<T>,
    level_injectors: Arc<[Injector<T>; LEVEL_NUM]>,
    stealers: Vec<Stealer<T>>,
    self_index: usize,
    manager: Arc<LevelManager>,
    rng: SmallRng,
}

impl<T> MultilevelQueueLocal<T> {}

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
                .gen_ratio(self.manager.get_level0_chance(), u32::max_value())
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

/// The runner builder for multilevel task queues.
pub struct MultilevelRunnerBuilder<B> {
    inner: B,
    manager: Arc<LevelManager>,
}

impl<B, R, S, T> RunnerBuilder for MultilevelRunnerBuilder<B>
where
    B: RunnerBuilder<Runner = R>,
    R: Runner<Spawn = S>,
    S: LocalSpawn<TaskCell = T>,
    T: TaskCell<Extras = MultilevelQueueExtras>,
{
    type Runner = MultilevelRunner<R>;

    fn build(&mut self) -> Self::Runner {
        MultilevelRunner {
            inner: self.inner.build(),
            manager: self.manager.clone(),
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
        if current_total > ADJUST_CHANCE_THRESHOLD {
            self.manager.maybe_adjust_chance();
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
    level0_chance: AtomicU32,
    level0_proportion_target: f64,
    adjusting: AtomicBool,
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
                let running_time = running_time.as_duration();
                self.level_time_threshold
                    .iter()
                    .enumerate()
                    .find(|(_, &threshold)| running_time < threshold)
                    .map(|(level, _)| level)
                    .unwrap_or(LEVEL_NUM - 1) as u8
            }
        };
        extras.current_level = current_level;
        extras.schedule_time = Some(now());
    }

    fn get_level0_chance(&self) -> u32 {
        self.level0_chance.load(SeqCst)
    }

    fn maybe_adjust_chance(&self) {
        if self.adjusting.compare_and_swap(false, true, SeqCst) {
            return;
        }
        // The statistics may be not so accurate because we cannot load two
        // atomics at the same time.
        let level0 = self.level0_elapsed.0.load(SeqCst);
        let total = self.total_elapsed.0.load(SeqCst);
        let current_proportion = level0 as f64 / total as f64;
        let diff = self.level0_proportion_target - current_proportion;
        let level0_chance = self.level0_chance.load(SeqCst);
        let new_chance = if diff > 0.05 {
            cmp::min(
                level0_chance.saturating_add(ADJUST_AMOUNT),
                MAX_LEVEL0_CHANCE,
            )
        } else if diff < -0.05 {
            cmp::max(level0_chance - ADJUST_AMOUNT, MIN_LEVEL0_CHANCE)
        } else {
            level0_chance
        };
        self.level0_chance.store(new_chance, SeqCst);
        self.adjusting.store(false, SeqCst);
    }
}

#[derive(Default, Debug)]
struct ElapsedTime(AtomicU64);

impl ElapsedTime {
    fn as_duration(&self) -> Duration {
        Duration::from_micros(self.0.load(SeqCst))
    }

    fn inc_by(&self, t: Duration) -> u64 {
        self.0.fetch_add(t.as_micros() as u64, SeqCst)
    }
}

thread_local!(static TLS_LAST_CLEANUP_TIME: Cell<Instant> = Cell::new(Instant::now()));

struct TaskElapsedMap {
    new_index: AtomicUsize,
    maps: [DashMap<u64, Arc<ElapsedTime>>; 2],
    cleanup_interval: Duration,
    last_cleanup_time: RwLock<Instant>,
    cleaning_up: AtomicBool,
}

impl Default for TaskElapsedMap {
    fn default() -> TaskElapsedMap {
        TaskElapsedMap::new(DEFAULT_CLEANUP_OLD_MAP_INTERVAL)
    }
}

impl TaskElapsedMap {
    fn new(cleanup_interval: Duration) -> TaskElapsedMap {
        TaskElapsedMap {
            new_index: AtomicUsize::new(0),
            maps: Default::default(),
            cleanup_interval: cleanup_interval,
            last_cleanup_time: RwLock::new(now()),
            cleaning_up: AtomicBool::new(false),
        }
    }

    fn get_elapsed(&self, key: u64) -> Arc<ElapsedTime> {
        let new_index = self.new_index.load(SeqCst);
        let new_map = &self.maps[new_index];
        let old_map = &self.maps[new_index ^ 1];
        if let Some(v) = new_map.get(&key) {
            return v.clone();
        }
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
        TLS_LAST_CLEANUP_TIME.with(|t| {
            if recent().saturating_duration_since(t.get()) > self.cleanup_interval {
                self.maybe_cleanup();
            }
        });
        elapsed
    }

    fn maybe_cleanup(&self) {
        let last_cleanup_time = *self.last_cleanup_time.read().unwrap();
        let do_cleanup = recent().saturating_duration_since(last_cleanup_time)
            > self.cleanup_interval
            && !self.cleaning_up.compare_and_swap(false, true, SeqCst);
        let last_cleanup_time = if do_cleanup {
            let old_index = self.new_index.load(SeqCst) ^ 1;
            self.maps[old_index].clear();
            self.new_index.store(old_index, SeqCst);
            let now = now();
            *self.last_cleanup_time.write().unwrap() = now;
            self.cleaning_up.store(false, SeqCst);
            now
        } else {
            last_cleanup_time
        };
        TLS_LAST_CLEANUP_TIME.with(|t| {
            t.set(last_cleanup_time);
        });
    }
}

/// The config of multilevel task queues.
pub struct Config {
    level_time_threshold: [Duration; LEVEL_NUM - 1],
    level0_proportion_target: f64,
}

impl Config {
    #[inline]
    pub fn level_time_threshold(mut self, value: [Duration; LEVEL_NUM - 1]) -> Self {
        self.level_time_threshold = value;
        self
    }

    #[inline]
    pub fn level0_proportion_target(mut self, value: f64) -> Self {
        self.level0_proportion_target = value;
        self
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            level_time_threshold: [Duration::from_millis(5), Duration::from_millis(100)],
            level0_proportion_target: 0.8,
        }
    }
}

/// The builder of a multilevel task queue.
pub struct Builder {
    manager: Arc<LevelManager>,
}

impl Builder {
    /// Creates a multilevel task queue builder from the config.
    pub fn new(config: Config) -> Builder {
        let manager = Arc::new(LevelManager {
            level0_elapsed: Default::default(),
            total_elapsed: Default::default(),
            task_elapsed_map: Default::default(),
            level_time_threshold: config.level_time_threshold,
            level0_chance: AtomicU32::new(INIT_LEVEL0_CHANCE),
            level0_proportion_target: config.level0_proportion_target,
            adjusting: AtomicBool::new(false),
        });
        Builder { manager }
    }

    /// Creates a runner builder for the multilevel task queue with a normal runner builder.
    pub fn runner_builder<B>(&self, inner_runner_builder: B) -> MultilevelRunnerBuilder<B> {
        MultilevelRunnerBuilder {
            inner: inner_runner_builder,
            manager: self.manager.clone(),
        }
    }

    /// Creates the injector and local queues of the multilevel task queue.
    pub fn build<T>(
        self,
        local_num: usize,
    ) -> (MultilevelQueueInjector<T>, Vec<MultilevelQueueLocal<T>>) {
        let level_injectors: Arc<[Injector<T>; LEVEL_NUM]> =
            Arc::new(InitWith::init_with(|| Injector::new()));
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
                manager: self.manager.clone(),
                rng: SmallRng::from_rng(thread_rng()).unwrap(),
            })
            .collect();

        (
            MultilevelQueueInjector {
                level_injectors,
                manager: self.manager,
            },
            local_queues,
        )
    }
}

thread_local!(static RECENT_NOW: Cell<Instant> = Cell::new(Instant::now()));

fn now() -> Instant {
    let res = Instant::now();
    RECENT_NOW.with(|r| r.set(res));
    res
}

fn recent() -> Instant {
    RECENT_NOW.with(|r| r.get())
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::thread;

    #[test]
    fn test_task_elapsed_map_increase() {
        let map = TaskElapsedMap::default();
        map.get_elapsed(1).inc_by(Duration::from_secs(1));
        map.get_elapsed(2).inc_by(Duration::from_millis(500));
        map.get_elapsed(1).inc_by(Duration::from_millis(500));
        assert_eq!(
            map.get_elapsed(1).as_duration(),
            Duration::from_millis(1500)
        );
        assert_eq!(map.get_elapsed(2).as_duration(), Duration::from_millis(500));
    }

    #[test]
    fn test_task_elapsed_map_cleanup() {
        let map = TaskElapsedMap::new(Duration::from_millis(200));
        map.get_elapsed(1).inc_by(Duration::from_secs(1));

        // Trigger a cleanup
        thread::sleep(Duration::from_millis(200));
        now(); // Update recent now
        map.get_elapsed(2).inc_by(Duration::from_secs(1));
        // After one cleanup, we can still read the old stats
        assert_eq!(map.get_elapsed(1).as_duration(), Duration::from_secs(1));

        // Trigger a cleanup
        thread::sleep(Duration::from_millis(200));
        now();
        map.get_elapsed(2).inc_by(Duration::from_secs(1));
        // Trigger another cleanup
        thread::sleep(Duration::from_millis(200));
        now();
        map.get_elapsed(2).inc_by(Duration::from_secs(1));
        assert_eq!(map.get_elapsed(2).as_duration(), Duration::from_secs(3));

        // After two cleanups, we won't be able to read the old stats with id = 1
        assert_eq!(map.get_elapsed(1).as_duration(), Duration::from_secs(0));
    }
}
