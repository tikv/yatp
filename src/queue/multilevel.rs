// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A multilevel feedback task queue.

use super::{LocalQueue, Pop, TaskCell, TaskInjector};

use crossbeam_deque::{Injector, Steal};
use crossbeam_epoch::{self, Atomic};
use dashmap::DashMap;
use init_with::InitWith;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{
    AtomicPtr, AtomicU32, AtomicU64, AtomicU8, AtomicUsize,
    Ordering::{Relaxed, SeqCst},
};
use std::sync::Arc;
use std::time::{Duration, Instant};

const LEVEL_NUM: usize = 3;
const ADJUST_RATIO_INTERVAL: Duration = Duration::from_secs(1);
const CLEANUP_OLD_MAP_THRESHOLD: u64 = 100_000;

pub struct MultilevelQueueInjector<T> {
    level_injectors: Arc<[Injector<T>; LEVEL_NUM]>,
    level_time_threshold: [Duration; LEVEL_NUM - 1],
    task_elapsed_map: TaskElapsedMap,
}

impl<T> Clone for MultilevelQueueInjector<T> {
    fn clone(&self) -> Self {
        Self {
            level_injectors: self.level_injectors.clone(),
            level_time_threshold: self.level_time_threshold,
            task_elapsed_map: self.task_elapsed_map.clone(),
        }
    }
}

/// The extras for the task cells pushed into a multilevel task queue.
///
/// [`Default::default`] can be used to create a [`MultilevelQueueExtras`] for
/// a task cell.
#[derive(Debug, Default, Clone)]
pub struct MultilevelQueueExtras {
    /// The instant when the task cell is pushed to the queue.
    schedule_time: Option<Instant>,
    running_time: TaskElapsed,
    current_level: u8,
    fixed_level: Option<u8>,
}

fn set_schedule_time<T>(task_cell: &mut T)
where
    T: TaskCell<Extras = MultilevelQueueExtras>,
{
    task_cell.mut_extras().schedule_time = Some(Instant::now());
}

impl<T> TaskInjector for MultilevelQueueInjector<T>
where
    T: TaskCell<Extras = MultilevelQueueExtras>,
{
    type TaskCell = T;

    fn push(&self, mut task_cell: Self::TaskCell) {
        let extras = task_cell.mut_extras();
        let push_level = match extras.fixed_level {
            Some(level) => level as usize,
            None => {
                let mut push_level = LEVEL_NUM - 1;
                let running_time = extras.running_time.get();
                for (level, &threshold) in self.level_time_threshold.iter().enumerate() {
                    if running_time < threshold {
                        push_level = level;
                        break;
                    }
                }
                push_level
            }
        };
        task_cell.mut_extras().current_level = push_level as u8;
        set_schedule_time(&mut task_cell);
        self.level_injectors[push_level].push(task_cell);
    }
}

// pub struct MultilevelQueue<T> {
//     injectors: [Injector<T>; LEVEL_NUM],
//     level_elapsed: LevelElapsed,
//     task_elapsed_map: TaskElapsedMap,
//     level_chance_ratio: LevelChanceRatio,
//     target_ratio: TargetRatio,
// }

// impl<Task, T: AsRef<MultiLevelTask<Task>>> MultiLevelQueue<Task, T> {
//     pub fn new() -> Self {
//         Self {
//             injectors: <[Injector<SchedUnit<T>>; LEVEL_NUM]>::init_with(|| Injector::new()),
//             level_elapsed: LevelElapsed::default(),
//             task_elapsed_map: TaskElapsedMap::new(),
//             level_chance_ratio: LevelChanceRatio::default(),
//             target_ratio: TargetRatio::default(),
//             _phantom: PhantomData,
//         }
//     }

//     pub fn create_task(
//         &self,
//         task: Task,
//         task_id: u64,
//         fixed_level: Option<u8>,
//     ) -> MultiLevelTask<Task> {
//         let elapsed = self.task_elapsed_map.get_elapsed(task_id);
//         let level = fixed_level.unwrap_or_else(|| self.expected_level(elapsed.get()));
//         MultiLevelTask {
//             task,
//             elapsed,
//             level: AtomicU8::new(level),
//             fixed_level,
//         }
//     }

//     fn expected_level(&self, elapsed: Duration) -> u8 {
//         match elapsed.as_micros() {
//             0..=999 => 0,
//             1000..=29_999 => 1,
//             _ => 2,
//         }
//     }

//     pub fn target_ratio(&self) -> TargetRatio {
//         self.target_ratio.clone()
//     }

//     pub fn async_adjust_level_ratio(&self) -> impl Future<Output = ()> {
//         let level_elapsed = self.level_elapsed.clone();
//         let level_chance_ratio = self.level_chance_ratio.clone();
//         let target_ratio = self.target_ratio.clone();
//         async move {
//             let mut last_elapsed = level_elapsed.load_all();
//             loop {
//                 Delay::new(ADJUST_RATIO_INTERVAL).await;
//                 let curr_elapsed = level_elapsed.load_all();
//                 let diff_elapsed = <[f32; LEVEL_NUM]>::init_with_indices(|i| {
//                     (curr_elapsed[i] - last_elapsed[i]) as f32
//                 });
//                 last_elapsed = curr_elapsed;

//                 let sum: f32 = diff_elapsed.iter().sum();
//                 if sum == 0.0 {
//                     continue;
//                 }
//                 let curr_l0_ratio = diff_elapsed[0] / sum;
//                 let target_l0_ratio = target_ratio.get_l0_target();
//                 if curr_l0_ratio > target_l0_ratio + 0.05 {
//                     let ratio01 = level_chance_ratio.0[0].load(SeqCst) as f32;
//                     let new_ratio01 = u32::min((ratio01 * 1.6).round() as u32, MAX_L0_CHANCE_RATIO);
//                     level_chance_ratio.0[0].store(new_ratio01, SeqCst);
//                 } else if curr_l0_ratio < target_l0_ratio - 0.05 {
//                     let ratio01 = level_chance_ratio.0[0].load(SeqCst) as f32;
//                     let new_ratio01 = u32::max((ratio01 / 1.6).round() as u32, MIN_L0_CHANCE_RATIO);
//                     level_chance_ratio.0[0].store(new_ratio01, SeqCst);
//                 }
//             }
//         }
//     }

//     pub fn async_cleanup_stats(&self) -> impl Future<Output = ()> {
//         self.task_elapsed_map.async_cleanup()
//     }
// }

// impl<Task, T: AsRef<MultiLevelTask<Task>>> GlobalQueue for MultiLevelQueue<Task, T> {
//     type Task = T;

//     fn steal_batch_and_pop(
//         &self,
//         local_queue: &crossbeam_deque::Worker<SchedUnit<T>>,
//     ) -> Steal<SchedUnit<T>> {
//         let level = self.level_chance_ratio.rand_level();
//         match self.injectors[level].steal_batch_and_pop(local_queue) {
//             s @ Steal::Success(_) | s @ Steal::Retry => return s,
//             _ => {}
//         }
//         for queue in self
//             .injectors
//             .iter()
//             .skip(level + 1)
//             .chain(&self.injectors)
//             .take(LEVEL_NUM)
//         {
//             match queue.steal_batch_and_pop(local_queue) {
//                 s @ Steal::Success(_) | s @ Steal::Retry => return s,
//                 _ => {}
//             }
//         }
//         Steal::Empty
//     }

//     fn push(&self, task: SchedUnit<T>) {
//         let multi_level_task = task.task.as_ref();
//         let elapsed = multi_level_task.elapsed.get();
//         let level = multi_level_task
//             .fixed_level
//             .unwrap_or_else(|| self.expected_level(elapsed));
//         multi_level_task.level.store(level, SeqCst);
//         self.injectors[level as usize].push(task);
//     }
// }

// const MAX_L0_CHANCE_RATIO: u32 = 256;
// const MIN_L0_CHANCE_RATIO: u32 = 1;

// /// The i-th value represents the chance ratio of L_i and Sum[L_k] (k > i).
// #[derive(Clone)]
// struct LevelChanceRatio(Arc<[AtomicU32; LEVEL_NUM - 1]>);

// impl Default for LevelChanceRatio {
//     fn default() -> Self {
//         Self(Arc::new([AtomicU32::new(32), AtomicU32::new(4)]))
//     }
// }

// impl LevelChanceRatio {
//     fn rand_level(&self) -> usize {
//         let mut rng = thread_rng();
//         for (level, ratio) in self.0.iter().enumerate() {
//             let ratio = ratio.load(Relaxed);
//             if rng.gen_ratio(ratio, ratio.saturating_add(1)) {
//                 return level;
//             }
//         }
//         return LEVEL_NUM - 1;
//     }
// }

/// The expected time percentage used by L0 tasks.
#[derive(Clone)]
pub struct TargetRatio(Arc<AtomicU8>);

impl Default for TargetRatio {
    fn default() -> Self {
        Self(Arc::new(AtomicU8::new(80)))
    }
}

impl TargetRatio {
    pub fn set_l0_target(&self, ratio: f32) {
        assert!(ratio >= 0.0 && ratio <= 1.0);
        self.0.store((100.0 * ratio) as u8, Relaxed);
    }

    pub fn get_l0_target(&self) -> f32 {
        self.0.load(Relaxed) as f32 / 100.0
    }
}

#[derive(Clone, Default)]
pub struct LevelElapsed(Arc<[AtomicU64; LEVEL_NUM]>);

impl LevelElapsed {
    pub fn inc_level_by(&self, level: u8, t: Duration) {
        self.0[level as usize].fetch_add(t.as_micros() as u64, Relaxed);
    }

    fn load_all(&self) -> [u64; LEVEL_NUM] {
        <[u64; LEVEL_NUM]>::init_with_indices(|i| self.0[i].load(Relaxed))
    }
}

#[derive(Clone, Default, Debug)]
pub struct TaskElapsed(Arc<AtomicU64>);

impl TaskElapsed {
    pub fn get(&self) -> Duration {
        Duration::from_micros(self.0.load(Relaxed))
    }

    pub fn inc_by(&self, t: Duration) {
        self.0.fetch_add(t.as_micros() as u64, Relaxed);
    }
}

#[derive(Clone)]
struct TaskElapsedMap {
    inner: Arc<TaskElapsedMapInner>,
}

#[derive(Default)]
struct TaskElapsedMapInner {
    counter: AtomicU64,
    new_index: AtomicUsize,
    maps: [DashMap<u64, TaskElapsed>; 2],
}

impl TaskElapsedMap {
    pub fn new() -> Self {
        TaskElapsedMap {
            inner: Arc::new(Default::default()),
        }
    }

    pub fn get_elapsed(&self, key: u64) -> TaskElapsed {
        let inner = &self.inner;
        let new_index = inner.new_index.load(SeqCst);
        let new_map = &inner.maps[new_index];
        let old_map = &inner.maps[new_index ^ 1];
        if let Some(v) = new_map.get(&key) {
            return v.clone();
        }
        inner.counter.fetch_add(1, Relaxed);
        let elapsed = match old_map.remove(&key) {
            Some((_, v)) => {
                new_map.insert(key, v.clone());
                v
            }
            _ => {
                let v = new_map.get_or_insert(&key, TaskElapsed::default());
                v.clone()
            }
        };
        self.maybe_cleanup();
        elapsed
    }

    fn maybe_cleanup(&self) {
        let inner = &self.inner;
        let mut old_counter = inner.counter.load(Relaxed);
        while old_counter > CLEANUP_OLD_MAP_THRESHOLD {
            match inner
                .counter
                .compare_exchange_weak(old_counter, 0, SeqCst, Relaxed)
            {
                Ok(_) => {
                    let old_index = inner.new_index.fetch_xor(1, SeqCst);
                    inner.maps[old_index].clear();
                }
                Err(v) => old_counter = v,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_map_cleanup() {
        use std::thread;
        let map = TaskElapsedMap::new();
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
