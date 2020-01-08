// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A multilevel feedback task queue. Long-running tasks are pushed to levels
//! with lower priority.
//!
//! The task queue requires that the accompanying [`MultilevelRunner`] must be
//! used to collect necessary information.

use super::{LocalQueueBuilder, Pop, TaskCell};
use crate::pool::{Local, Runner, RunnerBuilder};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use dashmap::DashMap;
use rand::prelude::*;
use std::cell::Cell;
use std::cmp;
use std::iter;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering::SeqCst};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const LEVEL_NUM: usize = 3;

/// The chance ratio of level 1 and level 2 tasks.
const CHANCE_RATIO: u32 = 4;

const DEFAULT_CLEANUP_OLD_MAP_INTERVAL: Duration = Duration::from_secs(10);

/// When total elapsed time exceeds this value, it will try to adjust level
/// chances and reset the total elapsed time.
const ADJUST_CHANCE_INTERVAL: Duration = Duration::from_secs(1);

/// When the deviation between the target and the actual level 0 proportion
/// exceeds this value, level chances need to be adjusted.
const ADJUST_CHANCE_THRESHOLD: f64 = 0.05;

// The chance values below are the numerators of fractions with u32::max_value()
// as the denominator.
const INIT_LEVEL0_CHANCE: u32 = 3_435_973_836; // 0.8
const MIN_LEVEL0_CHANCE: u32 = 1 << 31; // 0.5
const MAX_LEVEL0_CHANCE: u32 = 4_209_067_949; // 0.98
const ADJUST_AMOUNT: u32 = (MAX_LEVEL0_CHANCE - MIN_LEVEL0_CHANCE) / 8; // 0.06

/// The injector of a multilevel task queue.
pub(crate) struct TaskInjector<T> {
    level_injectors: Arc<[Injector<T>; LEVEL_NUM]>,
    manager: Arc<LevelManager>,
}

impl<T> Clone for TaskInjector<T> {
    fn clone(&self) -> Self {
        Self {
            level_injectors: self.level_injectors.clone(),
            manager: self.manager.clone(),
        }
    }
}

impl<T> TaskInjector<T>
where
    T: TaskCell + Send,
{
    pub(super) fn push(&self, mut task_cell: T) {
        self.manager.prepare_before_push(&mut task_cell);
        let level = task_cell.mut_extras().current_level as usize;
        self.level_injectors[level].push(task_cell);
    }
}

/// The local queue of a multilevel task queue.
pub(crate) struct LocalQueue<T> {
    local_queue: Rc<Worker<T>>,
    level_injectors: Arc<[Injector<T>; LEVEL_NUM]>,
    stealers: Vec<Stealer<T>>,
    manager: Arc<LevelManager>,
}

impl<T> LocalQueue<T>
where
    T: TaskCell,
{
    pub(super) fn push(&mut self, mut task_cell: T) {
        self.manager.prepare_before_push(&mut task_cell);
        self.local_queue.push(task_cell);
    }

    pub(super) fn pop(&mut self) -> Option<Pop<T>> {
        fn into_pop<T>(mut t: T, from_local: bool) -> Pop<T>
        where
            T: TaskCell,
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
        let mut rng = thread_rng();
        let mut need_retry = true;
        while need_retry {
            need_retry = false;
            let expected_level =
                if rng.gen_ratio(self.manager.get_level0_chance(), u32::max_value()) {
                    0
                } else {
                    (1..LEVEL_NUM - 1)
                        .find(|_| rng.gen_ratio(CHANCE_RATIO, CHANCE_RATIO + 1))
                        .unwrap_or(LEVEL_NUM - 1)
                };
            match self.level_injectors[expected_level].steal_batch_and_pop(&self.local_queue) {
                Steal::Success(t) => return Some(into_pop(t, false)),
                Steal::Retry => need_retry = true,
                _ => {}
            }
            if !self.stealers.is_empty() {
                let mut found = None;
                for (idx, stealer) in self.stealers.iter().enumerate() {
                    match stealer.steal_batch_and_pop(&self.local_queue) {
                        Steal::Success(t) => {
                            found = Some((idx, into_pop(t, false)));
                            break;
                        }
                        Steal::Retry => need_retry = true,
                        _ => {}
                    }
                }
                if let Some((idx, task)) = found {
                    let last_pos = self.stealers.len() - 1;
                    self.stealers.swap(idx, last_pos);
                    return Some(task);
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
        }
        None
    }

    pub(super) fn local_injector(&self) -> LocalInjector<T> {
        LocalInjector {
            manager: self.manager.clone(),
            worker: self.local_queue.clone(),
        }
    }
}

pub(crate) struct LocalInjector<T> {
    manager: Arc<LevelManager>,
    worker: Rc<Worker<T>>,
}

impl<T: TaskCell> LocalInjector<T> {
    pub(super) fn push(&self, mut task: T) {
        self.manager.prepare_before_push(&mut task);
        self.worker.push(task);
    }
}

/// The runner builder for multilevel task queues.
///
/// It can be created by [`Builder::runner_builder`].
pub struct MultilevelRunnerBuilder<B> {
    inner: B,
    manager: Arc<LevelManager>,
}

impl<B, R, T> RunnerBuilder for MultilevelRunnerBuilder<B>
where
    B: RunnerBuilder<Runner = R>,
    R: Runner<TaskCell = T>,
    T: TaskCell,
{
    type Runner = MultilevelRunner<R>;

    fn build(&mut self) -> Self::Runner {
        MultilevelRunner {
            inner: self.inner.build(),
            manager: self.manager.clone(),
        }
    }
}

/// The runner for multilevel task queues.
///
/// The runner helps multilevel task queues collect additional information.
/// [`MultilevelRunnerBuilder`] is the [`RunnerBuilder`] for this runner.
pub struct MultilevelRunner<R> {
    inner: R,
    manager: Arc<LevelManager>,
}

impl<R, T> Runner for MultilevelRunner<R>
where
    R: Runner<TaskCell = T>,
    T: TaskCell,
{
    type TaskCell = T;

    fn start(&mut self, local: &mut Local<T>) {
        self.inner.start(local)
    }

    fn handle(&mut self, local: &mut Local<T>, mut task_cell: T) -> bool {
        let extras = task_cell.mut_extras();
        let running_time = extras.running_time.clone();
        let level = extras.current_level;
        let begin = Instant::now();
        let res = self.inner.handle(local, task_cell);
        let elapsed = begin.elapsed();
        if let Some(running_time) = running_time {
            running_time.inc_by(elapsed);
        }
        if level == 0 {
            self.manager.level0_elapsed.inc_by(elapsed);
        }
        let current_total = Duration::from_micros(self.manager.total_elapsed.inc_by(elapsed));
        if current_total > ADJUST_CHANCE_INTERVAL {
            self.manager.maybe_adjust_chance();
        }
        res
    }

    fn pause(&mut self, local: &mut Local<T>) -> bool {
        self.inner.pause(local)
    }

    fn resume(&mut self, local: &mut Local<T>) {
        self.inner.resume(local)
    }

    fn end(&mut self, local: &mut Local<T>) {
        self.inner.end(local)
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
        T: TaskCell,
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
        let total = self.total_elapsed.0.load(SeqCst);
        if Duration::from_micros(total) < ADJUST_CHANCE_INTERVAL {
            // Another thread just adjusted the chances.
            return;
        }
        let total = self.total_elapsed.0.swap(0, SeqCst);
        let level0 = self.level0_elapsed.0.swap(0, SeqCst);
        let current_proportion = level0 as f64 / total as f64;
        let diff = self.level0_proportion_target - current_proportion;
        let level0_chance = self.level0_chance.load(SeqCst);
        let new_chance = if diff > ADJUST_CHANCE_THRESHOLD {
            cmp::min(
                level0_chance.saturating_add(ADJUST_AMOUNT),
                MAX_LEVEL0_CHANCE,
            )
        } else if diff < -ADJUST_CHANCE_THRESHOLD {
            cmp::max(level0_chance - ADJUST_AMOUNT, MIN_LEVEL0_CHANCE)
        } else {
            level0_chance
        };
        self.level0_chance.store(new_chance, SeqCst);
        self.adjusting.store(false, SeqCst);
    }
}

#[derive(Default, Debug)]
pub(crate) struct ElapsedTime(AtomicU64);

impl ElapsedTime {
    fn as_duration(&self) -> Duration {
        Duration::from_micros(self.0.load(SeqCst))
    }

    fn inc_by(&self, t: Duration) -> u64 {
        let micros = t.as_micros() as u64;
        self.0.fetch_add(micros, SeqCst) + micros
    }

    #[cfg(test)]
    fn from_duration(dur: Duration) -> Self {
        ElapsedTime(AtomicU64::new(dur.as_micros() as u64))
    }
}

thread_local!(static TLS_LAST_CLEANUP_TIME: Cell<Instant> = Cell::new(Instant::now()));

struct TaskElapsedMap {
    new_index: AtomicUsize,
    maps: [DashMap<u64, Arc<ElapsedTime>>; 2],
    cleanup_interval: Duration,
    last_cleanup_time: Mutex<Instant>,
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
            cleanup_interval,
            last_cleanup_time: Mutex::new(now()),
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
        let elapsed = match old_map.get(&key) {
            Some(v) => {
                new_map.insert(key, v.clone());
                v.clone()
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
        let last_cleanup_time = *self.last_cleanup_time.lock().unwrap();
        let do_cleanup = recent().saturating_duration_since(last_cleanup_time)
            > self.cleanup_interval
            && !self.cleaning_up.compare_and_swap(false, true, SeqCst);
        let last_cleanup_time = if do_cleanup {
            let old_index = self.new_index.load(SeqCst) ^ 1;
            self.maps[old_index].clear();
            self.new_index.store(old_index, SeqCst);
            let now = now();
            *self.last_cleanup_time.lock().unwrap() = now;
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

/// The configurations of multilevel task queues.
pub struct Config {
    level_time_threshold: [Duration; LEVEL_NUM - 1],
    level0_proportion_target: f64,
}

impl Config {
    /// Sets the time threshold of each level. It decides which level a task should be
    /// pushed into.
    #[inline]
    pub fn level_time_threshold(mut self, value: [Duration; LEVEL_NUM - 1]) -> Self {
        self.level_time_threshold = value;
        self
    }

    /// Sets the target proportion of time used by level 0 tasks.
    ///
    /// For example, if the value is set to `0.8`, the queue will try to let level 0
    /// tasks occupy 80% running time.
    ///
    /// The default value is `0.8`.
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
    pub(crate) fn build<T: Send + 'static>(
        self,
        local_num: usize,
    ) -> (super::TaskInjector<T>, Vec<LocalQueueBuilder<T>>) {
        let level_injectors: Arc<[Injector<T>; LEVEL_NUM]> =
            Arc::new([Injector::new(), Injector::new(), Injector::new()]);
        let workers: Vec<_> = iter::repeat_with(Worker::new_lifo)
            .take(local_num)
            .collect();
        let stealers: Vec<_> = workers.iter().map(Worker::stealer).collect();
        let locals = workers
            .into_iter()
            .enumerate()
            .map(
                |(self_index, local_queue)| -> Box<dyn FnOnce() -> super::LocalQueue<T> + Send> {
                    let mut stealers: Vec<_> = stealers
                        .iter()
                        .enumerate()
                        .filter(|(index, _)| *index != self_index)
                        .map(|(_, stealer)| stealer.clone())
                        .collect();
                    // Steal with a random start to avoid imbalance.
                    stealers.shuffle(&mut thread_rng());
                    let manager = self.manager.clone();
                    let level_injectors = level_injectors.clone();
                    Box::new(move || {
                        super::LocalQueue(super::LocalQueueInner::Multilevel(LocalQueue {
                            local_queue: Rc::new(local_queue),
                            level_injectors,
                            stealers,
                            manager,
                        }))
                    })
                },
            )
            .collect();
        (
            super::TaskInjector(super::InjectorInner::Multilevel(TaskInjector {
                level_injectors,
                manager: self.manager,
            })),
            locals,
        )
    }
}

thread_local!(static RECENT_NOW: Cell<Instant> = Cell::new(Instant::now()));

/// Returns an instant corresponding to now and updates the thread-local recent
/// now.
fn now() -> Instant {
    let res = Instant::now();
    RECENT_NOW.with(|r| r.set(res));
    res
}

/// Returns the thread-local recent now. It is used to save the cost of calling
/// `Instant::now` frequently.
///
/// You should only use it when the thread-local recent now is recently updated.
fn recent() -> Instant {
    RECENT_NOW.with(|r| r.get())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::build_spawn;
    use crate::queue::Extras;

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

    #[derive(Debug)]
    struct MockTask {
        sleep_ms: u64,
        extras: Extras,
    }

    impl MockTask {
        fn new(sleep_ms: u64, extras: Extras) -> Self {
            MockTask { sleep_ms, extras }
        }
    }

    impl TaskCell for MockTask {
        fn mut_extras(&mut self) -> &mut Extras {
            &mut self.extras
        }
    }

    struct MockRunner;

    impl Runner for MockRunner {
        type TaskCell = MockTask;

        fn handle(&mut self, _local: &mut Local<MockTask>, task_cell: MockTask) -> bool {
            thread::sleep(Duration::from_millis(task_cell.sleep_ms));
            true
        }
    }

    struct MockRunnerBuilder;

    impl RunnerBuilder for MockRunnerBuilder {
        type Runner = MockRunner;

        fn build(&mut self) -> MockRunner {
            MockRunner
        }
    }

    #[test]
    fn test_schedule_time_is_set() {
        const SLEEP_DUR: Duration = Duration::from_millis(5);

        let builder = Builder::new(Config::default());
        let (injector, locals) = builder.build(1);
        let mut locals: Vec<_> = locals.into_iter().map(|b| b()).collect();
        injector.push(MockTask::new(0, Extras::multilevel_default()));
        thread::sleep(SLEEP_DUR);
        let schedule_time = locals[0].pop().unwrap().schedule_time;
        assert!(schedule_time.elapsed() >= SLEEP_DUR);
    }

    #[test]
    fn test_push_task() {
        let builder = Builder::new(
            Config::default()
                .level_time_threshold([Duration::from_millis(1), Duration::from_millis(100)]),
        );
        let (injector, _) = builder.build(1);
        let injector = injector.into_multilevel();

        // Running time is 50us. It should be pushed to level 0.
        let extras = Extras {
            running_time: Some(Arc::new(ElapsedTime::from_duration(Duration::from_micros(
                50,
            )))),
            ..Extras::multilevel_default()
        };
        injector.push(MockTask::new(1, extras));
        assert_eq!(
            injector.level_injectors[0]
                .steal()
                .success()
                .unwrap()
                .sleep_ms,
            1
        );

        // Running time is 10ms. It should be pushed to level 1.
        let extras = Extras {
            running_time: Some(Arc::new(ElapsedTime::from_duration(Duration::from_millis(
                10,
            )))),
            ..Extras::multilevel_default()
        };
        injector.push(MockTask::new(2, extras));
        assert_eq!(
            injector.level_injectors[1]
                .steal()
                .success()
                .unwrap()
                .sleep_ms,
            2
        );

        // Running time is 1s. It should be pushed to level 2.
        let extras = Extras {
            running_time: Some(Arc::new(ElapsedTime::from_duration(Duration::from_secs(1)))),
            ..Extras::multilevel_default()
        };
        injector.push(MockTask::new(3, extras));
        assert_eq!(
            injector.level_injectors[2]
                .steal()
                .success()
                .unwrap()
                .sleep_ms,
            3
        );

        // Fixed level is set. It should be pushed to the set level.
        let extras = Extras {
            running_time: Some(Arc::new(ElapsedTime::from_duration(Duration::from_secs(1)))),
            fixed_level: Some(1),
            ..Extras::multilevel_default()
        };
        injector.push(MockTask::new(4, extras));
        assert_eq!(
            injector.level_injectors[1]
                .steal()
                .success()
                .unwrap()
                .sleep_ms,
            4
        );
    }

    #[test]
    fn test_pop_by_stealing_injector() {
        let builder = Builder::new(Config::default());
        let (injector, locals) = builder.build(3);
        let injector = injector.into_multilevel();
        let mut locals: Vec<_> = locals.into_iter().map(|b| b()).collect();
        for i in 0..100 {
            injector.push(MockTask::new(i, Extras::multilevel_default()));
        }
        let sum: u64 = (0..100)
            .map(|_| locals[2].pop().unwrap().task_cell.sleep_ms)
            .sum();
        assert_eq!(sum, (0..100).sum());
        assert!(locals.iter_mut().all(|c| c.pop().is_none()));
    }

    #[test]
    fn test_pop_by_steal_others() {
        let builder = Builder::new(Config::default());
        let (injector, locals) = builder.build(3);
        let injector = injector.into_multilevel();
        let mut locals: Vec<_> = locals.into_iter().map(|b| b().into_multilevel()).collect();
        for i in 0..50 {
            injector.push(MockTask::new(i, Extras::multilevel_default()));
        }
        assert!(injector.level_injectors[0]
            .steal_batch(&locals[0].local_queue)
            .is_success());
        for i in 50..100 {
            injector.push(MockTask::new(i, Extras::multilevel_default()));
        }
        assert!(injector.level_injectors[0]
            .steal_batch(&locals[1].local_queue)
            .is_success());
        let sum: u64 = (0..100)
            .map(|_| locals[2].pop().unwrap().task_cell.sleep_ms)
            .sum();
        assert_eq!(sum, (0..100).sum());
        assert!(locals.iter_mut().all(|c| c.pop().is_none()));
    }

    #[test]
    fn test_pop_concurrently() {
        let builder = Builder::new(Config::default());
        let (injector, locals) = builder.build(3);
        for i in 0..10_000 {
            injector.push(MockTask::new(i, Extras::multilevel_default()));
        }
        let sum = Arc::new(AtomicU64::new(0));
        let handles: Vec<_> = locals
            .into_iter()
            .map(|builder| {
                let sum = sum.clone();
                thread::spawn(move || {
                    let mut consumer = builder();
                    while let Some(pop) = consumer.pop() {
                        sum.fetch_add(pop.task_cell.sleep_ms, SeqCst);
                    }
                })
            })
            .collect();
        for handle in handles {
            let _ = handle.join();
        }
        assert_eq!(sum.load(SeqCst), (0..10_000).sum());
    }

    #[test]
    fn test_runner_records_handle_time() {
        let builder = Builder::new(Config::default());
        let mut runner_builder = builder.runner_builder(MockRunnerBuilder);
        let manager = builder.manager.clone();
        let (handle, mut locals) = build_spawn(builder, Default::default());
        let mut runner = runner_builder.build();

        handle.spawn(MockTask::new(100, Extras::new_multilevel(1, None)));
        if let Some(Pop { task_cell, .. }) = locals[0].pop() {
            assert!(runner.handle(&mut locals[0], task_cell));
        }
        assert!(
            manager.task_elapsed_map.get_elapsed(1).as_duration() >= Duration::from_millis(100)
        );
    }

    #[test]
    fn test_adjust_level_chance() {
        // Default level 0 target is 0.8
        let manager = Builder::new(Config::default()).manager;

        // Level 0 running time is lower than expected, level0_chance should
        // increase.
        let level0_chance_before = manager.get_level0_chance();
        manager.level0_elapsed.inc_by(Duration::from_millis(500));
        manager.total_elapsed.inc_by(Duration::from_millis(1500));
        manager.maybe_adjust_chance();
        let level0_chance_after = manager.get_level0_chance();
        assert!(level0_chance_before < level0_chance_after);

        // Level 0 running time is higher than expected, level0_chance should
        // decrease.
        let level0_chance_before = manager.get_level0_chance();
        manager.level0_elapsed.inc_by(Duration::from_millis(1400));
        manager.total_elapsed.inc_by(Duration::from_millis(1500));
        manager.maybe_adjust_chance();
        let level0_chance_after = manager.get_level0_chance();
        assert!(level0_chance_before > level0_chance_after);

        // Level 0 running time is roughly equivalent to expected,
        // level0_chance should not change.
        let level0_chance_before = manager.get_level0_chance();
        manager.level0_elapsed.inc_by(Duration::from_millis(1210));
        manager.total_elapsed.inc_by(Duration::from_millis(1500));
        manager.maybe_adjust_chance();
        let level0_chance_after = manager.get_level0_chance();
        assert_eq!(level0_chance_before, level0_chance_after);
    }
}
