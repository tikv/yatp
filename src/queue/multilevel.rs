// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A multilevel feedback task queue. Long-running tasks are pushed to levels
//! with lower priority.
//!
//! The task queue requires that the accompanying [`MultilevelRunner`] must be
//! used to collect necessary information.

use super::{Pop, TaskCell};
use crate::metrics::*;
use crate::pool::{Local, Runner, RunnerBuilder};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use dashmap::{try_result::TryResult::Present, DashMap};
use fail::fail_point;
use prometheus::local::{LocalHistogram, LocalIntCounter};
use prometheus::{Gauge, Histogram, HistogramOpts, IntCounter};
use rand::prelude::*;
use std::array;
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering::*};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{f64, fmt, iter};

/// Number of levels
pub const LEVEL_NUM: usize = 3;

/// The chance ratio of level 1 and level 2 tasks.
const CHANCE_RATIO: u32 = 4;

/// The default interval for cleaning up task elapsed map.
pub(super) const DEFAULT_CLEANUP_OLD_MAP_INTERVAL: Duration = Duration::from_secs(10);

/// When local total elapsed time exceeds this value in microseconds, the local
/// metrics is flushed to the global atomic metrics and try to trigger chance
/// adjustment.
pub(super) const FLUSH_LOCAL_THRESHOLD_US: u64 = 100_000;

/// When the incremental total elapsed time exceeds this value, it will try to
/// adjust level chances and reset the total elapsed time.
const ADJUST_CHANCE_INTERVAL_US: u64 = 1_000_000;

/// When the deviation between the target and the actual level 0 proportion
/// exceeds this value, level chances need to be adjusted.
const ADJUST_CHANCE_THRESHOLD: f64 = 0.05;

/// The initial chance that a level 0 task is scheduled.
///
/// The value is not so important because the actual chance will be adjusted
/// according to the real-time workload.
const INIT_LEVEL0_CHANCE: f64 = 0.8;

const MIN_LEVEL0_CHANCE: f64 = 0.5;
const MAX_LEVEL0_CHANCE: f64 = 0.98;

/// The amount that the level 0 chance is increased or decreased each time.
const ADJUST_AMOUNT: f64 = 0.06;

/// the default max number of tasks the can steal from each level queue.
const DEFAULT_STEAL_LIMIT_PER_LEVEL: [usize; LEVEL_NUM] = [64, 16, 1];
/// the maximum number of tasks that should steal from global queue for level > 0 queue.
const LEVEL_MAX_QUEUE_MAX_STEAL_SIZE: usize = 16;
/// the number of tasks threshold to trigger adjust level max steal size.
const ADJUST_LEVEL_STEAL_SIZE_THRESHOLD: usize = 100;

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
    local_queue: Worker<T>,
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
        let level0_chance = self.manager.level0_chance.get();
        while need_retry {
            need_retry = false;
            let expected_level = if rng.gen::<f64>() < level0_chance {
                0
            } else {
                (1..LEVEL_NUM - 1)
                    .find(|_| rng.gen_ratio(CHANCE_RATIO, CHANCE_RATIO + 1))
                    .unwrap_or(LEVEL_NUM - 1)
            };
            match self.steal_from_injector(expected_level) {
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
            for l in expected_level + 1..expected_level + LEVEL_NUM {
                match self.steal_from_injector(l % LEVEL_NUM) {
                    Steal::Success(t) => return Some(into_pop(t, false)),
                    Steal::Retry => need_retry = true,
                    _ => {}
                }
            }
        }
        None
    }

    #[inline]
    fn steal_from_injector(&self, level: usize) -> Steal<T> {
        // steal one task from level injector, for all level except the max level, we use a different
        // static max steal size for each level to restrict the max batch size, for the max level, by
        // default we only steal 1 task, but the dynamically adjusts the limit to avoid performance
        // regression when there are only low-priority tasks.
        let steal_limit = if level < LEVEL_NUM - 1 {
            DEFAULT_STEAL_LIMIT_PER_LEVEL[level]
        } else {
            self.manager.max_level_queue_steal_size.load(Relaxed)
        };
        self.level_injectors[level].steal_batch_with_limit_and_pop(&self.local_queue, steal_limit)
    }

    pub fn has_tasks_or_pull(&mut self) -> bool {
        if !self.local_queue.is_empty() {
            return true;
        }

        let mut rng = thread_rng();
        let level0_chance = self.manager.level0_chance.get();
        loop {
            let expected_level = if rng.gen::<f64>() < level0_chance {
                0
            } else {
                (1..LEVEL_NUM - 1)
                    .find(|_| rng.gen_ratio(CHANCE_RATIO, CHANCE_RATIO + 1))
                    .unwrap_or(LEVEL_NUM - 1)
            };
            match self.level_injectors[expected_level].steal_batch(&self.local_queue) {
                Steal::Success(()) => return true,
                Steal::Empty => return false,
                Steal::Retry => {}
            }
        }
    }
}

/// The runner builder for multilevel task queues.
///
/// It can be created by [`Builder::runner_builder`].
pub struct MultilevelRunnerBuilder<B> {
    inner: TrackedRunnerBuilder<B>,
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

/// `TrackedRunner` wraps a runner with some metrics.
pub struct TrackedRunner<R> {
    inner: R,
    local_level0_elapsed_us: LocalIntCounter,
    local_total_elapsed_us: LocalIntCounter,
    task_wait_duration: LocalHistogram,
    task_execute_duration: LocalHistogram,
    task_poll_duration: [LocalHistogram; LEVEL_NUM],
    task_execute_times: LocalHistogram,
    // whether to trigger local metrics flush.
    auto_flush_metrics: bool,
}

impl<R> TrackedRunner<R> {
    pub(super) fn flush(&mut self) {
        self.local_level0_elapsed_us.flush();
        self.local_total_elapsed_us.flush();
        self.task_execute_duration.flush();
        self.task_wait_duration.flush();
        self.task_execute_times.flush();
        for h in &self.task_poll_duration {
            h.flush();
        }
    }

    #[inline]
    pub(super) fn should_flush(&self) -> bool {
        self.local_total_elapsed_us.get() >= FLUSH_LOCAL_THRESHOLD_US
    }
}

impl<R, T> Runner for TrackedRunner<R>
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
        let total_running_time = extras.total_running_time.clone();
        let task_running_time = extras.running_time.clone().unwrap();
        let start_time = extras.start_time;
        let level = extras.current_level as usize;
        extras.exec_times += 1;
        let exec_times = extras.exec_times;
        let begin = Instant::now();
        let res = self.inner.handle(local, task_cell);
        let elapsed = begin.elapsed();

        task_running_time.inc_by(elapsed);
        if let Some(ref running_time) = total_running_time {
            running_time.inc_by(elapsed);
        }
        self.task_poll_duration[level].observe(elapsed.as_secs_f64());
        let elapsed_us = elapsed.as_micros() as u64;
        if level == 0 {
            self.local_level0_elapsed_us.inc_by(elapsed_us);
        }
        // set task execute time metrics
        if res {
            let exec_time = task_running_time.as_duration();
            let wait_time = start_time.elapsed().saturating_sub(exec_time);
            self.task_wait_duration.observe(wait_time.as_secs_f64());
            self.task_execute_duration.observe(exec_time.as_secs_f64());
            self.task_execute_times.observe(exec_times as f64);
        }
        self.local_total_elapsed_us.inc_by(elapsed_us);
        if self.auto_flush_metrics && self.should_flush() {
            self.flush();
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

#[derive(Clone)]
pub(super) struct MultiLevelMetrics {
    level0_elapsed_us: IntCounter,
    total_elapsed_us: IntCounter,
    task_wait_duration: Histogram,
    task_execute_duration: Histogram,
    task_poll_duration: [Histogram; LEVEL_NUM],
    task_execute_times: Histogram,
}

impl MultiLevelMetrics {
    pub fn new(name: Option<&str>) -> Self {
        let (
            level0_elapsed_us,
            total_elapsed_us,
            task_wait_duration,
            task_execute_duration,
            task_execute_times,
            task_poll_duration,
        ) = if let Some(name) = name {
            (
                MULTILEVEL_LEVEL_ELAPSED
                    .get_metric_with_label_values(&[name, "0"])
                    .unwrap(),
                MULTILEVEL_LEVEL_ELAPSED
                    .get_metric_with_label_values(&[name, "total"])
                    .unwrap(),
                TASK_WAIT_DURATION
                    .get_metric_with_label_values(&[name])
                    .unwrap(),
                TASK_EXEC_DURATION
                    .get_metric_with_label_values(&[name])
                    .unwrap(),
                TASK_EXEC_TIMES
                    .get_metric_with_label_values(&[name])
                    .unwrap(),
                array::from_fn(|i| {
                    TASK_POLL_DURATION
                        .get_metric_with_label_values(&[name, &format!("{i}")])
                        .unwrap()
                }),
            )
        } else {
            (
                IntCounter::new("_", "_").unwrap(),
                IntCounter::new("_", "_").unwrap(),
                Histogram::with_opts(HistogramOpts::new("_", "_")).unwrap(),
                Histogram::with_opts(HistogramOpts::new("_", "_")).unwrap(),
                Histogram::with_opts(HistogramOpts::new("_", "_")).unwrap(),
                array::from_fn(|_| Histogram::with_opts(HistogramOpts::new("_", "_")).unwrap()),
            )
        };
        Self {
            level0_elapsed_us,
            total_elapsed_us,
            task_wait_duration,
            task_execute_duration,
            task_execute_times,
            task_poll_duration,
        }
    }
}

/// TrackedRunnerBuilder is the runner builder for TrackedRunner
pub struct TrackedRunnerBuilder<B> {
    inner: B,
    metrics: MultiLevelMetrics,
    auto_flush_metrics: bool,
}

impl<B> TrackedRunnerBuilder<B> {
    pub(super) fn new(inner: B, metrics: MultiLevelMetrics, auto_flush_metrics: bool) -> Self {
        Self {
            inner,
            metrics,
            auto_flush_metrics,
        }
    }
}

impl<B, R, T> RunnerBuilder for TrackedRunnerBuilder<B>
where
    B: RunnerBuilder<Runner = R>,
    R: Runner<TaskCell = T>,
    T: TaskCell,
{
    type Runner = TrackedRunner<R>;

    fn build(&mut self) -> Self::Runner {
        TrackedRunner {
            inner: self.inner.build(),
            local_level0_elapsed_us: self.metrics.level0_elapsed_us.local(),
            local_total_elapsed_us: self.metrics.total_elapsed_us.local(),
            task_execute_duration: self.metrics.task_execute_duration.local(),
            task_wait_duration: self.metrics.task_wait_duration.local(),
            task_poll_duration: array::from_fn(|i| self.metrics.task_poll_duration[i].local()),
            task_execute_times: self.metrics.task_execute_times.local(),
            auto_flush_metrics: self.auto_flush_metrics,
        }
    }
}
/// The runner for multilevel task queues.
///
/// The runner helps multilevel task queues collect additional information.
/// [`MultilevelRunnerBuilder`] is the [`RunnerBuilder`] for this runner.
pub struct MultilevelRunner<R> {
    inner: TrackedRunner<R>,
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

    fn handle(&mut self, local: &mut Local<T>, task_cell: T) -> bool {
        let res = self.inner.handle(local, task_cell);
        if self.inner.should_flush() {
            self.inner.flush();
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
    level0_elapsed_us: IntCounter,
    total_elapsed_us: IntCounter,
    task_level_mgr: TaskLevelManager,
    level0_chance: Gauge,
    level0_proportion_target: f64,
    adjusting: AtomicBool,
    last_level0_elapsed_us: Cell<u64>,
    last_total_elapsed_us: Cell<u64>,
    task_poll_duration: [Histogram; LEVEL_NUM],
    last_exec_tasks_per_level: [Cell<u64>; LEVEL_NUM],
    max_level_queue_steal_size: AtomicUsize,
}

/// Safety: `last_level0_elapsed_us` and `last_total_elapsed_us` are only used
/// in `maybe_adjust_chance`. `maybe_adjust_chance` is protected by `adjusting`
/// so we can make sure only one thread can access them at the same time.
unsafe impl Sync for LevelManager {}

impl LevelManager {
    fn prepare_before_push<T>(&self, task_cell: &mut T)
    where
        T: TaskCell,
    {
        self.task_level_mgr.adjust_task_level(task_cell);
        task_cell.mut_extras().schedule_time = Some(now());
    }

    fn maybe_adjust_chance(&self) {
        if self
            .adjusting
            .compare_exchange(false, true, SeqCst, SeqCst)
            .is_err()
        {
            return;
        }
        // The statistics may be not so accurate because we cannot load two
        // atomics at the same time.
        let total = self.total_elapsed_us.get();
        let total_diff = total - self.last_total_elapsed_us.get();
        if total_diff < ADJUST_CHANCE_INTERVAL_US {
            // Needn't change it now.
            self.adjusting.store(false, SeqCst);
            return;
        }
        let level0 = self.level0_elapsed_us.get();
        let level0_diff = level0 - self.last_level0_elapsed_us.get();
        self.last_total_elapsed_us.set(total);
        self.last_level0_elapsed_us.set(level0);

        let current_proportion = level0_diff as f64 / total_diff as f64;
        let proportion_diff = self.level0_proportion_target - current_proportion;
        let level0_chance = self.level0_chance.get();
        let new_chance = if proportion_diff > ADJUST_CHANCE_THRESHOLD {
            f64::min(level0_chance + ADJUST_AMOUNT, MAX_LEVEL0_CHANCE)
        } else if proportion_diff < -ADJUST_CHANCE_THRESHOLD {
            f64::max(level0_chance - ADJUST_AMOUNT, MIN_LEVEL0_CHANCE)
        } else {
            level0_chance
        };
        self.level0_chance.set(new_chance);

        let cur_total_tasks_per_level: [u64; LEVEL_NUM] =
            array::from_fn(|i| self.task_poll_duration[i].get_sample_count());
        let cur_total_tasks = cur_total_tasks_per_level.iter().sum::<u64>();
        let last_level0_total_tasks = self.last_exec_tasks_per_level[0].get();
        let last_total_tasks: u64 = self.last_exec_tasks_per_level.iter().map(|c| c.get()).sum();

        let level_0_tasks = (cur_total_tasks_per_level[0] - last_level0_total_tasks) as usize;
        let total_tasks = (cur_total_tasks - last_total_tasks) as usize;
        // adjust the batch size after meeting enough tasks.
        if total_tasks > ADJUST_LEVEL_STEAL_SIZE_THRESHOLD {
            let new_steal_count = if level_0_tasks == 0 {
                // level 0 has no tasks, that means the current workloads are all low-priority tasks.
                LEVEL_MAX_QUEUE_MAX_STEAL_SIZE
            } else {
                // by default level0 contains 80% of all tasks, so in the most common case, only
                // pop 1 task from level max once, and increases level max batch size when the executed
                // tasks are more than level0.
                std::cmp::min(total_tasks / level_0_tasks, LEVEL_MAX_QUEUE_MAX_STEAL_SIZE)
            };
            self.max_level_queue_steal_size
                .store(new_steal_count, SeqCst);
            for (i, c) in self.last_exec_tasks_per_level.iter().enumerate() {
                c.set(cur_total_tasks_per_level[i]);
            }
        }

        self.adjusting.store(false, SeqCst);
    }
}

pub(super) struct TaskLevelManager {
    task_elapsed_map: TaskElapsedMap,
    level_time_threshold: [Duration; LEVEL_NUM - 1],
}

impl TaskLevelManager {
    pub fn new(
        level_time_threshold: [Duration; LEVEL_NUM - 1],
        cleanup_interval: Option<Duration>,
    ) -> Self {
        Self {
            task_elapsed_map: TaskElapsedMap::new(cleanup_interval),
            level_time_threshold,
        }
    }

    pub fn adjust_task_level<T>(&self, task_cell: &mut T)
    where
        T: TaskCell,
    {
        let extras = task_cell.mut_extras();
        let task_id = extras.task_id;
        let current_level = match extras.fixed_level {
            Some(level) => level,
            None => {
                let running_time = extras
                    .total_running_time
                    .get_or_insert_with(|| self.get_elapsed(task_id));
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
    }

    pub(super) fn try_cleanup(&self) -> Option<Instant> {
        self.task_elapsed_map.try_cleanup()
    }

    pub(super) fn get_elapsed(&self, key: u64) -> Arc<ElapsedTime> {
        self.task_elapsed_map.get_elapsed(key)
    }
}

pub(crate) struct ElapsedTime(AtomicU64);

impl ElapsedTime {
    pub(crate) fn as_duration(&self) -> Duration {
        Duration::from_micros(self.0.load(Relaxed))
    }

    pub(super) fn inc_by(&self, t: Duration) {
        self.0.fetch_add(t.as_micros() as u64, Relaxed);
    }

    #[cfg(test)]
    fn from_duration(dur: Duration) -> Self {
        let elapsed = ElapsedTime::default();
        elapsed.inc_by(dur);
        elapsed
    }
}

impl Default for ElapsedTime {
    fn default() -> ElapsedTime {
        ElapsedTime(AtomicU64::new(0))
    }
}

impl fmt::Debug for ElapsedTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.as_duration())
    }
}

thread_local!(static TLS_LAST_CLEANUP_TIME: Cell<Instant> = Cell::new(Instant::now()));

struct TaskElapsedMap {
    new_index: AtomicUsize,
    maps: [DashMap<u64, Arc<ElapsedTime>>; 2],
    cleanup_interval: Option<Duration>,
    last_cleanup_time: Mutex<Instant>,
    cleaning_up: AtomicBool,
}

impl Default for TaskElapsedMap {
    fn default() -> TaskElapsedMap {
        TaskElapsedMap::new(Some(DEFAULT_CLEANUP_OLD_MAP_INTERVAL))
    }
}

impl TaskElapsedMap {
    fn new(cleanup_interval: Option<Duration>) -> TaskElapsedMap {
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
        fail_point!("between-read-new-and-read-old");
        let elapsed = match old_map.try_get(&key) {
            Present(v) => {
                let v2 = v.clone();
                drop(v);
                fail_point!("between-get-from-old-and-insert-into-new");
                new_map.insert(key, v2.clone());
                v2
            }
            _ => {
                // the key is absent or the old map is under clearing
                fail_point!("before-insert-new");
                let v = new_map.entry(key).or_default();
                v.clone()
            }
        };
        self.maybe_cleanup();
        elapsed
    }

    fn try_cleanup(&self) -> Option<Instant> {
        self.cleaning_up
            .compare_exchange(false, true, SeqCst, SeqCst)
            .map(|_| {
                let old_index = self.new_index.load(SeqCst) ^ 1;
                self.maps[old_index].clear();
                self.new_index.store(old_index, SeqCst);
                let now = now();
                *self.last_cleanup_time.lock().unwrap() = now;
                self.cleaning_up.store(false, SeqCst);
                now
            })
            .ok()
    }

    fn maybe_cleanup(&self) {
        if let Some(cleanup_interval) = self.cleanup_interval {
            let min_cleanup_time = recent() - cleanup_interval;
            TLS_LAST_CLEANUP_TIME.with(|t| {
                let tls_last_cleanup_time = t.get();
                if tls_last_cleanup_time < min_cleanup_time {
                    let mut last_cleanup_time = *self.last_cleanup_time.lock().unwrap();
                    if last_cleanup_time < min_cleanup_time {
                        last_cleanup_time = self.try_cleanup().unwrap_or(last_cleanup_time)
                    }
                    if tls_last_cleanup_time < last_cleanup_time {
                        t.set(last_cleanup_time);
                    }
                }
            });
        }
    }
}

/// The configurations of multilevel task queues.
pub struct Config {
    name: Option<String>,
    cleanup_interval: Option<Duration>,
    level_time_threshold: [Duration; LEVEL_NUM - 1],
    level0_proportion_target: f64,
}

impl Config {
    /// Sets the name of the multilevel task queue. Metrics of multilevel
    /// task queues are available if name is provided.
    pub fn name(mut self, name: Option<impl Into<String>>) -> Self {
        self.name = name.map(Into::into);
        self
    }

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

    /// Sets the interval of cleaning up task elapsed map.
    ///
    /// The pool tries to cleanup task elapsed map for every given interval. However, it may introduce tail latency on
    /// spawning. You can set it to none to disable the auto cleanup, in which case, you also have to do the cleanup
    /// task yourself.
    ///
    /// The default value is 10s.
    #[inline]
    pub fn cleanup_interval(mut self, value: Option<Duration>) -> Self {
        self.cleanup_interval = value;
        self
    }
}

impl Default for Config {
    fn default() -> Config {
        Config {
            name: None,
            cleanup_interval: Some(DEFAULT_CLEANUP_OLD_MAP_INTERVAL),
            level_time_threshold: [Duration::from_millis(5), Duration::from_millis(100)],
            level0_proportion_target: 0.8,
        }
    }
}

/// The builder of a multilevel task queue.
pub struct Builder {
    metrics: MultiLevelMetrics,
    manager: Arc<LevelManager>,
}

impl Builder {
    /// Creates a multilevel task queue builder from the config.
    pub fn new(config: Config) -> Builder {
        let metrics = MultiLevelMetrics::new(config.name.as_deref());
        let level0_chance = if let Some(name) = config.name {
            MULTILEVEL_LEVEL0_CHANCE
                .get_metric_with_label_values(&[&name])
                .unwrap()
        } else {
            Gauge::new("_", "_").unwrap()
        };

        level0_chance.set(INIT_LEVEL0_CHANCE);
        let manager = Arc::new(LevelManager {
            level0_elapsed_us: metrics.level0_elapsed_us.clone(),
            total_elapsed_us: metrics.total_elapsed_us.clone(),
            task_level_mgr: TaskLevelManager::new(
                config.level_time_threshold,
                config.cleanup_interval,
            ),
            level0_chance,
            level0_proportion_target: config.level0_proportion_target,
            adjusting: AtomicBool::new(false),
            last_level0_elapsed_us: Cell::new(0),
            last_total_elapsed_us: Cell::new(0),
            last_exec_tasks_per_level: array::from_fn(|_| Cell::new(0)),
            task_poll_duration: metrics.task_poll_duration.clone(),
            max_level_queue_steal_size: AtomicUsize::new(
                DEFAULT_STEAL_LIMIT_PER_LEVEL[LEVEL_NUM - 1],
            ),
        });
        Builder { manager, metrics }
    }

    /// Creates a runner builder for the multilevel task queue with a normal runner builder.
    pub fn runner_builder<B>(&self, inner_runner_builder: B) -> MultilevelRunnerBuilder<B> {
        MultilevelRunnerBuilder {
            inner: TrackedRunnerBuilder {
                inner: inner_runner_builder,
                metrics: self.metrics.clone(),
                auto_flush_metrics: false,
            },
            manager: self.manager.clone(),
        }
    }

    /// Returns a function for cleaning up task elapsed map.
    pub fn cleanup_fn(&self) -> impl Fn() -> Option<Instant> {
        let m = self.manager.clone();
        move || m.task_level_mgr.try_cleanup()
    }

    fn build_raw<T>(self, local_num: usize) -> (TaskInjector<T>, Vec<LocalQueue<T>>) {
        let level_injectors: Arc<[Injector<T>; LEVEL_NUM]> =
            Arc::new([Injector::new(), Injector::new(), Injector::new()]);
        let workers: Vec<_> = iter::repeat_with(Worker::new_lifo)
            .take(local_num)
            .collect();
        let stealers: Vec<_> = workers.iter().map(Worker::stealer).collect();
        let locals = workers
            .into_iter()
            .enumerate()
            .map(|(self_index, local_queue)| {
                let mut stealers: Vec<_> = stealers
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| *index != self_index)
                    .map(|(_, stealer)| stealer.clone())
                    .collect();
                // Steal with a random start to avoid imbalance.
                stealers.shuffle(&mut thread_rng());
                LocalQueue {
                    local_queue,
                    level_injectors: level_injectors.clone(),
                    stealers,
                    manager: self.manager.clone(),
                }
            })
            .collect();

        (
            TaskInjector {
                level_injectors,
                manager: self.manager,
            },
            locals,
        )
    }

    /// Creates the injector and local queues of the multilevel task queue.
    pub(crate) fn build<T>(
        self,
        local_num: usize,
    ) -> (super::TaskInjector<T>, Vec<super::LocalQueue<T>>) {
        let (injector, locals) = self.build_raw(local_num);
        let local_queues = locals
            .into_iter()
            .map(|local| super::LocalQueue(super::LocalQueueInner::Multilevel(local)))
            .collect();
        (
            super::TaskInjector(super::InjectorInner::Multilevel(injector)),
            local_queues,
        )
    }
}

thread_local!(static RECENT_NOW: Cell<Instant> = Cell::new(Instant::now()));

/// Returns an instant corresponding to now and updates the thread-local recent
/// now.
pub(super) fn now() -> Instant {
    let res = Instant::now();
    RECENT_NOW.with(|r| r.set(res));
    res
}

/// Returns the thread-local recent now. It is used to save the cost of calling
/// `Instant::now` frequently.
///
/// You should only use it when the thread-local recent now is recently updated.
pub(super) fn recent() -> Instant {
    RECENT_NOW.with(|r| r.get())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::build_spawn;
    use crate::queue::Extras;

    use std::sync::atomic::AtomicU64;
    use std::sync::mpsc;
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
        // Create a map with cleanup interval
        let map1 = TaskElapsedMap::new(Some(Duration::from_millis(200)));
        map1.get_elapsed(1).inc_by(Duration::from_secs(1));
        // Create a map without cleanup interval
        let map2 = TaskElapsedMap::new(None);
        map2.get_elapsed(1).inc_by(Duration::from_secs(1));

        // Trigger a cleanup
        thread::sleep(Duration::from_millis(200));
        now(); // Update recent now
        map1.get_elapsed(2).inc_by(Duration::from_secs(1));
        map2.get_elapsed(2).inc_by(Duration::from_secs(1));
        // After one cleanup, we can still read the old stats
        assert_eq!(map1.get_elapsed(1).as_duration(), Duration::from_secs(1));
        assert_eq!(map2.get_elapsed(1).as_duration(), Duration::from_secs(1));

        // Trigger a cleanup
        thread::sleep(Duration::from_millis(200));
        now();
        map1.get_elapsed(2).inc_by(Duration::from_secs(1));
        map2.get_elapsed(2).inc_by(Duration::from_secs(1));
        // Trigger another cleanup
        thread::sleep(Duration::from_millis(200));
        now();
        map1.get_elapsed(2).inc_by(Duration::from_secs(1));
        map2.get_elapsed(2).inc_by(Duration::from_secs(1));
        assert_eq!(map1.get_elapsed(2).as_duration(), Duration::from_secs(3));
        assert_eq!(map2.get_elapsed(2).as_duration(), Duration::from_secs(3));

        // After two cleanups, we won't be able to read the old stats with id = 1 from map1
        assert_eq!(map1.get_elapsed(1).as_duration(), Duration::from_secs(0));
        // After two cleanups, we are still able to read the old stats with id = 1 from map2
        assert_eq!(map2.get_elapsed(1).as_duration(), Duration::from_secs(1));
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
        let (injector, mut locals) = builder.build(1);
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
        let (injector, _) = builder.build_raw(1);

        // Running time is 50us. It should be pushed to level 0.
        let extras = Extras {
            total_running_time: Some(Arc::new(ElapsedTime::from_duration(Duration::from_micros(
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
            total_running_time: Some(Arc::new(ElapsedTime::from_duration(Duration::from_millis(
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
            total_running_time: Some(Arc::new(ElapsedTime::from_duration(Duration::from_secs(1)))),
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
            total_running_time: Some(Arc::new(ElapsedTime::from_duration(Duration::from_secs(1)))),
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
    fn test_push_task_update_tls_recent() {
        // auto cleanup will be triggered only when tls_recent_now - tls_last_cleanup_time > cleanup_interval, thus we'd
        // better make sure that tls_recent always gets updated after pushing task.
        let builder = Builder::new(Config::default());
        let (injector, _) = builder.build::<MockTask>(1);
        let time_before_push = now();
        injector.push(MockTask::new(0, Extras::multilevel_default()));
        assert!(recent() > time_before_push);
    }

    #[test]
    fn test_pop_by_stealing_injector() {
        let builder = Builder::new(Config::default());
        let (injector, mut locals) = builder.build(3);
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
        let (injector, mut locals) = builder.build_raw(3);
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
            .map(|mut consumer| {
                let sum = sum.clone();
                thread::spawn(move || {
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
        let (remote, mut locals) = build_spawn(builder, Default::default());
        let mut runner = runner_builder.build();

        remote.spawn(MockTask::new(100, Extras::new_multilevel(1, None)));
        if let Some(Pop { task_cell, .. }) = locals[0].pop() {
            assert!(runner.handle(&mut locals[0], task_cell));
        }
        assert!(
            manager
                .task_level_mgr
                .task_elapsed_map
                .get_elapsed(1)
                .as_duration()
                >= Duration::from_millis(100)
        );
    }

    #[test]
    fn test_adjust_level_chance() {
        // Default level 0 target is 0.8
        let manager = Builder::new(Config::default()).manager;

        // Level 0 running time is lower than expected, level0_chance should
        // increase.
        let level0_chance_before = manager.level0_chance.get();
        manager.level0_elapsed_us.inc_by(500_000);
        manager.total_elapsed_us.inc_by(1_500_000);
        manager.maybe_adjust_chance();
        let level0_chance_after = manager.level0_chance.get();
        assert!(level0_chance_before < level0_chance_after);

        // Level 0 running time is higher than expected, level0_chance should
        // decrease.
        let level0_chance_before = manager.level0_chance.get();
        manager.level0_elapsed_us.inc_by(1_400_000);
        manager.total_elapsed_us.inc_by(1_500_000);
        manager.maybe_adjust_chance();
        let level0_chance_after = manager.level0_chance.get();
        assert!(level0_chance_before > level0_chance_after);

        // Level 0 running time is roughly equivalent to expected,
        // level0_chance should not change.
        let level0_chance_before = manager.level0_chance.get();
        manager.level0_elapsed_us.inc_by(1_210_000);
        manager.total_elapsed_us.inc_by(1_500_000);
        manager.maybe_adjust_chance();
        let level0_chance_after = manager.level0_chance.get();
        assert_eq!(level0_chance_before, level0_chance_after);
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_get_elapsed_deadlock() {
        let _guard = fail::FailScenario::setup();
        fail::cfg("between-get-from-old-and-insert-into-new", "delay(500)").unwrap();
        fail::cfg("before-insert-new", "delay(400)").unwrap();
        let map = Arc::new(TaskElapsedMap::new(Some(Duration::default())));

        let map2 = map.clone();
        thread::spawn(move || {
            // t = 0, new = 0, old = 1, read new fail, read old fail
            // t = 400, insert into new (0)
            map2.get_elapsed(1);
        });

        let map2 = map.clone();
        thread::spawn(move || {
            // t = 0, new = 0, old = 1, read new fail
            // t = 350, read old (1) get
            // t = 850, insert into new (0)
            thread::sleep(Duration::from_millis(50));
            fail::cfg("between-read-new-and-read-old", "delay(300)").unwrap();
            map2.get_elapsed(1);
        });

        // t = 100, new = 0, old = 1, clear old (1)
        // t = 100, swap index, new = 1, old = 0
        thread::sleep(Duration::from_millis(100));
        now();
        map.maybe_cleanup();

        let map3 = map.clone();
        thread::spawn(move || {
            // t = 200, new = 1, old = 0, read new fail
            // t = 200, read old fail, insert into 1
            thread::sleep(Duration::from_millis(200));
            fail::remove("between-read-new-and-read-old");
            fail::remove("before-insert-new");
            map3.get_elapsed(1);
        });

        let map4 = map.clone();
        let (tx, rx) = mpsc::channel();
        thread::spawn(move || {
            // t = 150, new = 1, old = 0, read new get none
            // t = 450, read old (0) get
            thread::sleep(Duration::from_millis(150));
            map4.get_elapsed(1);
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(5)).unwrap();
    }

    #[test]
    fn test_cleanup_fn() {
        let builder = Builder::new(Config::default().cleanup_interval(None));
        let cleanup = builder.cleanup_fn();
        let mgr = &builder.manager.task_level_mgr;
        mgr.get_elapsed(1).inc_by(Duration::from_secs(1));
        assert_eq!(mgr.get_elapsed(1).as_duration(), Duration::from_secs(1));
        cleanup();
        assert_eq!(mgr.get_elapsed(1).as_duration(), Duration::from_secs(1));
        cleanup();
        assert_eq!(mgr.get_elapsed(1).as_duration(), Duration::from_secs(1));
        cleanup();
        cleanup();
        assert_eq!(mgr.get_elapsed(1).as_duration(), Duration::from_secs(0));
    }
}
