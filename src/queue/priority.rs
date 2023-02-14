// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! A priority task queue. Tasks are scheduled based on its priority, tasks with small priority
//! value will be scheduler earlier than bigger onces. User should implement The [`TaskPriorityProvider`]
//! to provide the priority value for each task. The priority value is fetched from the
//! [`TaskPriorityProvider`] at each time the task is scheduled.
//!
//! The task queue requires that the accompanying [`PriorityRunner`] must beused to collect necessary
//! information.

use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crossbeam_skiplist::SkipMap;
use crossbeam_utils::atomic::AtomicCell;
use prometheus::local::LocalIntCounter;
use prometheus::IntCounter;

use crate::metrics::*;
use crate::pool::{Local, Runner, RunnerBuilder};
use crate::queue::{
    multilevel::{
        now, TaskLevelManager, DEFAULT_CLEANUP_OLD_MAP_INTERVAL, FLUSH_LOCAL_THRESHOLD_US,
        LEVEL_NUM,
    },
    Extras, Pop, TaskCell,
};

// a wrapper of u64 with an extra sequence number to avoid duplicate value.
#[derive(Eq, PartialEq, Ord, PartialOrd)]
struct MapKey(u64, u64);

/// The injector of a single level work stealing task queue.
#[derive(Clone)]
pub struct TaskInjector<T> {
    queue: Arc<QueueCore<T>>,
    task_manager: PriorityTaskManager,
}

impl<T> TaskInjector<T>
where
    T: TaskCell + Send,
{
    /// Pushes the task cell to the queue. The schedule time in the extras is
    /// assigned to be now.
    pub fn push(&self, mut task_cell: T) {
        let priority = self.task_manager.prepare_before_push(&mut task_cell);
        self.queue.push(task_cell, priority);
    }
}

/// priority queue does not have local queue, all tasks are always put in the global queue.
pub(crate) type LocalQueue<T> = TaskInjector<T>;

impl<T> LocalQueue<T>
where
    T: TaskCell + Send,
{
    pub(super) fn pop(&mut self) -> Option<Pop<T>> {
        self.queue.pop()
    }

    pub(super) fn has_tasks_or_pull(&mut self) -> bool {
        !self.queue.is_empty()
    }
}

/// A trait used to generate priority value for each task.
pub trait TaskPriorityProvider: Send + Sync + 'static {
    /// Return a priority value of this task, all tasks in the priority
    /// queue is ordered by this value.
    fn priority_of(&self, extras: &Extras) -> u64;
}

#[derive(Clone)]
struct PriorityTaskManager {
    level_manager: Arc<TaskLevelManager>,
    priority_manager: Arc<dyn TaskPriorityProvider>,
}

impl PriorityTaskManager {
    fn prepare_before_push<T>(&self, task_cell: &mut T) -> u64
    where
        T: TaskCell,
    {
        self.level_manager.adjust_task_level(task_cell);
        task_cell.mut_extras().schedule_time = Some(now());
        self.priority_manager.priority_of(task_cell.mut_extras())
    }
}

/// The global priority queue. We use a `SkipMap` as a priority queue,
/// The key is the priority and value is the task.
struct QueueCore<T> {
    pq: SkipMap<MapKey, Slot<T>>,
    /// a global sequence generator to ensure all task keys are unique.
    sequence: AtomicU64,
}

impl<T> QueueCore<T> {
    fn new() -> Self {
        Self {
            pq: SkipMap::new(),
            sequence: AtomicU64::new(0),
        }
    }

    fn is_empty(&self) -> bool {
        self.pq.is_empty()
    }
}

impl<T: TaskCell + Send + 'static> QueueCore<T> {
    fn push(&self, msg: T, priority: u64) {
        self.pq.insert(self.gen_key(priority), Slot::new(msg));
    }

    pub fn pop(&self) -> Option<Pop<T>> {
        fn into_pop<T>(mut t: T) -> Pop<T>
        where
            T: TaskCell,
        {
            let schedule_time = t.mut_extras().schedule_time.unwrap();
            Pop {
                task_cell: t,
                schedule_time,
                from_local: false,
            }
        }

        self.pq
            .pop_front()
            .map(|e| into_pop(e.value().take().unwrap()))
    }

    #[inline]
    fn gen_key(&self, priority: u64) -> MapKey {
        MapKey(priority, self.sequence.fetch_add(1, Ordering::Relaxed))
    }
}

/// A holder to store task. Wrap the task in a AtomicCell becuase crossbeam-skip only provide
/// readonly acess to a popped Entry.
struct Slot<T> {
    cell: AtomicCell<Option<T>>,
}

impl<T> Slot<T> {
    fn new(value: T) -> Self {
        Self {
            cell: AtomicCell::new(Some(value)),
        }
    }

    fn take(&self) -> Option<T> {
        self.cell.take()
    }
}

/// The runner for priority queues.
///
/// The runner helps collect additional information to support auto-adjust task level.
/// [`PriorityRunnerBuiler`] is the [`RunnerBuilder`] for this runner.
pub struct PriorityRunner<R> {
    inner: R,
    local_level0_elapsed_us: LocalIntCounter,
    local_total_elapsed_us: LocalIntCounter,
}

impl<R, T> Runner for PriorityRunner<R>
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
        let level = extras.current_level();
        let total_running_time = extras.total_running_time.clone();
        let begin = Instant::now();
        let res = self.inner.handle(local, task_cell);
        let elapsed = begin.elapsed();
        let elapsed_us = elapsed.as_micros() as u64;
        if let Some(ref running_time) = total_running_time {
            running_time.inc_by(elapsed);
        }
        if level == 0 {
            self.local_level0_elapsed_us.inc_by(elapsed_us);
        }
        self.local_total_elapsed_us.inc_by(elapsed_us);
        let local_total = self.local_total_elapsed_us.get();
        if local_total > FLUSH_LOCAL_THRESHOLD_US {
            self.local_level0_elapsed_us.flush();
            self.local_total_elapsed_us.flush();
        }
        res
    }

    fn pause(&mut self, local: &mut Local<Self::TaskCell>) -> bool {
        self.inner.pause(local)
    }

    fn resume(&mut self, local: &mut Local<Self::TaskCell>) {
        self.inner.resume(local)
    }

    fn end(&mut self, local: &mut Local<Self::TaskCell>) {
        self.inner.end(local)
    }
}

/// The runner builder for priority task queues.
///
/// It can be created by [`Builder::runner_builder`].
pub struct PriorityRunnerBuiler<B> {
    inner: B,
    level0_elapsed_us: IntCounter,
    total_elapsed_us: IntCounter,
}

impl<B, R, T> RunnerBuilder for PriorityRunnerBuiler<B>
where
    B: RunnerBuilder<Runner = R>,
    R: Runner<TaskCell = T>,
    T: TaskCell,
{
    type Runner = PriorityRunner<R>;

    fn build(&mut self) -> Self::Runner {
        PriorityRunner {
            inner: self.inner.build(),
            local_level0_elapsed_us: self.level0_elapsed_us.local(),
            local_total_elapsed_us: self.total_elapsed_us.local(),
        }
    }
}

/// The configurations of priority task queues.
pub struct Config {
    name: Option<String>,
    cleanup_interval: Option<Duration>,
    level_time_threshold: [Duration; LEVEL_NUM - 1],
}

impl Config {
    /// Sets the name of the priority task queue. Metrics are available if name is provided.
    pub fn name(mut self, name: Option<impl Into<String>>) -> Self {
        self.name = name.map(Into::into);
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
        }
    }
}

/// The builder of a priority task queue.
pub struct Builder {
    manager: PriorityTaskManager,
    level0_elapsed_us: IntCounter,
    total_elapsed_us: IntCounter,
}

impl Builder {
    /// Creates a priority task queue builder with specified config and [`TaskPriorityProvider`].
    pub fn new(config: Config, priority_manager: Arc<dyn TaskPriorityProvider>) -> Builder {
        let Config {
            name,
            cleanup_interval,
            level_time_threshold,
        } = config;
        let (level0_elapsed_us, total_elapsed_us) = if let Some(name) = name {
            (
                MULTILEVEL_LEVEL_ELAPSED
                    .get_metric_with_label_values(&[&name, "0"])
                    .unwrap(),
                MULTILEVEL_LEVEL_ELAPSED
                    .get_metric_with_label_values(&[&name, "total"])
                    .unwrap(),
            )
        } else {
            (
                IntCounter::new("_", "_").unwrap(),
                IntCounter::new("_", "_").unwrap(),
            )
        };
        Self {
            manager: PriorityTaskManager {
                level_manager: Arc::new(TaskLevelManager::new(
                    level_time_threshold,
                    cleanup_interval,
                )),
                priority_manager,
            },
            level0_elapsed_us,
            total_elapsed_us,
        }
    }

    /// Creates a runner builder for the multilevel task queue with a normal runner builder.
    pub fn runner_builder<B>(&self, inner_runner_builder: B) -> PriorityRunnerBuiler<B> {
        PriorityRunnerBuiler {
            inner: inner_runner_builder,
            level0_elapsed_us: self.level0_elapsed_us.clone(),
            total_elapsed_us: self.total_elapsed_us.clone(),
        }
    }

    /// Returns a function for trying to cleanup task elapsed map.
    pub fn cleanup_fn(&self) -> impl Fn() -> Option<Instant> {
        let m = self.manager.level_manager.clone();
        move || m.try_cleanup()
    }

    pub(crate) fn build_raw<T>(self, local_num: usize) -> (TaskInjector<T>, Vec<LocalQueue<T>>) {
        let queue = Arc::new(QueueCore::new());
        let local_queue = std::iter::repeat_with(|| LocalQueue {
            queue: queue.clone(),
            task_manager: self.manager.clone(),
        })
        .take(local_num)
        .collect();

        let injector = TaskInjector {
            queue,
            task_manager: self.manager,
        };

        (injector, local_queue)
    }

    pub(crate) fn build<T>(
        self,
        local_num: usize,
    ) -> (super::TaskInjector<T>, Vec<super::LocalQueue<T>>) {
        let (injector, locals) = self.build_raw(local_num);
        (
            super::TaskInjector(super::InjectorInner::Priority(injector)),
            locals
                .into_iter()
                .map(|i| super::LocalQueue(super::LocalQueueInner::Priority(i)))
                .collect(),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;
    use crate::queue::{
        multilevel::{now, recent},
        Extras, InjectorInner,
    };
    use rand::RngCore;
    #[derive(Debug)]
    struct MockTask {
        sleep_ms: u64,
        extras: Extras,
    }

    impl MockTask {
        fn new(sleep_ms: u64, task_id: u64) -> Self {
            MockTask {
                sleep_ms,
                extras: Extras::new_multilevel(task_id, None),
            }
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

    struct OrderByIdProvider;

    impl TaskPriorityProvider for OrderByIdProvider {
        fn priority_of(&self, extras: &Extras) -> u64 {
            return extras.task_id();
        }
    }

    #[test]
    fn test_priority_queue() {
        let local_count = 5usize;
        let builder = Builder::new(Config::default(), Arc::new(OrderByIdProvider));
        let mut runner = builder.runner_builder(MockRunnerBuilder).build();
        let (injecter, locals) = builder.build(local_count);
        let pq = match &injecter.0 {
            InjectorInner::Priority(p) => p.queue.clone(),
            _ => unreachable!(),
        };
        let core = Arc::new(crate::pool::spawn::QueueCore::new(
            injecter,
            crate::pool::SchedConfig::default(),
        ));
        let mut locals: Vec<_> = locals
            .into_iter()
            .enumerate()
            .map(|(i, l)| Local::new(i, l, core.clone()))
            .collect();

        let mut local = locals.pop().unwrap();
        let remote = crate::Remote::new(core.clone());
        for i in (0..5).rev() {
            remote.spawn(MockTask::new(0, i));
        }

        for i in 0..5 {
            let mut task = pq.pop().unwrap().task_cell;
            assert_eq!(task.mut_extras().task_id(), i);
        }

        // test multiple threads
        let task_per_thread = 10usize;
        let mut handlers = Vec::with_capacity(local_count);
        for mut local in locals {
            let h = std::thread::spawn(move || {
                let mut rng = rand::thread_rng();
                for _i in 0..task_per_thread {
                    let task = MockTask::new(0, rng.next_u64());
                    local.spawn(task);
                }
            });
            handlers.push(h);
        }
        for h in handlers {
            h.join().unwrap();
        }
        let mut last_id = 0u64;
        for _ in 0..task_per_thread * (local_count - 1) {
            let mut task = pq.pop().unwrap().task_cell;
            assert!(task.mut_extras().task_id() >= last_id);
            last_id = task.mut_extras().task_id();
        }
        assert!(pq.is_empty());

        // test dynamic level

        let mut run_task = |sleep_ms, level| {
            remote.spawn(MockTask::new(sleep_ms, 1));
            let mut task = pq.pop().unwrap().task_cell;
            assert_eq!(task.mut_extras().current_level(), level);
            runner.handle(&mut local, task);
        };

        run_task(5, 0);
        // after 5ms, the task should be put to level1
        run_task(95, 1);
        // after 100ms, the task should be put to level2
        run_task(1, 2);
    }

    #[test]
    fn test_push_task_update_tls_recent() {
        // auto cleanup will be triggered only when tls_recent_now - tls_last_cleanup_time > cleanup_interval, thus we'd
        // better make sure that tls_recent always gets updated after pushing task.
        let builder = Builder::new(Config::default(), Arc::new(OrderByIdProvider));
        let (injector, _) = builder.build::<MockTask>(1);
        let time_before_push = now();
        injector.push(MockTask::new(0, 0));
        assert!(recent() > time_before_push);
    }

    #[test]
    fn test_cleanup_fn() {
        let builder = Builder::new(
            Config::default().cleanup_interval(None),
            Arc::new(OrderByIdProvider),
        );
        let cleanup = builder.cleanup_fn();
        let mgr = builder.manager.level_manager.clone();
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
