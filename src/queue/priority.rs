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

use crate::queue::{
    multilevel::{
        now, MultiLevelMetrics, TaskLevelManager, TrackedRunnerBuilder,
        DEFAULT_CLEANUP_OLD_MAP_INTERVAL, LEVEL_NUM,
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
    T: TaskCell + Send + 'static,
{
    /// Pushes the task cell to the queue. The schedule time in the extras is
    /// assigned to be now.
    pub fn push(&self, mut task_cell: T) {
        let priority = self.task_manager.prepare_before_push(&mut task_cell);
        self.queue.push(task_cell, priority);
    }

    /// Attempts to evict the lowest-priority task from the queue if the
    /// incoming priority is strictly higher (lower numeric value).
    /// Returns `Some(task)` on successful eviction, `None` otherwise.
    pub fn try_evict(&self, incoming_priority: u64) -> Option<T> {
        match self.queue.try_evict_for_priority(incoming_priority) {
            Ok(Some(task)) => Some(task),
            _ => None,
        }
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

    /// If the incoming priority is strictly higher (lower numeric value) than
    /// the lowest-priority task in the queue, evict that task and return it.
    /// Returns `Ok(Some(task))` on successful eviction, `Ok(None)` if the
    /// queue is empty or a concurrent caller removed the back entry first,
    /// or `Err(())` if the incoming priority is not strictly higher than the
    /// lowest-priority queued task.
    ///
    /// This is best-effort under contention: a concurrent removal between
    /// `back()` and `remove()` causes this call to return `Ok(None)` even
    /// though the new back may still qualify. Callers may retry if needed.
    fn try_evict_for_priority(&self, incoming_priority: u64) -> Result<Option<T>, ()> {
        let back = match self.pq.back() {
            Some(entry) => entry,
            None => return Ok(None),
        };
        if incoming_priority < back.key().0 {
            // Atomically remove this specific entry. If a concurrent caller
            // already removed it, `remove()` returns false and we avoid
            // accidentally evicting a different (possibly higher-priority) task.
            if back.remove() {
                if let Some(task) = back.value().take() {
                    return Ok(Some(task));
                }
            }
            // Race: a concurrent caller already removed this entry.
            Ok(None)
        } else {
            Err(()) // incoming is not higher priority
        }
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
    metrics: MultiLevelMetrics,
}

impl Builder {
    /// Creates a priority task queue builder with specified config and [`TaskPriorityProvider`].
    pub fn new(config: Config, priority_manager: Arc<dyn TaskPriorityProvider>) -> Builder {
        let Config {
            name,
            cleanup_interval,
            level_time_threshold,
        } = config;
        let metrics = MultiLevelMetrics::new(name.as_deref());
        Self {
            manager: PriorityTaskManager {
                level_manager: Arc::new(TaskLevelManager::new(
                    level_time_threshold,
                    cleanup_interval,
                )),
                priority_manager,
            },
            metrics,
        }
    }

    /// Creates a runner builder for the multilevel task queue with a normal runner builder.
    pub fn runner_builder<B>(&self, inner_runner_builder: B) -> TrackedRunnerBuilder<B> {
        TrackedRunnerBuilder::new(inner_runner_builder, self.metrics.clone(), true)
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
    use crate::metrics::*;
    use crate::pool::{build_spawn, Local, Runner, RunnerBuilder};
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

    #[test]
    fn test_evict_higher_priority() {
        // Push tasks with priorities 10, 20, 30. Evicting with priority 5
        // should return the task with priority 30 (the lowest-priority / highest
        // numeric value).
        let builder = Builder::new(Config::default(), Arc::new(OrderByIdProvider));
        let (injector, _) = builder.build_raw::<MockTask>(1);

        // task_id is used as priority by OrderByIdProvider
        injector.push(MockTask::new(0, 20));
        injector.push(MockTask::new(0, 10));
        injector.push(MockTask::new(0, 30));

        // Evict with priority 5 (higher than all queued tasks)
        let evicted = injector.try_evict(5);
        assert!(evicted.is_some());
        let mut evicted_task = evicted.unwrap();
        assert_eq!(evicted_task.mut_extras().task_id(), 30);

        // Two tasks remain
        let mut t1 = injector.queue.pop().unwrap().task_cell;
        assert_eq!(t1.mut_extras().task_id(), 10);
        let mut t2 = injector.queue.pop().unwrap().task_cell;
        assert_eq!(t2.mut_extras().task_id(), 20);
        assert!(injector.queue.pop().is_none());
    }

    #[test]
    fn test_evict_lower_priority_fails() {
        // try_evict with priority 50 when queue has tasks at 10, 20, 30
        // should return None because incoming is not higher priority.
        let builder = Builder::new(Config::default(), Arc::new(OrderByIdProvider));
        let (injector, _) = builder.build_raw::<MockTask>(1);

        injector.push(MockTask::new(0, 10));
        injector.push(MockTask::new(0, 20));
        injector.push(MockTask::new(0, 30));

        let evicted = injector.try_evict(50);
        assert!(evicted.is_none());

        // All three tasks should still be in the queue
        assert!(injector.queue.pop().is_some());
        assert!(injector.queue.pop().is_some());
        assert!(injector.queue.pop().is_some());
        assert!(injector.queue.pop().is_none());
    }

    #[test]
    fn test_evict_empty_queue() {
        let builder = Builder::new(Config::default(), Arc::new(OrderByIdProvider));
        let (injector, _) = builder.build_raw::<MockTask>(1);

        let evicted = injector.try_evict(5);
        assert!(evicted.is_none());
    }

    #[test]
    fn test_evict_equal_priority_fails() {
        // try_evict with same priority as lowest task should fail
        // (must be strictly higher).
        let builder = Builder::new(Config::default(), Arc::new(OrderByIdProvider));
        let (injector, _) = builder.build_raw::<MockTask>(1);

        injector.push(MockTask::new(0, 10));

        let evicted = injector.try_evict(10);
        // Equal priority — sequence number of existing task may differ, but
        // the priority component (key.0) is equal, so eviction should fail.
        assert!(evicted.is_none());
    }

    #[test]
    fn test_evict_concurrent_stress() {
        // Concurrent push/evict stress test — should not panic or deadlock.
        use std::sync::atomic::AtomicUsize;

        let builder = Builder::new(Config::default(), Arc::new(OrderByIdProvider));
        let (injector, _) = builder.build_raw::<MockTask>(1);
        let injector = Arc::new(injector);
        let evicted_count = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        // Spawn pushers
        for t in 0..4 {
            let inj = injector.clone();
            let h = thread::spawn(move || {
                for i in 0..100 {
                    inj.push(MockTask::new(0, (t * 1000 + i) as u64));
                }
            });
            handles.push(h);
        }

        // Spawn evictors
        for _ in 0..4 {
            let inj = injector.clone();
            let cnt = evicted_count.clone();
            let h = thread::spawn(move || {
                for _ in 0..100 {
                    if inj.try_evict(0).is_some() {
                        cnt.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            handles.push(h);
        }

        for h in handles {
            h.join().unwrap();
        }

        // Drain remaining
        let mut remaining = 0;
        while injector.queue.pop().is_some() {
            remaining += 1;
        }

        // Total pushed: 400. evicted + remaining should equal 400.
        assert_eq!(
            evicted_count.load(Ordering::Relaxed) + remaining,
            400,
            "evicted={} remaining={} total should be 400",
            evicted_count.load(Ordering::Relaxed),
            remaining
        );
    }

    #[test]
    fn test_evict_concurrent_priority_contract() {
        // Verify the strict-priority contract under concurrency: every evicted
        // task must have priority strictly greater than the incoming_priority
        // used by the evictor that returned it.  This catches the TOCTOU race
        // where a concurrent removal exposes a new back entry whose priority
        // is <= the caller's incoming_priority.
        use std::sync::atomic::AtomicBool;

        let builder = Builder::new(Config::default(), Arc::new(OrderByIdProvider));
        let (injector, _) = builder.build_raw::<MockTask>(1);
        let injector = Arc::new(injector);
        let contract_violated = Arc::new(AtomicBool::new(false));

        let mut handles = vec![];

        // Pushers: add tasks with priorities 1..=500 per thread (offset by
        // thread id) so that the queue always contains values near and
        // overlapping with every evictor's incoming_priority.
        for t in 0..4 {
            let inj = injector.clone();
            let h = thread::spawn(move || {
                for i in 0..500 {
                    let priority = (i + 1 + t) as u64;
                    inj.push(MockTask::new(0, priority));
                }
            });
            handles.push(h);
        }

        // Evictors with varying incoming priorities that sit inside the
        // pushed range, creating frequent boundary overlap.
        let incoming_priorities: Vec<u64> = vec![25, 100, 250, 400];
        for incoming in incoming_priorities {
            let inj = injector.clone();
            let violated = contract_violated.clone();
            let h = thread::spawn(move || {
                for _ in 0..500 {
                    if let Some(mut task) = inj.try_evict(incoming) {
                        let evicted_priority = task.mut_extras().task_id();
                        if evicted_priority <= incoming {
                            violated.store(true, Ordering::Relaxed);
                        }
                    }
                }
            });
            handles.push(h);
        }

        for h in handles {
            if let Err(payload) = h.join() {
                std::panic::resume_unwind(payload);
            }
        }

        assert!(
            !contract_violated.load(Ordering::Relaxed),
            "evicted a task whose priority was <= the evictor's incoming_priority"
        );
    }

    #[test]
    fn test_metrics() {
        let name = "test_priority_metrics";
        let builder = Builder::new(
            Config::default().name(Some(name)),
            Arc::new(OrderByIdProvider),
        );
        let mut runner = builder.runner_builder(MockRunnerBuilder).build();
        let (remote, mut locals) = build_spawn(builder, Default::default());

        for i in 0..4 {
            remote.spawn(MockTask::new(35, i));
        }
        while let Some(Pop { task_cell, .. }) = locals[0].pop() {
            assert!(runner.handle(&mut locals[0], task_cell));
        }

        // we spawn 4 tasks here but the metrics of the last one is not flush, so only check the first 3 here.
        assert!(
            MULTILEVEL_LEVEL_ELAPSED
                .get_metric_with_label_values(&[name, "0"])
                .unwrap()
                .get()
                > 100_000
        );
        assert!(
            TASK_WAIT_DURATION
                .get_metric_with_label_values(&[name])
                .unwrap()
                .get_sample_count()
                >= 3
        );
        assert!(
            TASK_EXEC_DURATION
                .get_metric_with_label_values(&[name])
                .unwrap()
                .get_sample_count()
                >= 3
        );
        assert!(
            TASK_EXEC_DURATION
                .get_metric_with_label_values(&[name])
                .unwrap()
                .get_sample_sum()
                >= 0.1
        );
        assert!(
            TASK_POLL_DURATION
                .get_metric_with_label_values(&[name, "0"])
                .unwrap()
                .get_sample_count()
                >= 3
        );
        assert!(
            TASK_POLL_DURATION
                .get_metric_with_label_values(&[name, "0"])
                .unwrap()
                .get_sample_sum()
                >= 0.1
        );
        assert!(
            TASK_EXEC_TIMES
                .get_metric_with_label_values(&[name])
                .unwrap()
                .get_sample_count()
                >= 3
        );
        assert!(
            TASK_EXEC_TIMES
                .get_metric_with_label_values(&[name])
                .unwrap()
                .get_sample_sum()
                >= 3.0
        );
    }
}
