// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Custom task queue abstractions.
//!
//! This module defines the common interfaces implemented by custom task queues.

use std::sync::Arc;

use super::{
    multilevel::{MultiLevelMetrics, TrackedRunnerBuilder},
    PopResult,
};

/// Common interface implemented by custom task queues used by the thread pool.
///
/// A task queue accepts task cells from producers and selects tasks for workers
/// to run. [`TaskQueue::pop`] describes both task availability and scheduling
/// readiness:
///
/// - [`PopResult::Ready`] means a task is available and should be run
///   immediately.
/// - [`PopResult::Pending`] means the queue contains tasks, but none are
///   ready to run now. The worker may sleep, but it can still be woken by a
///   later push and must retry no later than `retry_at`. This is useful for
///   queues with waiting states, such as waiting for rate-limit tokens before
///   a task can run.
/// - [`PopResult::Empty`] means the queue contains no tasks. If workers
///   keep seeing an empty queue for a while, they may park until a later
///   [`TaskQueue::push`] wakes them.
///
/// Queues must not return [`PopResult::Empty`] while they still contain
/// delayed or throttled tasks. Doing so can let workers park indefinitely and
/// leave those tasks unscheduled for a long time. Return
/// [`PopResult::Pending`] for that case instead.
pub trait TaskQueue<T>: Send + Sync + 'static {
    /// Pushes a task into the queue.
    fn push(&self, task_cell: T);

    /// Pops the next task to run, or reports why no task can run now.
    fn pop(&self) -> PopResult<T>;

    /// Drains all queued tasks regardless of scheduling readiness.
    ///
    /// This is used by shutdown paths to drop remaining tasks. Unlike
    /// [`TaskQueue::pop`], it should not leave delayed or throttled tasks in
    /// the queue just because they are not ready to run.
    ///
    /// A custom queue is shared by all worker-local handles, so shutdown may
    /// call this method multiple times or concurrently. Implementations must be
    /// idempotent and thread-safe.
    fn drain(&self);

    /// Returns whether the queue may have a ready task.
    ///
    /// This method must not remove a task from the queue. It is used as a
    /// preemption hint, so returning `false` while ready tasks exist can delay
    /// those tasks and hurt scheduling fairness. Returning `true` while no task
    /// is ready can cause unnecessary rescheduling.
    fn has_ready_task(&self) -> bool;
}

/// The configurations of custom task queues.
#[derive(Default)]
pub struct Config {
    name: Option<String>,
}

impl Config {
    /// Sets the name of the custom task queue. Metrics are available if name is provided.
    pub fn name(mut self, name: Option<impl Into<String>>) -> Self {
        self.name = name.map(Into::into);
        self
    }
}

/// The builder of a custom task queue.
pub struct Builder<T> {
    queue: Arc<dyn TaskQueue<T>>,
    metrics: MultiLevelMetrics,
}

impl<T> Builder<T> {
    /// Creates a custom task queue builder from a shared queue.
    pub fn new(config: Config, queue: Arc<dyn TaskQueue<T>>) -> Builder<T> {
        Builder {
            queue,
            metrics: MultiLevelMetrics::new(config.name.as_deref()),
        }
    }

    /// Creates a runner builder for the custom task queue with a normal runner builder.
    pub fn runner_builder<B>(&self, inner_runner_builder: B) -> TrackedRunnerBuilder<B> {
        TrackedRunnerBuilder::new(inner_runner_builder, self.metrics.clone(), true)
    }
}

impl<T: 'static> Builder<T> {
    /// Creates the injector and local queue handles of the custom task queue.
    pub(crate) fn build(
        self,
        local_num: usize,
    ) -> (super::TaskInjector<T>, Vec<super::LocalQueue<T>>) {
        let injector = TaskInjector::new(self.queue.clone());
        let locals: Vec<LocalQueue<T>> =
            std::iter::repeat_with(|| LocalQueue::new(self.queue.clone()))
                .take(local_num)
                .collect();

        (
            super::TaskInjector(super::InjectorInner::Custom(injector)),
            locals
                .into_iter()
                .map(|local| super::LocalQueue(super::LocalQueueInner::Custom(local)))
                .collect(),
        )
    }
}

/// The injector of a custom task queue.
pub struct TaskInjector<T> {
    queue: Arc<dyn TaskQueue<T>>,
}

impl<T> Clone for TaskInjector<T> {
    #[inline]
    fn clone(&self) -> TaskInjector<T> {
        TaskInjector {
            queue: self.queue.clone(),
        }
    }
}

impl<T: 'static> TaskInjector<T> {
    /// Creates a custom task queue injector from a shared queue.
    #[inline]
    pub fn new(queue: Arc<dyn TaskQueue<T>>) -> TaskInjector<T> {
        TaskInjector { queue }
    }

    /// Pushes a task into the custom queue.
    #[inline]
    pub fn push(&self, task_cell: T) {
        self.queue.push(task_cell);
    }
}

/// The local queue handle of a custom task queue.
pub struct LocalQueue<T> {
    queue: Arc<dyn TaskQueue<T>>,
}

impl<T> Clone for LocalQueue<T> {
    #[inline]
    fn clone(&self) -> LocalQueue<T> {
        LocalQueue {
            queue: self.queue.clone(),
        }
    }
}

impl<T: 'static> LocalQueue<T> {
    /// Creates a local queue handle from a shared custom queue.
    #[inline]
    pub fn new(queue: Arc<dyn TaskQueue<T>>) -> LocalQueue<T> {
        LocalQueue { queue }
    }

    /// Pushes a task into the custom queue.
    #[inline]
    pub fn push(&self, task_cell: T) {
        self.queue.push(task_cell);
    }

    /// Pops a task from the custom queue.
    #[inline]
    pub fn pop(&self) -> PopResult<T> {
        self.queue.pop()
    }

    /// Forcefully drains all tasks from the custom queue.
    ///
    /// Some queue implementations have local handles backed by shared queue
    /// state. This adapter forwards each local drain directly to the
    /// user-provided [`TaskQueue`], so shutdown may call [`TaskQueue::drain`]
    /// from multiple worker threads. Implementations must be idempotent and
    /// thread-safe.
    #[inline]
    pub fn drain(&self) {
        self.queue.drain();
    }

    /// Returns whether the custom queue may have ready work for preemption.
    ///
    /// This forwards to [`TaskQueue::has_ready_task`], which must be a
    /// non-consuming readiness hint.
    #[inline]
    pub fn has_tasks_or_pull(&self) -> bool {
        self.queue.has_ready_task()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            mpsc, Arc, Barrier, Mutex,
        },
        thread,
        time::{Duration, Instant},
    };

    use super::*;
    use crate::{
        metrics::{
            MULTILEVEL_LEVEL_ELAPSED, TASK_EXEC_DURATION, TASK_EXEC_TIMES, TASK_POLL_DURATION,
            TASK_WAIT_DURATION,
        },
        pool::{build_spawn, Local, Runner, RunnerBuilder},
        queue::{Extras, Pop, TaskCell},
    };

    struct MockTask {
        id: u64,
        sleep_ms: u64,
        extras: Extras,
    }

    impl MockTask {
        fn new(id: u64) -> MockTask {
            MockTask {
                id,
                sleep_ms: 0,
                extras: Extras::multilevel_default(),
            }
        }

        fn with_sleep(id: u64, sleep_ms: u64) -> MockTask {
            MockTask {
                id,
                sleep_ms,
                extras: Extras::multilevel_default(),
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

    struct MockQueue<T> {
        tasks: Mutex<VecDeque<T>>,
        scripted_results: Mutex<VecDeque<PopResult<T>>>,
        ready_hint: AtomicBool,
        drain_count: AtomicUsize,
    }

    impl<T> Default for MockQueue<T> {
        fn default() -> MockQueue<T> {
            MockQueue {
                tasks: Mutex::new(VecDeque::new()),
                scripted_results: Mutex::new(VecDeque::new()),
                ready_hint: AtomicBool::new(false),
                drain_count: AtomicUsize::new(0),
            }
        }
    }

    impl<T> MockQueue<T> {
        fn push_scripted_result(&self, result: PopResult<T>) {
            self.scripted_results.lock().unwrap().push_back(result);
        }

        fn set_ready_hint(&self, ready: bool) {
            self.ready_hint.store(ready, Ordering::SeqCst);
        }

        fn len(&self) -> usize {
            self.tasks.lock().unwrap().len()
        }

        fn drain_count(&self) -> usize {
            self.drain_count.load(Ordering::SeqCst)
        }
    }

    impl<T: Send + 'static> TaskQueue<T> for MockQueue<T> {
        fn push(&self, task_cell: T) {
            self.tasks.lock().unwrap().push_back(task_cell);
            self.ready_hint.store(true, Ordering::SeqCst);
        }

        fn pop(&self) -> PopResult<T> {
            if let Some(result) = self.scripted_results.lock().unwrap().pop_front() {
                return result;
            }

            let mut tasks = self.tasks.lock().unwrap();
            let task = tasks.pop_front();
            self.ready_hint.store(!tasks.is_empty(), Ordering::SeqCst);
            task.map(|task_cell| Pop {
                task_cell,
                schedule_time: Instant::now(),
                from_local: false,
            })
            .into()
        }

        fn drain(&self) {
            self.drain_count.fetch_add(1, Ordering::SeqCst);
            self.tasks.lock().unwrap().clear();
            self.scripted_results.lock().unwrap().clear();
            self.ready_hint.store(false, Ordering::SeqCst);
        }

        fn has_ready_task(&self) -> bool {
            self.ready_hint.load(Ordering::SeqCst)
        }
    }

    struct PendingQueue<T> {
        tasks: Mutex<VecDeque<T>>,
        pending_tx: Mutex<Option<mpsc::Sender<()>>>,
        drain_count: AtomicUsize,
    }

    impl<T> PendingQueue<T> {
        fn new(pending_tx: mpsc::Sender<()>) -> PendingQueue<T> {
            PendingQueue {
                tasks: Mutex::new(VecDeque::new()),
                pending_tx: Mutex::new(Some(pending_tx)),
                drain_count: AtomicUsize::new(0),
            }
        }

        fn len(&self) -> usize {
            self.tasks.lock().unwrap().len()
        }

        fn drain_count(&self) -> usize {
            self.drain_count.load(Ordering::SeqCst)
        }
    }

    impl<T: Send + 'static> TaskQueue<T> for PendingQueue<T> {
        fn push(&self, task_cell: T) {
            self.tasks.lock().unwrap().push_back(task_cell);
        }

        fn pop(&self) -> PopResult<T> {
            if self.tasks.lock().unwrap().is_empty() {
                return PopResult::Empty;
            }

            if let Some(tx) = self.pending_tx.lock().unwrap().take() {
                let _ = tx.send(());
            }
            PopResult::Pending {
                retry_at: Instant::now() + Duration::from_secs(60),
            }
        }

        fn drain(&self) {
            self.drain_count.fetch_add(1, Ordering::SeqCst);
            self.tasks.lock().unwrap().clear();
        }

        fn has_ready_task(&self) -> bool {
            false
        }
    }

    #[test]
    fn test_build_uses_shared_queue() {
        // Custom queues do not create separate per-worker queues. This test
        // verifies that the builder wires the injector and every local handle
        // to the same user-provided queue.
        let queue = Arc::new(MockQueue::default());
        let builder = Builder::new(Config::default(), queue);
        let (injector, mut locals) = builder.build(3);

        // The injector and all local handles wrap the same custom queue, so a
        // task pushed through the injector can be popped from any local handle.
        injector.push(MockTask::new(1));
        assert_eq!(locals[0].pop().unwrap_ready().task_cell.id, 1);

        // A push through one local handle also goes to the shared custom queue,
        // not to per-worker local storage.
        locals[1].push(MockTask::new(2));
        assert_eq!(locals[2].pop().unwrap_ready().task_cell.id, 2);
        assert!(locals[0].pop().is_empty());
    }

    #[test]
    fn test_pop_result_forwarding() {
        // Custom local queues should preserve the exact pop state reported by
        // the user-provided queue. The scheduler distinguishes Ready, Pending,
        // and Empty when deciding whether to run, retry later, or sleep.
        let queue = Arc::new(MockQueue::default());
        let retry_at = Instant::now() + Duration::from_millis(10);
        let schedule_time = Instant::now();
        queue.push_scripted_result(PopResult::Ready(Pop {
            task_cell: MockTask::new(1),
            schedule_time,
            from_local: true,
        }));
        queue.push_scripted_result(PopResult::Pending { retry_at });
        queue.push_scripted_result(PopResult::Empty);

        let builder = Builder::new(Config::default(), queue);
        let (_, mut locals) = builder.build(1);

        // Ready keeps the original Pop metadata.
        let pop = locals[0].pop().unwrap_ready();
        assert_eq!(pop.task_cell.id, 1);
        assert_eq!(pop.schedule_time, schedule_time);
        assert!(pop.from_local);
        // Pending and Empty must not be collapsed into each other.
        assert_eq!(locals[0].pop().unwrap_pending(), retry_at);
        locals[0].pop().unwrap_empty();
    }

    #[test]
    fn test_drain_forwards_to_task_queue() {
        // Shutdown drains through LocalQueue::drain, so custom queues must see
        // the drain call instead of relying on repeated readiness-aware pops.
        let queue = Arc::new(MockQueue::default());
        let builder = Builder::new(Config::default(), queue.clone());
        let (injector, mut locals) = builder.build(1);

        injector.push(MockTask::new(1));
        injector.push(MockTask::new(2));
        assert_eq!(queue.len(), 2);

        // Draining should clear all queued tasks, including tasks a custom
        // queue might otherwise report as Pending.
        locals[0].drain();
        assert!(queue.drain_count() >= 1);
        assert_eq!(queue.len(), 0);
        assert!(locals[0].pop().is_empty());
    }

    #[test]
    fn test_drain_clears_tasks_that_pop_reports_pending() {
        // A custom queue can own tasks that are not ready yet and report
        // Pending from pop. Shutdown still needs drain to drop those tasks.
        let queue = Arc::new(MockQueue::default());
        let builder = Builder::new(Config::default(), queue.clone());
        let (injector, mut locals) = builder.build(1);
        let retry_at = Instant::now() + Duration::from_secs(1);

        queue.push_scripted_result(PopResult::Pending { retry_at });
        injector.push(MockTask::new(1));

        assert!(locals[0].pop().is_pending());
        assert_eq!(queue.len(), 1);

        locals[0].drain();
        assert!(queue.drain_count() >= 1);
        assert_eq!(queue.len(), 0);
        assert!(locals[0].pop().is_empty());
    }

    #[test]
    fn test_drain_is_idempotent_when_called_by_shared_locals() {
        // Some queue implementations have local handles backed by shared queue
        // state. The custom adapter exposes this contract to the user-provided
        // queue by forwarding each local drain directly, so implementations must
        // tolerate repeated and concurrent calls while leaving the queue fully
        // cleared.
        let queue = Arc::new(MockQueue::default());
        let builder = Builder::new(Config::default(), queue.clone());
        let (injector, locals) = builder.build(4);
        let local_num = locals.len();
        let barrier = Arc::new(Barrier::new(local_num));

        for i in 0..16 {
            injector.push(MockTask::new(i));
        }
        assert_eq!(queue.len(), 16);

        let handles: Vec<_> = locals
            .into_iter()
            .map(|mut local| {
                let barrier = barrier.clone();
                thread::spawn(move || {
                    barrier.wait();
                    local.drain();
                    local.drain();
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        assert!(queue.drain_count() >= local_num);
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_has_tasks_or_pull_forwards_ready_hint() {
        // Custom queues provide their own non-consuming readiness hint for
        // preemption. The adapter should forward it without popping a task.
        let queue = Arc::new(MockQueue::default());
        let builder = Builder::new(Config::default(), queue.clone());
        let (injector, mut locals) = builder.build(1);

        injector.push(MockTask::new(1));
        queue.set_ready_hint(true);
        assert!(locals[0].has_tasks_or_pull());
        assert_eq!(queue.len(), 1);

        // A false hint should also be forwarded as-is, and checking it must
        // still leave queued tasks untouched.
        queue.set_ready_hint(false);
        assert!(!locals[0].has_tasks_or_pull());
        assert_eq!(queue.len(), 1);
    }

    #[test]
    fn test_default_extras_are_multilevel_for_custom_queue() {
        // Custom future pools reuse the tracked runner, so both remote spawns
        // and worker-local spawns need multilevel-compatible default extras.
        let queue = Arc::new(MockQueue::<MockTask>::default());
        let builder = Builder::new(Config::default(), queue);
        let (injector, locals) = builder.build(1);

        // Remote::spawn gets defaults from the injector.
        let injector_extras = injector.default_extras();
        assert!(injector_extras.running_time.is_some());
        assert_eq!(injector_extras.current_level(), 0);

        // Local::spawn gets defaults from the local queue handle.
        let local_extras = locals[0].default_extras();
        assert!(local_extras.running_time.is_some());
        assert_eq!(local_extras.current_level(), 0);
    }

    #[test]
    fn test_custom_metrics() {
        // A named custom queue reuses the tracked runner metrics. This verifies
        // that the custom runner builder wires execution and wait metrics the
        // same way as the built-in tracked queues.
        let name = "test_custom_metrics";
        let level0_elapsed = MULTILEVEL_LEVEL_ELAPSED
            .get_metric_with_label_values(&[name, "0"])
            .unwrap();
        let total_elapsed = MULTILEVEL_LEVEL_ELAPSED
            .get_metric_with_label_values(&[name, "total"])
            .unwrap();
        let wait_duration = TASK_WAIT_DURATION
            .get_metric_with_label_values(&[name])
            .unwrap();
        let exec_duration = TASK_EXEC_DURATION
            .get_metric_with_label_values(&[name])
            .unwrap();
        let poll_duration = TASK_POLL_DURATION
            .get_metric_with_label_values(&[name, "0"])
            .unwrap();
        let exec_times = TASK_EXEC_TIMES
            .get_metric_with_label_values(&[name])
            .unwrap();
        let level0_elapsed_before = level0_elapsed.get();
        let total_elapsed_before = total_elapsed.get();
        let wait_count_before = wait_duration.get_sample_count();
        let exec_duration_count_before = exec_duration.get_sample_count();
        let exec_duration_sum_before = exec_duration.get_sample_sum();
        let poll_duration_count_before = poll_duration.get_sample_count();
        let poll_duration_sum_before = poll_duration.get_sample_sum();
        let exec_times_count_before = exec_times.get_sample_count();
        let exec_times_sum_before = exec_times.get_sample_sum();
        let queue = Arc::new(MockQueue::default());
        let builder = Builder::new(Config::default().name(Some(name)), queue);
        let mut runner = builder.runner_builder(MockRunnerBuilder).build();
        let (remote, mut locals) = build_spawn(builder, Default::default());

        for i in 0..4 {
            remote.spawn(MockTask::with_sleep(i, 35));
        }
        while let PopResult::Ready(Pop { task_cell, .. }) = locals[0].pop() {
            assert!(runner.handle(&mut locals[0], task_cell));
        }
        runner.flush();

        // Explicitly flush local metrics so the assertions do not depend on
        // whether the elapsed-time threshold was crossed before the last task.
        assert!(level0_elapsed.get() - level0_elapsed_before > 100_000);
        assert!(total_elapsed.get() - total_elapsed_before > 100_000);
        assert_eq!(wait_duration.get_sample_count() - wait_count_before, 4);
        assert_eq!(
            exec_duration.get_sample_count() - exec_duration_count_before,
            4
        );
        assert!(exec_duration.get_sample_sum() - exec_duration_sum_before >= 0.1);
        assert_eq!(
            poll_duration.get_sample_count() - poll_duration_count_before,
            4
        );
        assert!(poll_duration.get_sample_sum() - poll_duration_sum_before >= 0.1);
        assert_eq!(exec_times.get_sample_count() - exec_times_count_before, 4);
        assert!(exec_times.get_sample_sum() - exec_times_sum_before >= 3.0);
    }

    #[test]
    fn test_build_custom_future_pool() {
        // Smoke test the public builder path: a custom TaskQueue should be
        // enough to build a future pool and execute a spawned future.
        let queue = Arc::new(MockQueue::default());
        let mut builder = crate::pool::Builder::new("test-custom-future-pool");
        builder
            .min_thread_count(1)
            .max_thread_count(1)
            .core_thread_count(1);
        let pool = builder.build_custom_future_pool(queue);
        let (tx, rx) = mpsc::channel();

        pool.spawn(async move {
            tx.send(()).unwrap();
        });

        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        pool.shutdown();
    }

    #[test]
    fn test_custom_callback_pool_supports_worker_local_spawn() {
        // This covers the worker-local spawn path. A task running on a worker
        // can create a new task through Handle::spawn, which gets default
        // extras from the local custom queue handle before the tracked runner
        // executes it.
        let queue: Arc<dyn TaskQueue<crate::task::callback::TaskCell>> =
            Arc::new(MockQueue::default());
        let queue_builder = Builder::new(Config::default(), queue);
        let runner_builder = queue_builder.runner_builder(crate::pool::CloneRunnerBuilder(
            crate::task::callback::Runner::default(),
        ));
        let mut pool_builder = crate::pool::Builder::new("test-custom-callback-pool");
        pool_builder
            .min_thread_count(1)
            .max_thread_count(1)
            .core_thread_count(1);
        let pool = pool_builder.build_with_queue_and_runner(queue_builder.into(), runner_builder);
        let (tx, rx) = mpsc::channel();
        let child_tx = tx.clone();

        pool.spawn(move |handle: &mut crate::task::callback::Handle<'_>| {
            tx.send(1).unwrap();
            handle.spawn(move |_: &mut crate::task::callback::Handle<'_>| {
                child_tx.send(2).unwrap();
            });
        });

        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
        assert_eq!(rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
        pool.shutdown();
    }

    #[test]
    fn test_shutdown_drains_pending_custom_queue() {
        // This is the real worker shutdown path. The custom queue owns a task
        // but reports it as Pending, so the worker must not rely on pop() to
        // drain the queue when the pool shuts down.
        let (pending_tx, pending_rx) = mpsc::channel();
        let queue = Arc::new(PendingQueue::new(pending_tx));
        let mut builder = crate::pool::Builder::new("test-shutdown-drains-pending-custom-queue");
        builder
            .min_thread_count(1)
            .max_thread_count(1)
            .core_thread_count(1);
        let pool = builder.build_custom_future_pool(queue.clone());

        pool.spawn(async {});

        // Wait until the worker has observed Pending. This makes the test check
        // shutdown-from-pending rather than shutdown-before-the-worker-runs.
        pending_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(queue.len(), 1);

        pool.shutdown();
        assert!(queue.drain_count() >= 1);
        assert_eq!(queue.len(), 0);
    }
}
