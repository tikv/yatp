// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::{Local, Runner};
use crate::queue::{Pop, PopResult, TaskCell};
use parking_lot_core::SpinWait;

pub(crate) struct WorkerThread<T, R> {
    local: Local<T>,
    runner: R,
}

impl<T, R> WorkerThread<T, R> {
    pub fn new(local: Local<T>, runner: R) -> WorkerThread<T, R> {
        WorkerThread { local, runner }
    }
}

impl<T, R> WorkerThread<T, R>
where
    T: TaskCell + Send,
    R: Runner<TaskCell = T>,
{
    #[inline]
    fn pop(&mut self) -> Option<Pop<T>> {
        // Wait some time before going to sleep, which is more expensive.
        let mut spin = SpinWait::new();
        let initial_retry_at = loop {
            let retry_at = match self.local.pop() {
                PopResult::Ready(task) => return Some(task),
                PopResult::Pending { retry_at } => Some(retry_at),
                PopResult::Empty => None,
            };
            if !spin.spin() {
                break retry_at;
            }
        };
        self.runner.pause(&mut self.local);
        let t = self.local.pop_or_sleep(initial_retry_at);
        self.runner.resume(&mut self.local);
        t
    }

    pub fn run(mut self) {
        self.runner.start(&mut self.local);
        while !self.local.core().is_shutdown() {
            let task = match self.pop() {
                Some(t) => t,
                None => continue,
            };
            self.runner.handle(&mut self.local, task.task_cell);
        }
        self.runner.end(&mut self.local);

        // Drain all futures in the queue
        self.local.drain();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::spawn::*;
    use crate::pool::SchedConfig;
    use crate::queue::{CustomBuilder, CustomConfig, Extras, QueueType, TaskQueue};
    use crate::task::callback;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::*;
    use std::thread;
    use std::thread::JoinHandle;
    use std::time::*;

    #[derive(Default, PartialEq, Debug)]
    struct Metrics {
        start: usize,
        handle: usize,
        pause: usize,
        resume: usize,
        end: usize,
    }

    struct Runner {
        runner: callback::Runner,
        metrics: Arc<Mutex<Metrics>>,
        tx: mpsc::Sender<()>,
    }

    impl crate::pool::Runner for Runner {
        type TaskCell = callback::TaskCell;

        fn start(&mut self, local: &mut Local<Self::TaskCell>) {
            self.metrics.lock().unwrap().start += 1;
            self.runner.start(local);
        }

        fn handle(&mut self, local: &mut Local<Self::TaskCell>, t: Self::TaskCell) -> bool {
            self.metrics.lock().unwrap().handle += 1;
            self.runner.handle(local, t)
        }

        /// Called when the runner is put to sleep.
        fn pause(&mut self, local: &mut Local<Self::TaskCell>) -> bool {
            self.metrics.lock().unwrap().pause += 1;
            let b = self.runner.pause(local);
            let _ = self.tx.send(());
            b
        }

        /// Called when the runner is woken up.
        fn resume(&mut self, local: &mut Local<Self::TaskCell>) {
            self.metrics.lock().unwrap().resume += 1;
            self.runner.resume(local)
        }

        /// Called when the runner is about to be destroyed.
        ///
        /// It's guaranteed that no other method will be called after this method.
        fn end(&mut self, local: &mut Local<Self::TaskCell>) {
            self.metrics.lock().unwrap().end += 1;
            self.runner.end(local)
        }
    }

    struct TestTask {
        id: usize,
        extras: Extras,
    }

    impl TestTask {
        fn new(id: usize) -> TestTask {
            TestTask {
                id,
                extras: Extras::multilevel_default(),
            }
        }
    }

    impl TaskCell for TestTask {
        fn mut_extras(&mut self) -> &mut Extras {
            &mut self.extras
        }
    }

    struct ScriptedQueue<T> {
        scripted_results: Mutex<VecDeque<PopResult<T>>>,
        pushed_tasks: Mutex<VecDeque<T>>,
        push_tx: Mutex<Option<mpsc::Sender<()>>>,
    }

    impl<T> ScriptedQueue<T> {
        fn new(scripted_results: Vec<PopResult<T>>) -> ScriptedQueue<T> {
            ScriptedQueue {
                scripted_results: Mutex::new(scripted_results.into()),
                pushed_tasks: Mutex::new(VecDeque::new()),
                push_tx: Mutex::new(None),
            }
        }

        fn with_push_signal(
            scripted_results: Vec<PopResult<T>>,
            push_tx: mpsc::Sender<()>,
        ) -> ScriptedQueue<T> {
            ScriptedQueue {
                scripted_results: Mutex::new(scripted_results.into()),
                pushed_tasks: Mutex::new(VecDeque::new()),
                push_tx: Mutex::new(Some(push_tx)),
            }
        }
    }

    impl ScriptedQueue<TestTask> {
        fn ready(id: usize) -> PopResult<TestTask> {
            PopResult::Ready(Pop {
                task_cell: TestTask::new(id),
                schedule_time: Instant::now(),
                from_local: false,
            })
        }
    }

    impl<T: Send + 'static> TaskQueue<T> for ScriptedQueue<T> {
        fn push(&self, task_cell: T) {
            self.pushed_tasks.lock().unwrap().push_back(task_cell);
            if let Some(push_tx) = self.push_tx.lock().unwrap().as_ref() {
                let _ = push_tx.send(());
            }
        }

        fn pop(&self) -> PopResult<T> {
            if let Some(result) = self.scripted_results.lock().unwrap().pop_front() {
                return result;
            }
            self.pushed_tasks
                .lock()
                .unwrap()
                .pop_front()
                .map(|task_cell| Pop {
                    task_cell,
                    schedule_time: Instant::now(),
                    from_local: false,
                })
                .into()
        }

        fn drain(&self) {
            self.scripted_results.lock().unwrap().clear();
            self.pushed_tasks.lock().unwrap().clear();
        }

        fn has_ready_task(&self) -> bool {
            !self.scripted_results.lock().unwrap().is_empty()
                || !self.pushed_tasks.lock().unwrap().is_empty()
        }
    }

    fn one_thread_config() -> SchedConfig {
        SchedConfig {
            min_thread_count: 1,
            max_thread_count: 1,
            core_thread_count: AtomicUsize::new(1),
            ..Default::default()
        }
    }

    fn two_thread_config() -> SchedConfig {
        SchedConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            core_thread_count: AtomicUsize::new(2),
            ..Default::default()
        }
    }

    fn build_scripted_local(queue: Arc<ScriptedQueue<TestTask>>) -> Local<TestTask> {
        let queue_builder = CustomBuilder::new(CustomConfig::default(), queue);
        let (_, mut locals) = build_spawn(queue_builder, one_thread_config());
        locals.remove(0)
    }

    fn assert_next_ready_task(local: &mut Local<TestTask>, id: usize) {
        assert_eq!(local.pop().unwrap_ready().task_cell.id, id);
    }

    fn callback_task(
        task: impl FnOnce(&mut callback::Handle<'_>) + Send + 'static,
    ) -> callback::TaskCell {
        callback::TaskCell {
            task: callback::Task::new_once(task),
            extras: Extras::multilevel_default(),
        }
    }

    const WORKER_SPIN_POP_COUNT: usize = 11;
    const DELAYED_TASK_DELAY: Duration = Duration::from_millis(50);
    const MAX_DELAYED_TASK_LAG: Duration = Duration::from_secs(2);
    const LATER_RETRY_OFFSET: Duration = Duration::from_secs(10);

    #[derive(Clone, Copy)]
    enum DelayedQueueScenario {
        PendingDuringSpinAndValidate,
        EmptyDuringSpin,
        EarlierRetryInValidate,
    }

    enum DeadlineScript {
        Empty,
        Pending(Instant),
        PendingAfter(Duration),
    }

    struct DeadlineTask<T> {
        task_cell: T,
        ready_at: Instant,
    }

    struct DeadlineQueueState<T> {
        delays: VecDeque<Duration>,
        ready_ats: Vec<Instant>,
        scripted_results: VecDeque<DeadlineScript>,
        tasks: VecDeque<DeadlineTask<T>>,
    }

    struct DeadlineQueue<T> {
        state: Mutex<DeadlineQueueState<T>>,
    }

    struct AlwaysPendingQueue {
        retry_at: Instant,
    }

    impl TaskQueue<callback::TaskCell> for AlwaysPendingQueue {
        fn push(&self, _: callback::TaskCell) {}

        fn pop(&self) -> PopResult<callback::TaskCell> {
            PopResult::Pending {
                retry_at: self.retry_at,
            }
        }

        fn drain(&self) {}

        fn has_ready_task(&self) -> bool {
            false
        }
    }

    impl<T> DeadlineQueue<T> {
        fn new(delays: Vec<Duration>) -> DeadlineQueue<T> {
            DeadlineQueue {
                state: Mutex::new(DeadlineQueueState {
                    delays: delays.into(),
                    ready_ats: Vec::new(),
                    scripted_results: VecDeque::new(),
                    tasks: VecDeque::new(),
                }),
            }
        }

        fn push_scripted_result(&self, result: DeadlineScript) {
            self.state
                .lock()
                .unwrap()
                .scripted_results
                .push_back(result);
        }

        fn ready_at(&self, index: usize) -> Instant {
            self.state.lock().unwrap().ready_ats[index]
        }
    }

    impl<T: Send + 'static> TaskQueue<T> for DeadlineQueue<T> {
        fn push(&self, task_cell: T) {
            let mut state = self.state.lock().unwrap();
            let delay = state.delays.pop_front().unwrap();
            let ready_at = Instant::now() + delay;
            state.ready_ats.push(ready_at);
            state.tasks.push_back(DeadlineTask {
                task_cell,
                ready_at,
            });
        }

        fn pop(&self) -> PopResult<T> {
            let mut state = self.state.lock().unwrap();
            if let Some(result) = state.scripted_results.pop_front() {
                return match result {
                    DeadlineScript::Empty => PopResult::Empty,
                    DeadlineScript::Pending(retry_at) => PopResult::Pending { retry_at },
                    DeadlineScript::PendingAfter(delay) => {
                        let retry_at = Instant::now() + delay;
                        let index = state
                            .tasks
                            .iter()
                            .enumerate()
                            .min_by_key(|(_, task)| task.ready_at)
                            .map(|(index, _)| index);
                        if let Some(index) = index {
                            state.tasks[index].ready_at = retry_at;
                            state.ready_ats[index] = retry_at;
                        }
                        PopResult::Pending { retry_at }
                    }
                };
            }

            let (index, ready_at) = match state
                .tasks
                .iter()
                .enumerate()
                .min_by_key(|(_, task)| task.ready_at)
                .map(|(index, task)| (index, task.ready_at))
            {
                Some(task) => task,
                None => return PopResult::Empty,
            };

            if Instant::now() < ready_at {
                return PopResult::Pending { retry_at: ready_at };
            }

            let task = state.tasks.remove(index).unwrap();
            PopResult::Ready(Pop {
                task_cell: task.task_cell,
                schedule_time: task.ready_at,
                from_local: false,
            })
        }

        fn drain(&self) {
            let mut state = self.state.lock().unwrap();
            state.scripted_results.clear();
            state.tasks.clear();
        }

        fn has_ready_task(&self) -> bool {
            self.state
                .lock()
                .unwrap()
                .tasks
                .iter()
                .any(|task| Instant::now() >= task.ready_at)
        }
    }

    fn ready_callback_pop(tx: mpsc::Sender<Instant>) -> PopResult<callback::TaskCell> {
        PopResult::Ready(Pop {
            task_cell: callback_task(move |_: &mut callback::Handle<'_>| {
                tx.send(Instant::now()).unwrap();
            }),
            schedule_time: Instant::now(),
            from_local: false,
        })
    }

    fn check_scripted_worker_runs_task(
        queue: Arc<ScriptedQueue<callback::TaskCell>>,
        done_rx: mpsc::Receiver<Instant>,
    ) -> Instant {
        let (remote, _pause_rx, metrics, handle) = build_custom_worker(queue);
        let executed_value = done_rx
            .recv_timeout(MAX_DELAYED_TASK_LAG + Duration::from_secs(1))
            .unwrap();

        {
            let metrics = metrics.lock().unwrap();
            assert_eq!(metrics.start, 1);
            assert_eq!(metrics.handle, 1);
            assert!(metrics.pause >= 1);
            assert!(metrics.resume >= 1);
        }

        remote.stop();
        handle.join().unwrap();
        assert_eq!(metrics.lock().unwrap().end, 1);

        executed_value
    }

    fn build_custom_worker(
        queue: Arc<dyn TaskQueue<callback::TaskCell>>,
    ) -> (
        Remote<callback::TaskCell>,
        mpsc::Receiver<()>,
        Arc<Mutex<Metrics>>,
        JoinHandle<()>,
    ) {
        let queue_builder = CustomBuilder::new(CustomConfig::default(), queue);
        let (remote, mut locals) = build_spawn(queue_builder, one_thread_config());
        let (pause_rx, metrics, handle) = start_custom_worker(locals.remove(0));

        (remote, pause_rx, metrics, handle)
    }

    fn start_custom_worker(
        local: Local<callback::TaskCell>,
    ) -> (mpsc::Receiver<()>, Arc<Mutex<Metrics>>, JoinHandle<()>) {
        let (pause_tx, pause_rx) = mpsc::channel();
        let metrics = Arc::new(Mutex::new(Metrics::default()));
        let runner = Runner {
            runner: callback::Runner::default(),
            metrics: metrics.clone(),
            tx: pause_tx,
        };
        let worker = WorkerThread::new(local, runner);
        let handle = thread::spawn(move || worker.run());

        (pause_rx, metrics, handle)
    }

    fn check_worker_runs_ready_task_inserted_while_pending() {
        let _lock = lock_failpoint_tests();
        let queue = Arc::new(DeadlineQueue::new(vec![LATER_RETRY_OFFSET, Duration::ZERO]));
        let _guard = fail::FailScenario::setup();
        let (entered_rx, release_tx) =
            configure_blocking_failpoint("worker-pop-or-sleep-before-sleep");

        let (unexpected_tx, unexpected_rx) = mpsc::channel();
        queue.push(callback_task(move |_: &mut callback::Handle<'_>| {
            unexpected_tx.send(()).unwrap();
        }));
        let pending_retry_at = queue.ready_at(0);
        let (remote, _pause_rx, metrics, handle) = build_custom_worker(queue.clone());
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let (done_tx, done_rx) = mpsc::channel();
        let ready_inserted_at = Instant::now();
        remote.spawn(move |_: &mut callback::Handle<'_>| {
            done_tx.send(Instant::now()).unwrap();
        });
        release_tx.send(()).unwrap();

        let executed_at = done_rx.recv_timeout(MAX_DELAYED_TASK_LAG).unwrap();
        assert!(executed_at >= ready_inserted_at);
        assert!(executed_at.duration_since(ready_inserted_at) <= MAX_DELAYED_TASK_LAG);
        assert!(executed_at < pending_retry_at);
        assert!(unexpected_rx.try_recv().is_err());
        {
            let metrics = metrics.lock().unwrap();
            assert_eq!(metrics.start, 1);
            assert_eq!(metrics.handle, 1);
        }

        remote.stop();
        handle.join().unwrap();
        {
            let metrics = metrics.lock().unwrap();
            assert_eq!(metrics.handle, 1);
            assert_eq!(metrics.end, 1);
        }
    }

    fn configure_blocking_failpoint(name: &'static str) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let entered_tx = Arc::new(Mutex::new(Some(entered_tx)));
        let release_rx = Arc::new(Mutex::new(Some(release_rx)));
        let fired = Arc::new(AtomicBool::new(false));
        fail::cfg_callback(name, move || {
            if fired.swap(true, Ordering::SeqCst) {
                return;
            }
            if let Ok(mut entered_tx) = entered_tx.lock() {
                if let Some(entered_tx) = entered_tx.take() {
                    let _ = entered_tx.send(());
                }
            }
            if let Ok(mut release_rx) = release_rx.lock() {
                if let Some(release_rx) = release_rx.take() {
                    let _ = release_rx.recv_timeout(Duration::from_secs(3));
                }
            }
        })
        .unwrap();

        (entered_rx, release_tx)
    }

    fn configure_counting_failpoint(
        name: &'static str,
        blocked_count: usize,
    ) -> (mpsc::Receiver<usize>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let count = Arc::new(AtomicUsize::new(0));

        fail::cfg_callback(name, move || {
            let current = count.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = entered_tx.send(current);
            if current <= blocked_count {
                let _ = release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(3));
            }
        })
        .unwrap();

        (entered_rx, release_tx)
    }

    fn lock_failpoint_tests() -> std::sync::MutexGuard<'static, ()> {
        static FAILPOINT_TEST_LOCK: Mutex<()> = Mutex::new(());
        FAILPOINT_TEST_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn spawn_task_and_wait_until_pushed(
        remote: Remote<callback::TaskCell>,
        push_rx: &mpsc::Receiver<()>,
        done_tx: mpsc::Sender<usize>,
        value: usize,
    ) -> JoinHandle<()> {
        let handle = thread::spawn(move || {
            remote.spawn(move |_: &mut callback::Handle<'_>| {
                done_tx.send(value).unwrap();
            });
        });
        push_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        handle
    }

    fn check_worker_wakes_when_task_is_inserted_at(failpoint: &'static str) {
        let _lock = lock_failpoint_tests();
        let _guard = fail::FailScenario::setup();
        let (entered_rx, release_tx) = configure_blocking_failpoint(failpoint);
        let (push_tx, push_rx) = mpsc::channel();
        let queue = Arc::new(ScriptedQueue::with_push_signal(Vec::new(), push_tx));
        let (remote, pause_rx, metrics, handle) = build_custom_worker(queue);

        // The first pause means the worker has already observed Empty during
        // spin and is about to enter pop_or_sleep.
        pause_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();

        let (done_tx, done_rx) = mpsc::channel();
        let spawn_handle = spawn_task_and_wait_until_pushed(remote.clone(), &push_rx, done_tx, 42);
        release_tx.send(()).unwrap();

        assert_eq!(done_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 42);
        {
            let metrics = metrics.lock().unwrap();
            assert_eq!(metrics.start, 1);
            assert_eq!(metrics.handle, 1);
            assert!(metrics.resume >= 1);
            assert!(metrics.pause >= 1);
        }
        spawn_handle.join().unwrap();
        remote.stop();
        handle.join().unwrap();
        {
            let metrics = metrics.lock().unwrap();
            assert_eq!(metrics.start, 1);
            assert_eq!(metrics.handle, 1);
            assert_eq!(metrics.end, 1);
            assert!(metrics.pause >= 1);
            assert!(metrics.resume >= 1);
        }
    }

    #[test]
    fn test_hooks() {
        let (tx, rx) = mpsc::channel();
        let r = Runner {
            runner: callback::Runner::default(),
            metrics: Default::default(),
            tx: tx.clone(),
        };
        let metrics = r.metrics.clone();
        let mut expected_metrics = Metrics::default();
        let mut config: SchedConfig = Default::default();
        config.core_thread_count = AtomicUsize::new(config.max_thread_count);
        let (injector, mut locals) = build_spawn(QueueType::SingleLevel, config);
        let th = WorkerThread::new(locals.remove(0), r);
        let handle = std::thread::spawn(move || {
            th.run();
        });
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        expected_metrics.start = 1;
        expected_metrics.pause = 1;
        assert_eq!(expected_metrics, *metrics.lock().unwrap());

        injector.spawn(move |_: &mut callback::Handle<'_>| {});
        rx.recv_timeout(Duration::from_secs(1)).unwrap();
        expected_metrics.pause = 2;
        expected_metrics.handle = 1;
        expected_metrics.resume = 1;
        assert_eq!(expected_metrics, *metrics.lock().unwrap());

        injector.stop();
        handle.join().unwrap();
        expected_metrics.resume = 2;
        expected_metrics.end = 1;
        assert_eq!(expected_metrics, *metrics.lock().unwrap());
    }

    #[test]
    fn test_pop_or_sleep_uses_pending_retry_from_validate() {
        let retry_at = Instant::now() + Duration::from_millis(20);
        let queue = Arc::new(ScriptedQueue::new(vec![
            PopResult::Pending { retry_at },
            PopResult::Pending { retry_at },
            ScriptedQueue::ready(1),
        ]));
        let mut local = build_scripted_local(queue);

        assert!(local.pop_or_sleep(None).is_none());
        assert!(Instant::now() >= retry_at);
        assert_next_ready_task(&mut local, 1);
    }

    #[test]
    fn test_pop_or_sleep_uses_initial_retry_when_validate_empty() {
        let retry_at = Instant::now() + Duration::from_millis(20);
        let queue = Arc::new(ScriptedQueue::new(vec![
            PopResult::Empty,
            ScriptedQueue::ready(1),
        ]));
        let mut local = build_scripted_local(queue);

        assert!(local.pop_or_sleep(Some(retry_at)).is_none());
        assert!(Instant::now() >= retry_at);
        assert_next_ready_task(&mut local, 1);
    }

    #[test]
    fn test_pop_or_sleep_returns_ready_from_validate_without_sleeping() {
        let retry_at = Instant::now() + LATER_RETRY_OFFSET;
        let queue = Arc::new(ScriptedQueue::new(vec![ScriptedQueue::ready(1)]));
        let mut local = build_scripted_local(queue);

        let pop = local.pop_or_sleep(Some(retry_at)).unwrap();
        assert_eq!(pop.task_cell.id, 1);
        assert!(Instant::now() < retry_at);
    }

    #[test]
    fn test_pop_or_sleep_uses_min_retry_when_validate_pending() {
        let earlier_retry_at = Instant::now() + Duration::from_millis(20);
        let later_retry_at = Instant::now() + LATER_RETRY_OFFSET;
        let queue = Arc::new(ScriptedQueue::new(vec![
            PopResult::Pending {
                retry_at: earlier_retry_at,
            },
            PopResult::Pending {
                retry_at: earlier_retry_at,
            },
            ScriptedQueue::ready(1),
        ]));
        let mut local = build_scripted_local(queue);

        assert!(local.pop_or_sleep(Some(later_retry_at)).is_none());
        assert!(Instant::now() >= earlier_retry_at);
        assert!(Instant::now() < later_retry_at);
        assert_next_ready_task(&mut local, 1);

        let earlier_retry_at = Instant::now() + Duration::from_millis(20);
        let later_retry_at = Instant::now() + LATER_RETRY_OFFSET;
        let queue = Arc::new(ScriptedQueue::new(vec![
            PopResult::Pending {
                retry_at: later_retry_at,
            },
            ScriptedQueue::ready(2),
        ]));
        let mut local = build_scripted_local(queue);

        assert!(local.pop_or_sleep(Some(earlier_retry_at)).is_none());
        assert!(Instant::now() >= earlier_retry_at);
        assert!(Instant::now() < later_retry_at);
        assert_next_ready_task(&mut local, 2);
    }

    #[test]
    fn test_worker_retries_immediately_when_retry_time_has_passed() {
        // A custom queue may report a stale retry time. The worker should not
        // block for such a Pending result; it should retry immediately and run
        // the task once the queue reports it as Ready.
        let retry_at = Instant::now() - Duration::from_millis(10);
        let (done_tx, done_rx) = mpsc::channel();
        let mut results = Vec::new();
        for _ in 0..WORKER_SPIN_POP_COUNT {
            results.push(PopResult::Pending { retry_at });
        }
        results.push(PopResult::Pending { retry_at });
        results.push(ready_callback_pop(done_tx));
        let queue = Arc::new(ScriptedQueue::new(results));

        let started_at = Instant::now();
        let executed_at = check_scripted_worker_runs_task(queue, done_rx);
        assert!(executed_at.duration_since(started_at) <= MAX_DELAYED_TASK_LAG);
    }

    #[test]
    fn test_worker_uses_last_spin_retry_when_it_gets_shorter() {
        // The spin loop should pass its last observed retry time into
        // pop_or_sleep. If the last observation gets shorter, the worker should
        // wake at the shorter deadline instead of an older longer one.
        let earlier_retry_at = Instant::now() + Duration::from_millis(50);
        let later_retry_at = Instant::now() + LATER_RETRY_OFFSET;
        let (done_tx, done_rx) = mpsc::channel();
        let mut results = Vec::new();
        for _ in 1..WORKER_SPIN_POP_COUNT {
            results.push(PopResult::Pending {
                retry_at: later_retry_at,
            });
        }
        results.push(PopResult::Pending {
            retry_at: earlier_retry_at,
        });
        results.push(PopResult::Empty);
        results.push(ready_callback_pop(done_tx));
        let queue = Arc::new(ScriptedQueue::new(results));

        let executed_at = check_scripted_worker_runs_task(queue, done_rx);
        assert!(executed_at >= earlier_retry_at);
        assert!(executed_at < later_retry_at);
    }

    #[test]
    fn test_worker_uses_last_spin_retry_when_it_gets_longer() {
        // Conversely, if the last spin observation gets longer, the worker
        // should not keep a stale shorter retry time.
        let earlier_retry_at = Instant::now() + Duration::from_millis(20);
        let later_retry_at = Instant::now() + Duration::from_millis(100);
        let (done_tx, done_rx) = mpsc::channel();
        let mut results = Vec::new();
        for _ in 1..WORKER_SPIN_POP_COUNT {
            results.push(PopResult::Pending {
                retry_at: earlier_retry_at,
            });
        }
        results.push(PopResult::Pending {
            retry_at: later_retry_at,
        });
        results.push(PopResult::Empty);
        results.push(ready_callback_pop(done_tx));
        let queue = Arc::new(ScriptedQueue::new(results));

        let executed_at = check_scripted_worker_runs_task(queue, done_rx);
        assert!(executed_at >= later_retry_at);
        assert!(executed_at.duration_since(later_retry_at) <= MAX_DELAYED_TASK_LAG);
    }

    fn check_worker_stops_at_pop_or_sleep_failpoint(failpoint: &'static str, stop_in_thread: bool) {
        let _lock = lock_failpoint_tests();
        let _guard = fail::FailScenario::setup();
        let (entered_rx, release_tx) = configure_blocking_failpoint(failpoint);
        let queue = Arc::new(ScriptedQueue::new(Vec::new()));
        let (remote, _pause_rx, metrics, handle) = build_custom_worker(queue);

        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let stop_handle = if stop_in_thread {
            let remote = remote.clone();
            Some(thread::spawn(move || remote.stop()))
        } else {
            remote.stop();
            None
        };
        release_tx.send(()).unwrap();
        if let Some(stop_handle) = stop_handle {
            stop_handle.join().unwrap();
        }
        handle.join().unwrap();

        let metrics = metrics.lock().unwrap();
        assert_eq!(metrics.start, 1);
        assert_eq!(metrics.handle, 0);
        assert_eq!(metrics.end, 1);
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_worker_stops_when_shutdown_before_mark_sleep() {
        // Shutdown before mark_sleep should make validate fail without
        // decrementing the active worker count.
        check_worker_stops_at_pop_or_sleep_failpoint("worker-pop-or-sleep-before-park", false);
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_worker_stops_when_shutdown_during_validate() {
        // Shutdown while validate is running happens after mark_sleep has
        // succeeded. The worker should still return from park and finish.
        check_worker_stops_at_pop_or_sleep_failpoint(
            "worker-pop-or-sleep-before-validate-pop",
            true,
        );
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_worker_wakes_ready_task_after_pending_then_validate_empty() {
        // The worker carries an initial Pending retry from spin, validate then
        // sees Empty and parks with that timeout. A newly inserted ready task
        // should still wake the worker immediately instead of waiting for the
        // old retry deadline.
        let _lock = lock_failpoint_tests();
        let _guard = fail::FailScenario::setup();
        let (entered_rx, release_tx) =
            configure_blocking_failpoint("worker-pop-or-sleep-before-sleep");
        let retry_at = Instant::now() + LATER_RETRY_OFFSET;
        let mut results = Vec::new();
        for _ in 0..WORKER_SPIN_POP_COUNT {
            results.push(PopResult::Pending { retry_at });
        }
        results.push(PopResult::Empty);
        let queue = Arc::new(ScriptedQueue::new(results));
        let (remote, _pause_rx, metrics, handle) = build_custom_worker(queue);

        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let inserted_at = Instant::now();
        remote.spawn(move |_: &mut callback::Handle<'_>| {
            done_tx.send(Instant::now()).unwrap();
        });
        release_tx.send(()).unwrap();

        let executed_at = done_rx.recv_timeout(MAX_DELAYED_TASK_LAG).unwrap();
        assert!(executed_at >= inserted_at);
        assert!(executed_at.duration_since(inserted_at) <= MAX_DELAYED_TASK_LAG);
        assert!(executed_at < retry_at);

        remote.stop();
        handle.join().unwrap();
        let metrics = metrics.lock().unwrap();
        assert_eq!(metrics.start, 1);
        assert_eq!(metrics.handle, 1);
        assert_eq!(metrics.end, 1);
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_worker_runs_ready_task_inserted_while_pending() {
        // The worker is sleeping for a delayed task's Pending retry. A later
        // ready task should wake it and run immediately, without waiting for
        // the delayed task's retry time.
        check_worker_runs_ready_task_inserted_while_pending();
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_worker_refreshes_timeout_when_earlier_pending_task_is_inserted() {
        // The worker is already queued to sleep with a later Pending retry.
        // Pushing another delayed task with an earlier retry should wake it so
        // the worker can replace the old timeout with the earlier deadline.
        let _lock = lock_failpoint_tests();
        let _guard = fail::FailScenario::setup();
        let (entered_rx, release_tx) =
            configure_blocking_failpoint("worker-pop-or-sleep-before-sleep");
        let queue = Arc::new(DeadlineQueue::new(vec![
            LATER_RETRY_OFFSET,
            DELAYED_TASK_DELAY,
        ]));
        let (unexpected_tx, unexpected_rx) = mpsc::channel();
        queue.push(callback_task(move |_: &mut callback::Handle<'_>| {
            unexpected_tx.send(()).unwrap();
        }));
        let later_retry_at = queue.ready_at(0);
        let (remote, _pause_rx, metrics, handle) = build_custom_worker(queue.clone());
        entered_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        release_tx.send(()).unwrap();

        // Let the worker pass the before_sleep hook and block on the later
        // timeout before pushing the earlier delayed task.
        thread::sleep(Duration::from_millis(20));
        let (done_tx, done_rx) = mpsc::channel();
        remote.spawn(move |_: &mut callback::Handle<'_>| {
            done_tx.send(Instant::now()).unwrap();
        });
        let earlier_retry_at = queue.ready_at(1);

        let executed_at = done_rx
            .recv_timeout(MAX_DELAYED_TASK_LAG + Duration::from_secs(1))
            .unwrap();
        assert!(executed_at >= earlier_retry_at);
        assert!(executed_at.duration_since(earlier_retry_at) <= MAX_DELAYED_TASK_LAG);
        assert!(executed_at < later_retry_at);
        assert!(unexpected_rx.try_recv().is_err());

        remote.stop();
        handle.join().unwrap();
        let metrics = metrics.lock().unwrap();
        assert_eq!(metrics.start, 1);
        assert_eq!(metrics.handle, 1);
        assert_eq!(metrics.end, 1);
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_scaled_down_worker_reparks_after_pending_timeout() {
        // A worker that becomes above core_thread_count while carrying a
        // Pending timeout should not return to the outer spin-pop path when
        // the timeout fires. It should clear the timeout and park again.
        let _lock = lock_failpoint_tests();
        let _guard = fail::FailScenario::setup();
        let (sleep_rx, release_tx) =
            configure_counting_failpoint("worker-pop-or-sleep-before-sleep", 2);
        let queue = Arc::new(AlwaysPendingQueue {
            retry_at: Instant::now() + Duration::from_millis(20),
        });
        let queue_builder = CustomBuilder::new(CustomConfig::default(), queue);
        let (remote, mut locals) = build_spawn(queue_builder, two_thread_config());
        let (pause_rx, metrics, handle) = start_custom_worker(locals.remove(1));

        pause_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(sleep_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
        remote.scale_workers(1);
        release_tx.send(()).unwrap();

        assert_eq!(sleep_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 2);
        {
            let metrics = metrics.lock().unwrap();
            assert_eq!(metrics.pause, 1);
            assert_eq!(metrics.resume, 0);
        }
        release_tx.send(()).unwrap();
        thread::sleep(Duration::from_millis(20));

        remote.stop();
        handle.join().unwrap();
        let metrics = metrics.lock().unwrap();
        assert_eq!(metrics.start, 1);
        assert_eq!(metrics.handle, 0);
        assert_eq!(metrics.pause, 1);
        assert_eq!(metrics.resume, 1);
        assert_eq!(metrics.end, 1);
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_scaled_down_pending_timeout_wakes_core_worker() {
        // Worker 1 parks on an empty queue without a timeout. A delayed task is
        // then inserted directly into the custom queue so no push wakeup is
        // sent. Worker 2 observes the Pending timeout, gets scaled down, and
        // must wake worker 1 when the timeout fires.
        let _lock = lock_failpoint_tests();
        let _guard = fail::FailScenario::setup();
        let (sleep_rx, _) = configure_counting_failpoint("worker-pop-or-sleep-before-sleep", 0);
        let queue = Arc::new(DeadlineQueue::new(vec![Duration::from_millis(100)]));
        let queue_builder = CustomBuilder::new(CustomConfig::default(), queue.clone());
        let (remote, mut locals) = build_spawn(queue_builder, two_thread_config());
        let local_2 = locals.remove(1);
        let local_1 = locals.remove(0);
        let (pause_rx_1, metrics_1, handle_1) = start_custom_worker(local_1);

        pause_rx_1.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(sleep_rx.recv_timeout(Duration::from_secs(1)).unwrap(), 1);
        fail::remove("worker-pop-or-sleep-before-sleep");
        thread::sleep(Duration::from_millis(20));

        let (done_tx, done_rx) = mpsc::channel();
        queue.push(callback_task(move |_: &mut callback::Handle<'_>| {
            done_tx.send(Instant::now()).unwrap();
        }));
        let ready_at = queue.ready_at(0);
        let (pause_rx_2, metrics_2, handle_2) = start_custom_worker(local_2);

        pause_rx_2.recv_timeout(Duration::from_secs(1)).unwrap();
        remote.scale_workers(1);

        let executed_at = done_rx
            .recv_timeout(MAX_DELAYED_TASK_LAG + Duration::from_secs(1))
            .unwrap();
        assert!(executed_at >= ready_at);
        assert!(executed_at.duration_since(ready_at) <= MAX_DELAYED_TASK_LAG);

        remote.stop();
        handle_1.join().unwrap();
        handle_2.join().unwrap();
        let metrics_1 = metrics_1.lock().unwrap();
        let metrics_2 = metrics_2.lock().unwrap();
        assert_eq!(metrics_1.handle, 1);
        assert_eq!(metrics_1.end, 1);
        assert_eq!(metrics_2.handle, 0);
        assert_eq!(metrics_2.end, 1);
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_worker_wakes_when_task_inserted_before_park() {
        // The task is inserted after the worker decides to sleep, but before it
        // calls parking_lot_core::park. The validate callback should pop the
        // task as Ready and abort the park.
        check_worker_wakes_when_task_is_inserted_at("worker-pop-or-sleep-before-park");
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_worker_wakes_when_task_inserted_before_validate_pop() {
        // The task is inserted after mark_sleep succeeds inside validate, but
        // before validate pops from the queue. The pop should see the inserted
        // task immediately and avoid sleeping.
        check_worker_wakes_when_task_is_inserted_at("worker-pop-or-sleep-before-validate-pop");
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_worker_wakes_when_task_inserted_before_sleep() {
        // The task is inserted after validate has returned Empty and the worker
        // has been queued for parking, but before it actually sleeps. The push
        // should unpark the worker, and the next worker loop should run the
        // inserted task immediately.
        check_worker_wakes_when_task_is_inserted_at("worker-pop-or-sleep-before-sleep");
    }

    fn check_worker_runs_delayed_task_without_new_insert(
        scenario: DelayedQueueScenario,
    ) -> (Instant, Instant) {
        let queue = Arc::new(DeadlineQueue::new(vec![LATER_RETRY_OFFSET]));
        let (done_tx, done_rx) = mpsc::channel();
        queue.push(callback_task(move |_: &mut callback::Handle<'_>| {
            done_tx.send(Instant::now()).unwrap();
        }));
        match scenario {
            DelayedQueueScenario::PendingDuringSpinAndValidate => {
                let later_retry_at = Instant::now() + LATER_RETRY_OFFSET;
                for _ in 0..WORKER_SPIN_POP_COUNT {
                    queue.push_scripted_result(DeadlineScript::Pending(later_retry_at));
                }
            }
            DelayedQueueScenario::EmptyDuringSpin => {
                for _ in 0..WORKER_SPIN_POP_COUNT {
                    queue.push_scripted_result(DeadlineScript::Empty);
                }
            }
            DelayedQueueScenario::EarlierRetryInValidate => {
                let later_retry_at = Instant::now() + LATER_RETRY_OFFSET;
                for _ in 0..WORKER_SPIN_POP_COUNT {
                    queue.push_scripted_result(DeadlineScript::Pending(later_retry_at));
                }
            }
        }
        // The retry deadline is installed while the worker is in pop_or_sleep
        // validation. This keeps the test from depending on how quickly the CI
        // runner starts the worker thread after the task was pushed.
        queue.push_scripted_result(DeadlineScript::PendingAfter(DELAYED_TASK_DELAY));
        let (remote, _pause_rx, metrics, handle) = build_custom_worker(queue.clone());
        let executed_at = done_rx
            .recv_timeout(MAX_DELAYED_TASK_LAG + Duration::from_secs(1))
            .unwrap();
        let ready_at = queue.ready_at(0);

        assert!(executed_at >= ready_at);
        assert!(executed_at.duration_since(ready_at) <= MAX_DELAYED_TASK_LAG);
        {
            let metrics = metrics.lock().unwrap();
            assert_eq!(metrics.start, 1);
            assert_eq!(metrics.handle, 1);
            assert!(metrics.resume >= 1);
            assert!(metrics.pause >= 1);
        }

        remote.stop();
        handle.join().unwrap();
        assert_eq!(metrics.lock().unwrap().end, 1);

        (executed_at, ready_at)
    }

    #[test]
    fn test_worker_runs_delayed_task_after_pending_then_pending() {
        // The worker first observes Pending during spin, then observes Pending
        // again inside pop_or_sleep. With no later push, it should wake by the
        // retry timeout and run the delayed task.
        check_worker_runs_delayed_task_without_new_insert(
            DelayedQueueScenario::PendingDuringSpinAndValidate,
        );
    }

    #[test]
    fn test_worker_runs_delayed_task_after_empty_then_pending() {
        // The worker first observes Empty during spin, then Pending inside
        // pop_or_sleep. The validate Pending retry should still drive a timed
        // park and let the delayed task run once it becomes ready.
        check_worker_runs_delayed_task_without_new_insert(DelayedQueueScenario::EmptyDuringSpin);
    }

    #[test]
    fn test_worker_uses_shorter_retry_from_pending_validate() {
        // The worker observes a later Pending retry during spin, then a shorter
        // Pending retry in pop_or_sleep validate. It should use the shorter
        // retry instead of sleeping until the stale later deadline.
        let (executed_at, ready_at) = check_worker_runs_delayed_task_without_new_insert(
            DelayedQueueScenario::EarlierRetryInValidate,
        );
        assert!(executed_at < ready_at + LATER_RETRY_OFFSET);
    }
}
