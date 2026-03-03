// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::{Local, Runner};
use crate::queue::{AcquireState, Pop, TaskCell};
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
    /// Pops a task from the queue.
    /// Returns `Some((Pop<T>, AcquireState))` if a task is found, where `AcquireState` indicates
    /// how the task was acquired (immediate, after spin, or after park).
    #[inline]
    fn pop(&mut self, retry_after_park: bool) -> Option<(Pop<T>, AcquireState)> {
        // Wait some time before going to sleep, which is more expensive.
        let mut spin = SpinWait::new();
        let mut state = if retry_after_park {
            AcquireState::AfterPark
        } else {
            AcquireState::Immediate
        };
        loop {
            if let Some(t) = self.local.pop() {
                return Some((t, state));
            }
            if !spin.spin() {
                break;
            }
            if state == AcquireState::Immediate {
                state = AcquireState::AfterSpin;
            }
        }
        self.runner.pause(&mut self.local);
        let t = self.local.pop_or_sleep();
        self.runner.resume(&mut self.local);
        t.map(|task| (task, AcquireState::AfterPark))
    }

    pub fn run(mut self) {
        self.runner.start(&mut self.local);
        let mut retry_after_park = false;
        while !self.local.core().is_shutdown() {
            let (mut task, acquire_state) = match self.pop(retry_after_park) {
                Some(t) => {
                    retry_after_park = false;
                    t
                }
                None => {
                    retry_after_park = true;
                    continue;
                }
            };
            let extras = task.task_cell.mut_extras();
            extras.acquire_state = Some(acquire_state);
            extras.task_source = Some(task.task_source);
            self.runner.handle(&mut self.local, task.task_cell);
        }
        self.runner.end(&mut self.local);

        // Drain all futures in the queue
        while self.local.pop().is_some() {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::spawn::*;
    use crate::pool::SchedConfig;
    use crate::queue::{AcquireState, Extras, QueueType, TaskCell, TaskSource};
    use crate::task::callback;
    use std::sync::atomic::AtomicUsize;
    use std::sync::*;
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

    struct InspectTask {
        extras: Extras,
    }

    impl InspectTask {
        fn new() -> Self {
            InspectTask {
                extras: Extras::single_level(),
            }
        }
    }

    impl TaskCell for InspectTask {
        fn mut_extras(&mut self) -> &mut Extras {
            &mut self.extras
        }
    }

    enum Event {
        Paused,
        Handled(TaskSource, AcquireState),
    }

    struct InspectRunner {
        tx: mpsc::Sender<Event>,
    }

    impl crate::pool::Runner for InspectRunner {
        type TaskCell = InspectTask;

        fn handle(
            &mut self,
            _local: &mut Local<Self::TaskCell>,
            mut task_cell: Self::TaskCell,
        ) -> bool {
            let extras = task_cell.mut_extras();
            let task_source = extras.task_source().unwrap();
            let acquire_state = extras.acquire_state().unwrap();
            self.tx
                .send(Event::Handled(task_source, acquire_state))
                .unwrap();
            true
        }

        fn pause(&mut self, _local: &mut Local<Self::TaskCell>) -> bool {
            self.tx.send(Event::Paused).unwrap();
            true
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
    fn test_worker_run_task_from_local_immediate() {
        let mut config: SchedConfig = Default::default();
        config.max_thread_count = 1;
        config.core_thread_count = AtomicUsize::new(1);
        let (remote, mut locals) = build_spawn(QueueType::SingleLevel, config);
        let (tx, rx) = mpsc::channel();
        let runner = InspectRunner { tx };

        let mut local = locals.remove(0);
        local.spawn(InspectTask::new()); // spawn a local task before worker starts
        let th = WorkerThread::new(local, runner);
        let handle = std::thread::spawn(move || {
            th.run();
        });

        match rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            Event::Handled(task_source, acquire_state) => {
                assert_eq!(task_source, TaskSource::LocalQueue);
                assert_eq!(acquire_state, AcquireState::Immediate);
            }
            Event::Paused => panic!("did not expect pause before handling task"),
        }

        remote.stop();
        handle.join().unwrap();
    }

    #[test]
    fn test_worker_run_task_from_global_after_park() {
        let mut config: SchedConfig = Default::default();
        config.max_thread_count = 1;
        config.core_thread_count = AtomicUsize::new(1);
        let (remote, mut locals) = build_spawn(QueueType::SingleLevel, config);
        let (tx, rx) = mpsc::channel();
        let runner = InspectRunner { tx };

        let th = WorkerThread::new(locals.remove(0), runner);
        let handle = std::thread::spawn(move || {
            th.run();
        });

        match rx.recv_timeout(Duration::from_secs(1)).unwrap() {
            Event::Paused => {}
            Event::Handled(_, _) => panic!("expected pause before handling task"),
        }

        remote.spawn(InspectTask::new());

        let deadline = Instant::now() + Duration::from_secs(1);
        let (task_source, acquire_state) = loop {
            let timeout = deadline.saturating_duration_since(Instant::now());
            let event = rx.recv_timeout(timeout).unwrap();
            if let Event::Handled(task_source, acquire_state) = event {
                break (task_source, acquire_state);
            }
        };

        assert_eq!(task_source, TaskSource::GlobalQueue);
        assert_eq!(acquire_state, AcquireState::AfterPark);

        remote.stop();
        handle.join().unwrap();
    }
}
