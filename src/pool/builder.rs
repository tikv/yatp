// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::spawn::QueueCore;
use crate::pool::worker::WorkerThread;
use crate::pool::{CloneRunnerBuilder, Handle, Local, Runner, RunnerBuilder, ThreadPool};
use crate::queue::{self, LocalQueueBuilder, QueueType, TaskCell};
use crate::task::{callback, future};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Configuration for schedule algorithm.
#[derive(Clone)]
pub struct SchedConfig {
    /// The maximum number of running threads at the same time.
    pub max_thread_count: usize,
    /// The minimum number of running threads at the same time.
    pub min_thread_count: usize,
    /// The maximum tries to rerun an unfinished task before pushing
    /// back to queue.
    pub max_inplace_spin: usize,
    /// The maximum allowed idle time for a thread. Thread will only be
    /// woken up when algorithm thinks it needs more worker.
    pub max_idle_time: Duration,
    /// The maximum time to wait for a task before increasing the
    /// running thread slots.
    pub max_wait_time: Duration,
    /// The minimum interval between waking a thread.
    pub wake_backoff: Duration,
    /// The minimum interval between increasing running threads.
    pub alloc_slot_backoff: Duration,
}

impl Default for SchedConfig {
    fn default() -> SchedConfig {
        SchedConfig {
            max_thread_count: num_cpus::get(),
            min_thread_count: 1,
            max_inplace_spin: 4,
            max_idle_time: Duration::from_millis(1),
            max_wait_time: Duration::from_millis(1),
            wake_backoff: Duration::from_millis(1),
            alloc_slot_backoff: Duration::from_millis(2),
        }
    }
}

/// A builder for lazy spawning.
pub struct LazyBuilder<T> {
    builder: Builder,
    core: Arc<QueueCore<T>>,
    local_queue_builders: Vec<LocalQueueBuilder<T>>,
}

impl<T: Send + 'static> LazyBuilder<T> {
    /// Sets the name prefix of threads. The thread name will follow the
    /// format "prefix-index".
    pub fn name(mut self, name_prefix: impl Into<String>) -> LazyBuilder<T> {
        self.builder.name_prefix = name_prefix.into();
        self
    }
}

impl<T: Send + 'static> LazyBuilder<T>
where
    T: TaskCell + Send + 'static,
{
    /// Spawns all the required threads.
    ///
    /// There will be `max_thread_count` threads spawned. Generally only a few
    /// will keep running in the background, most of them are put to sleep
    /// immediately.
    pub fn build<F>(self, mut factory: F) -> ThreadPool<T>
    where
        F: RunnerBuilder,
        F::Runner: Runner<TaskCell = T> + Send + 'static,
    {
        let mut threads = Vec::with_capacity(self.builder.sched_config.max_thread_count);
        for (i, queue_builder) in self.local_queue_builders.into_iter().enumerate() {
            let runner = factory.build();
            let name = format!("{}-{}", self.builder.name_prefix, i);
            let mut builder = thread::Builder::new().name(name);
            if let Some(size) = self.builder.stack_size {
                builder = builder.stack_size(size)
            }
            let core = self.core.clone();
            threads.push(
                builder
                    .spawn(move || {
                        let local = Local::new(i + 1, queue_builder(), core);
                        let thd = WorkerThread::new(local, runner);
                        thd.run();
                    })
                    .unwrap(),
            );
        }
        ThreadPool {
            handle: Handle::new(self.core.clone()),
            threads: Mutex::new(threads),
        }
    }
}

/// A builder for the thread pool.
#[derive(Clone)]
pub struct Builder {
    name_prefix: String,
    stack_size: Option<usize>,
    sched_config: SchedConfig,
}

impl Builder {
    /// Create a builder using the given name prefix.
    pub fn new(name_prefix: impl Into<String>) -> Builder {
        Builder {
            name_prefix: name_prefix.into(),
            stack_size: None,
            sched_config: SchedConfig::default(),
        }
    }

    /// Sets the maximum number of running threads at the same time.
    pub fn max_thread_count(&mut self, count: usize) -> &mut Self {
        if count > 0 {
            self.sched_config.max_thread_count = count;
        }
        self
    }

    /// Sets the minimum number of running threads at the same time.
    pub fn min_thread_count(&mut self, count: usize) -> &mut Self {
        if count > 0 {
            self.sched_config.min_thread_count = count;
        }
        self
    }

    /// Sets the maximum tries to rerun an unfinished task before pushing
    /// back to queue.
    pub fn max_inplace_spin(&mut self, count: usize) -> &mut Self {
        self.sched_config.max_inplace_spin = count;
        self
    }

    /// Sets the maximum allowed idle time for a thread. Thread will only be
    /// woken up when algorithm thinks it needs more worker.
    pub fn max_idle_time(&mut self, time: Duration) -> &mut Self {
        self.sched_config.max_idle_time = time;
        self
    }

    /// Sets the maximum time to wait for a task before increasing the
    /// running thread slots.
    pub fn max_wait_time(&mut self, time: Duration) -> &mut Self {
        self.sched_config.max_wait_time = time;
        self
    }

    /// Sets the minimum interval between waking a thread.
    pub fn wake_backoff(&mut self, time: Duration) -> &mut Self {
        self.sched_config.wake_backoff = time;
        self
    }

    /// Sets the minimum interval between increasing running threads.
    pub fn alloc_slot_backoff(&mut self, time: Duration) -> &mut Self {
        self.sched_config.alloc_slot_backoff = time;
        self
    }

    /// Sets the stack size of the spawned threads.
    pub fn stack_size(&mut self, size: usize) -> &mut Self {
        if size > 0 {
            self.stack_size = Some(size);
        }
        self
    }

    /// Freezes the configurations and returns the task scheduler and
    /// a builder to for lazy spawning threads.
    ///
    /// It internally uses single level queue to setup the pool.
    ///
    /// In some cases, especially building up a large application, a task
    /// scheduler is required before spawning new threads. You can use this
    /// to separate the construction and starting.
    pub fn freeze<T>(&self) -> (Handle<T>, LazyBuilder<T>)
    where
        T: TaskCell + Send + 'static,
    {
        self.freeze_with_queue(QueueType::SingleLevel)
    }

    /// Freezes the configurations and returns the task scheduler and
    /// a builder to for lazy spawning threads.
    ///
    /// `queue_builder` is a closure that creates a task queue. It accepts the
    /// number of local queues and returns the task injector and local queues.
    ///
    /// In some cases, especially building up a large application, a task
    /// scheduler is required before spawning new threads. You can use this
    /// to separate the construction and starting.
    pub fn freeze_with_queue<T>(&self, queue_type: QueueType) -> (Handle<T>, LazyBuilder<T>)
    where
        T: TaskCell + Send + 'static,
    {
        assert!(self.sched_config.min_thread_count <= self.sched_config.max_thread_count);
        let (injector, local_queue_builders) =
            queue::build(queue_type, self.sched_config.max_thread_count);
        let core = Arc::new(QueueCore::new(injector, self.sched_config.clone()));

        (
            Handle::new(core.clone()),
            LazyBuilder {
                builder: self.clone(),
                core,
                local_queue_builders,
            },
        )
    }

    /// Spawns a callback pool.
    ///
    /// It setups the pool with single level queue.
    pub fn build_callback_pool(&self) -> ThreadPool<callback::TaskCell> {
        let rb = CloneRunnerBuilder(callback::Runner::default());
        self.build_with_queue_and_runner(QueueType::SingleLevel, rb)
    }

    /// Spawns a future pool.
    ///
    /// It setups the pool with single level queue.
    pub fn build_future_pool(&self) -> ThreadPool<future::TaskCell> {
        let fb = CloneRunnerBuilder(future::Runner::default());
        self.build_with_queue_and_runner(QueueType::SingleLevel, fb)
    }

    /// Spawns the thread pool immediately.
    ///
    /// `queue_builder` is a closure that creates a task queue. It accepts the
    /// number of local queues and returns the task injector and local queues.
    pub fn build_with_queue_and_runner<T, B>(
        &self,
        queue_type: QueueType,
        runner_builder: B,
    ) -> ThreadPool<T>
    where
        T: TaskCell + Send + 'static,
        B: RunnerBuilder,
        B::Runner: Runner<TaskCell = T> + Send + 'static,
    {
        self.freeze_with_queue(queue_type).1.build(runner_builder)
    }
}
