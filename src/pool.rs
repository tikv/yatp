// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::queue::{LocalQueue, TaskCell, TaskInjector};
use crate::runner::{Runner, RunnerBuilder};
use std::mem;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Configuration for schedule algorithm.
#[derive(Clone)]
pub(crate) struct SchedConfig {
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

/// A builder for lazy spawning.
pub struct LazyBuilder<T> {
    builder: Builder,
    injector: Arc<TaskInjector<T>>,
    local_queues: Vec<LocalQueue<T>>,
}

impl<T> LazyBuilder<T> {
    /// Sets the name prefix of threads. The thread name will follow the
    /// format "prefix-index".
    pub fn name(mut self, name_prefix: impl Into<String>) -> LazyBuilder<T> {
        self.builder.name_prefix = name_prefix.into();
        self
    }
}

impl<T: TaskCell + Send + 'static> LazyBuilder<T> {
    /// Spawns all the required threads.
    ///
    /// There will be `max_thread_count` threads spawned. Generally only a few
    /// will keep running in the background, most of them are put to sleep
    /// immediately.
    pub fn build<F>(self, mut factory: F) -> ThreadPool<T>
    where
        F: RunnerBuilder,
        F::Runner: Runner + Send + 'static,
    {
        let mut threads = Vec::with_capacity(self.builder.sched_config.max_thread_count);
        for (i, local_queue) in self.local_queues.into_iter().enumerate() {
            let _r = factory.build();
            let name = format!("{}-{}", self.builder.name_prefix, i);
            let mut builder = thread::Builder::new().name(name);
            if let Some(size) = self.builder.stack_size {
                builder = builder.stack_size(size)
            }
            threads.push(
                builder
                    .spawn(move || {
                        drop(local_queue);
                        unimplemented!()
                    })
                    .unwrap(),
            );
        }
        ThreadPool {
            remote: Remote {
                injector: self.injector,
            },
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
            sched_config: SchedConfig {
                max_thread_count: num_cpus::get(),
                min_thread_count: 1,
                max_inplace_spin: 4,
                max_idle_time: Duration::from_millis(1),
                max_wait_time: Duration::from_millis(1),
                wake_backoff: Duration::from_millis(1),
                alloc_slot_backoff: Duration::from_millis(2),
            },
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
    /// `queue_builder` is a closure that creates a task queue. It accepts the
    /// number of local queues and returns the task injector and local queues.
    ///
    /// In some cases, especially building up a large application, a task
    /// scheduler is required before spawning new threads. You can use this
    /// to separate the construction and starting.
    pub fn freeze<T>(
        &self,
        queue_builder: impl FnOnce(usize) -> (TaskInjector<T>, Vec<LocalQueue<T>>),
    ) -> (Remote<T>, LazyBuilder<T>) {
        assert!(self.sched_config.min_thread_count <= self.sched_config.max_thread_count);
        let (injector, local_queues) = queue_builder(self.sched_config.max_thread_count);
        let injector = Arc::new(injector);
        (
            Remote {
                injector: injector.clone(),
            },
            LazyBuilder {
                builder: self.clone(),
                injector,
                local_queues,
            },
        )
    }

    /// Spawns the thread pool immediately.
    ///
    /// `queue_builder` is a closure that creates a task queue. It accepts the
    /// number of local queues and returns the task injector and local queues.
    pub fn build<T, B>(
        &self,
        queue_builder: impl FnOnce(usize) -> (TaskInjector<T>, Vec<LocalQueue<T>>),
        runner_builder: B,
    ) -> ThreadPool<T>
    where
        T: TaskCell + Send + 'static,
        B: RunnerBuilder,
        B::Runner: Runner + Send + 'static,
    {
        self.freeze(queue_builder).1.build(runner_builder)
    }
}

/// A generic thread pool.
pub struct ThreadPool<T: TaskCell + Send> {
    remote: Remote<T>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl<T: TaskCell + Send> ThreadPool<T> {
    /// Spawns the task into the thread pool.
    ///
    /// If the pool is shutdown, it becomes no-op.
    pub fn spawn(&self, t: impl Into<T>) {
        self.remote.spawn(t);
    }

    /// Shutdowns the pool.
    ///
    /// Closes the queue and wait for all threads to exit.
    pub fn shutdown(&self) {
        let mut threads = mem::replace(&mut *self.threads.lock().unwrap(), Vec::new());
        for j in threads.drain(..) {
            j.join().unwrap();
        }
    }

    /// Get a remote queue for spawning tasks without owning the thread pool.
    pub fn remote(&self) -> Remote<T> {
        self.remote.clone()
    }
}

impl<T: TaskCell + Send> Drop for ThreadPool<T> {
    /// Will shutdown the thread pool if it has not.
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A remote handle that can spawn tasks to the thread pool without owning
/// it.
pub struct Remote<T> {
    injector: Arc<TaskInjector<T>>,
}

impl<T: TaskCell> Remote<T> {
    /// Spawns the tasks into thread pool.
    ///
    /// If the thread pool is shutdown, it becomes no-op.
    pub fn spawn(&self, _t: impl Into<T>) {
        unimplemented!()
    }
}

impl<T> Clone for Remote<T> {
    fn clone(&self) -> Remote<T> {
        Remote {
            injector: self.injector.clone(),
        }
    }
}
