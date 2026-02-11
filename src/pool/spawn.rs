// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements how task are pushed and polled. Threads are
//! woken up when new tasks arrived and go to sleep when there are no
//! tasks waiting to be handled.

use crate::pool::SchedConfig;
use crate::queue::{Extras, LocalQueue, Pop, TaskCell, TaskInjector, WithExtras};
use fail::fail_point;
use parking_lot_core::{FilterOp, ParkResult, ParkToken, UnparkToken};
use prometheus::local::LocalCounter;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Weak,
};
use std::time::Instant;

/// An usize is used to trace the threads that are working actively.
/// To save additional memory and atomic operation, the number and
/// shutdown hint are merged into one number in the following format
/// ```text
/// 0...00
/// ^    ^
/// |    The least significant bit indicates whether the queue is shutting down.
/// Bits represent the thread count
/// ```
const SHUTDOWN_BIT: usize = 1;
const WORKER_COUNT_SHIFT: usize = 1;
const WORKER_COUNT_BASE: usize = 2;
const ACTIVE_TIME_FLUSH_THRESHOLD_SECS: f64 = 1.0;
const ACTIVE_TIME_SAMPLE_EVERY_TASKS: u32 = 256;

/// Checks if shutdown bit is set.
pub fn is_shutdown(cnt: usize) -> bool {
    cnt & SHUTDOWN_BIT == SHUTDOWN_BIT
}

/// The core of queues.
///
/// Every thread pool instance should have one and only `QueueCore`. It's
/// saved in an `Arc` and shared between all worker threads and remote handles.
pub(crate) struct QueueCore<T> {
    global_queue: TaskInjector<T>,
    active_workers: AtomicUsize,
    config: SchedConfig,
}

impl<T> QueueCore<T> {
    pub fn new(global_queue: TaskInjector<T>, config: SchedConfig) -> QueueCore<T> {
        QueueCore {
            global_queue,
            active_workers: AtomicUsize::new(config.max_thread_count << WORKER_COUNT_SHIFT),
            config,
        }
    }

    /// Ensures there are enough workers to handle pending tasks.
    ///
    /// If the method is going to wake up any threads, source is used to trace who triggers
    /// the action.
    pub fn ensure_workers(&self, source: usize) {
        let cnt = self.active_workers.load(Ordering::SeqCst);
        if (cnt >> WORKER_COUNT_SHIFT) >= self.config.core_thread_count.load(Ordering::SeqCst)
            || is_shutdown(cnt)
        {
            return;
        }

        let addr = self as *const QueueCore<T> as usize;
        let mut unparked_once = false;

        unsafe {
            parking_lot_core::unpark_filter(
                addr,
                |p: ParkToken| {
                    if !unparked_once && p.0 <= self.config.core_thread_count.load(Ordering::SeqCst)
                    {
                        unparked_once = true;
                        FilterOp::Unpark
                    } else {
                        FilterOp::Skip
                    }
                },
                |_| UnparkToken(source),
            );
        }
    }

    /// Sets the shutdown bit and notify all threads.
    ///
    /// `source` is used to trace who triggers the action.
    pub fn mark_shutdown(&self, source: usize) {
        self.active_workers.fetch_or(SHUTDOWN_BIT, Ordering::SeqCst);
        let addr = self as *const QueueCore<T> as usize;
        unsafe {
            parking_lot_core::unpark_all(addr, UnparkToken(source));
        }
    }

    /// Checks if the thread pool is shutting down.
    pub fn is_shutdown(&self) -> bool {
        let cnt = self.active_workers.load(Ordering::SeqCst);
        is_shutdown(cnt)
    }

    /// Marks the current thread in sleep state.
    ///
    /// It can be marked as sleep only when the pool is not shutting down.
    pub fn mark_sleep(&self) -> bool {
        let mut cnt = self.active_workers.load(Ordering::SeqCst);
        loop {
            if is_shutdown(cnt) {
                return false;
            }

            match self.active_workers.compare_exchange_weak(
                cnt,
                cnt - WORKER_COUNT_BASE,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return true,
                Err(n) => cnt = n,
            }
        }
    }

    /// Marks current thread as woken up states.
    pub fn mark_woken(&self) {
        let mut cnt = self.active_workers.load(Ordering::SeqCst);
        loop {
            match self.active_workers.compare_exchange_weak(
                cnt,
                cnt + WORKER_COUNT_BASE,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(n) => cnt = n,
            }
        }
    }

    /// Scale workers.
    pub fn scale_workers(&self, mut new_thread_count: usize) {
        if new_thread_count == 0 || new_thread_count > self.config.max_thread_count {
            new_thread_count = self.config.max_thread_count;
        } else if new_thread_count < self.config.min_thread_count {
            new_thread_count = self.config.min_thread_count;
        }
        self.config
            .core_thread_count
            .store(new_thread_count, Ordering::SeqCst);
    }

    pub fn config(&self) -> &SchedConfig {
        &self.config
    }
}

impl<T: TaskCell + Send> QueueCore<T> {
    /// Pushes the task to global queue.
    ///
    /// `source` is used to trace who triggers the action.
    fn push(&self, source: usize, task: T) {
        self.global_queue.push(task);
        self.ensure_workers(source);
    }

    fn default_extras(&self) -> Extras {
        self.global_queue.default_extras()
    }
}

/// Submits tasks to associated thread pool.
///
/// Note that thread pool can be shutdown and dropped even not all remotes are
/// dropped.
pub struct Remote<T> {
    pub(crate) core: Arc<QueueCore<T>>,
}

impl<T: TaskCell + Send> Remote<T> {
    pub(crate) fn new(core: Arc<QueueCore<T>>) -> Remote<T> {
        Remote { core }
    }

    /// Submits a task to the thread pool.
    pub fn spawn(&self, task: impl WithExtras<T>) {
        let t = task.with_extras(|| self.core.default_extras());
        self.core.push(0, t);
    }

    /// Scales workers of the thread pool.
    pub fn scale_workers(&self, new_thread_count: usize) {
        self.core.scale_workers(new_thread_count)
    }

    pub(crate) fn stop(&self) {
        self.core.mark_shutdown(0);
    }
}

impl<T> Clone for Remote<T> {
    fn clone(&self) -> Remote<T> {
        Remote {
            core: self.core.clone(),
        }
    }
}

/// Note that implements of Runner assumes `Remote` is `Sync` and `Send`.
/// So we need to use assert trait to ensure the constraint at compile time
/// to avoid future breaks.
#[allow(dead_code)]
trait AssertSync: Sync {}
impl<T: Send> AssertSync for Remote<T> {}
#[allow(dead_code)]
trait AssertSend: Send {}
impl<T: Send> AssertSend for Remote<T> {}

/// `WeakRemote` is a weak reference to the inner queue.
pub(crate) struct WeakRemote<T> {
    core: Weak<QueueCore<T>>,
}

impl<T: TaskCell + Send> WeakRemote<T> {
    /// Upgrade a `WeakRemote` to `Remote`.
    pub fn upgrade(&self) -> Option<Remote<T>> {
        self.core.upgrade().map(|core| Remote { core })
    }

    /// Returns the ptr of the inner queue core.
    pub fn as_core_ptr(&self) -> *const QueueCore<T> {
        self.core.as_ptr()
    }
}

impl<T> Clone for WeakRemote<T> {
    fn clone(&self) -> WeakRemote<T> {
        WeakRemote {
            core: self.core.clone(),
        }
    }
}

impl<T: Send> AssertSync for WeakRemote<T> {}
impl<T: Send> AssertSend for WeakRemote<T> {}
struct WorkerActivity {
    counter: LocalCounter,
    active_since: Option<Instant>,
    ticks: u32,
}

impl WorkerActivity {
    fn new(counter: LocalCounter) -> WorkerActivity {
        WorkerActivity {
            counter,
            active_since: None,
            ticks: 0,
        }
    }

    #[inline]
    fn on_start(&mut self) {
        self.active_since = Some(Instant::now());
    }

    #[inline]
    fn on_task_complete(&mut self) {
        self.ticks = self.ticks.wrapping_add(1);
        if self.ticks == ACTIVE_TIME_SAMPLE_EVERY_TASKS {
            self.ticks = 0;
            self.checkpoint();
        }
    }

    #[inline]
    fn on_park(&mut self) {
        let now = Instant::now();
        self.add_elapsed(now);
        self.active_since = None;
        self.counter.flush();
    }

    #[inline]
    fn on_unpark(&mut self) {
        if self.active_since.is_none() {
            self.active_since = Some(Instant::now());
        }
    }

    #[inline]
    fn on_end(&mut self) {
        let now = Instant::now();
        self.add_elapsed(now);
        self.active_since = None;
        self.counter.flush();
    }

    #[inline]
    fn checkpoint(&mut self) {
        if self.active_since.is_none() {
            return;
        }
        let now = Instant::now();
        self.add_elapsed(now);
        self.active_since = Some(now);
        if self.counter.get() >= ACTIVE_TIME_FLUSH_THRESHOLD_SECS {
            self.counter.flush();
        }
    }

    #[inline]
    fn add_elapsed(&mut self, now: Instant) {
        if let Some(since) = self.active_since {
            let elapsed = now.saturating_duration_since(since);
            if !elapsed.is_zero() {
                self.counter.inc_by(elapsed.as_secs_f64());
            }
        }
    }
}

/// Spawns tasks to the associated thread pool.
///
/// It's different from `Remote` because it submits tasks to the local queue
/// instead of global queue, so new tasks can take advantage of cache
/// coherence.
pub struct Local<T> {
    id: usize,
    local_queue: LocalQueue<T>,
    core: Arc<QueueCore<T>>,
    activity: Option<WorkerActivity>,
}

impl<T: TaskCell + Send> Local<T> {
    pub(crate) fn new(id: usize, local_queue: LocalQueue<T>, core: Arc<QueueCore<T>>) -> Local<T> {
        Local {
            id,
            local_queue,
            core,
            activity: None,
        }
    }

    /// Spawns a task to the local queue.
    pub fn spawn(&mut self, task: impl WithExtras<T>) {
        let t = task.with_extras(|| self.local_queue.default_extras());
        self.local_queue.push(t);
    }

    /// Spawns a task to the remote queue.
    pub fn spawn_remote(&self, task: impl WithExtras<T>) {
        let t = task.with_extras(|| self.local_queue.default_extras());
        self.core.push(self.id, t);
    }

    /// Gets a remote so that tasks can be spawned from other threads.
    pub fn remote(&self) -> Remote<T> {
        Remote::new(self.core.clone())
    }

    pub(crate) fn weak_remote(&self) -> WeakRemote<T> {
        WeakRemote {
            core: Arc::downgrade(&self.core),
        }
    }

    pub(crate) fn core(&self) -> &Arc<QueueCore<T>> {
        &self.core
    }

    pub(crate) fn pop(&mut self) -> Option<Pop<T>> {
        self.local_queue.pop()
    }

    /// Pops a task from the queue.
    ///
    /// If there are no tasks at the moment, it will go to sleep until woken
    /// up by other threads.
    pub(crate) fn pop_or_sleep(&mut self) -> Option<Pop<T>> {
        let Local {
            id,
            local_queue,
            core,
            activity,
        } = self;
        let address = &**core as *const QueueCore<T> as usize;
        let mut task = None;
        let id = *id;

        let res = unsafe {
            parking_lot_core::park(
                address,
                || {
                    if !core.mark_sleep() {
                        return false;
                    }
                    task = local_queue.pop();
                    task.is_none()
                },
                || {
                    if let Some(activity) = activity.as_mut() {
                        activity.on_park();
                    }
                },
                |_, _| {},
                ParkToken(id),
                None,
            )
        };
        match res {
            ParkResult::Unparked(_) | ParkResult::Invalid => {
                core.mark_woken();
                if let Some(activity) = activity.as_mut() {
                    activity.on_unpark();
                }
                task
            }
            ParkResult::TimedOut => unreachable!(),
        }
    }

    /// Returns whether there are preemptive tasks to run.
    ///
    /// If the pool is not busy, other tasks should not preempt the current running task.
    pub(crate) fn need_preempt(&mut self) -> bool {
        fail_point!("need-preempt", |r| { r.unwrap().parse().unwrap() });
        self.local_queue.has_tasks_or_pull()
    }

    pub(crate) fn enable_worker_activity(&mut self, counter: LocalCounter) {
        self.activity = Some(WorkerActivity::new(counter));
    }

    pub(crate) fn on_worker_start(&mut self) {
        if let Some(activity) = self.activity.as_mut() {
            activity.on_start();
        }
    }

    pub(crate) fn on_worker_end(&mut self) {
        if let Some(activity) = self.activity.as_mut() {
            activity.on_end();
        }
    }

    pub(crate) fn on_task_complete(&mut self) {
        if let Some(activity) = self.activity.as_mut() {
            activity.on_task_complete();
        }
    }
}

/// Building remotes and locals from the given queue and configuration.
///
/// This is only for tests purpose so that a thread pool doesn't have to be
/// spawned to test a Runner.
pub fn build_spawn<T>(
    queue_type: impl Into<crate::queue::QueueType>,
    config: SchedConfig,
) -> (Remote<T>, Vec<Local<T>>)
where
    T: TaskCell + Send,
{
    let queue_type = queue_type.into();
    let (global, locals) = crate::queue::build(queue_type, config.max_thread_count);
    let core = Arc::new(QueueCore::new(global, config));
    let l = locals
        .into_iter()
        .enumerate()
        .map(|(i, l)| Local::new(i + 1, l, core.clone()))
        .collect();
    let g = Remote::new(core);
    (g, l)
}
