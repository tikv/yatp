// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements how task are pushed and polled. Threads are
//! woken up when new tasks arrived and go to sleep when there are no
//! tasks waiting to be handled.

use super::builder::BurstMonitorConfig;
use crate::metrics::{MaxGauge, QUEUE_CORE_BURST_THROUGHPUT};
use crate::pool::SchedConfig;
use crate::queue::{Extras, LocalQueue, Pop, TaskCell, TaskInjector, WithExtras};
use fail::fail_point;
use parking_lot_core::{FilterOp, ParkResult, ParkToken, UnparkToken};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, OnceLock, Weak,
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

fn now_ns() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}

pub(crate) struct BurstMonitor {
    per_worker_multiplier: usize,
    min_sample_size: usize,
    count: AtomicUsize,
    sample_target: AtomicUsize,
    start_ns: AtomicU64,
    metric: MaxGauge,
}

impl BurstMonitor {
    pub(crate) fn new(name: &str, config: BurstMonitorConfig) -> BurstMonitor {
        let metric = QUEUE_CORE_BURST_THROUGHPUT
            .get_metric_with_label_values(&[name])
            .unwrap();
        BurstMonitor {
            per_worker_multiplier: config.per_worker_multiplier,
            min_sample_size: config.min_sample_size,
            count: AtomicUsize::new(0),
            sample_target: AtomicUsize::new(0),
            start_ns: AtomicU64::new(0),
            metric,
        }
    }

    fn on_enqueue(&self, active_workers: &AtomicUsize) {
        let new_count = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        if new_count == 1 {
            let workers = active_workers.load(Ordering::Relaxed) >> WORKER_COUNT_SHIFT;
            let target = self
                .per_worker_multiplier
                .saturating_mul(workers)
                .max(self.min_sample_size);
            self.sample_target.store(target, Ordering::Relaxed);
            self.start_ns.store(now_ns(), Ordering::Relaxed);
        }

        let target = self.sample_target.load(Ordering::Relaxed);
        if target == 0 || new_count < target {
            return;
        }

        let start_ns = self.start_ns.load(Ordering::Relaxed);
        if let Ok(count) = self
            .count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |c| {
                if c >= target {
                    Some(0)
                } else {
                    None
                }
            })
        {
            // `count` can exceed `target` under contention; it reflects actual enqueues
            // observed before the reset, so the measured window size is variable.
            let elapsed_ns = now_ns().saturating_sub(start_ns);
            if elapsed_ns > 0 {
                let throughput = (count as f64) / (elapsed_ns as f64 / 1_000_000_000.0);
                self.metric.observe(throughput);
            }
        }
    }
}

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
    burst_monitor: Option<BurstMonitor>,
}

impl<T> QueueCore<T> {
    pub fn new(
        global_queue: TaskInjector<T>,
        config: SchedConfig,
        burst_monitor: Option<BurstMonitor>,
    ) -> QueueCore<T> {
        QueueCore {
            global_queue,
            active_workers: AtomicUsize::new(config.max_thread_count << WORKER_COUNT_SHIFT),
            config,
            burst_monitor,
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
        if let Some(monitor) = self.burst_monitor.as_ref() {
            monitor.on_enqueue(&self.active_workers);
        }
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

/// Spawns tasks to the associated thread pool.
///
/// It's different from `Remote` because it submits tasks to the local queue
/// instead of global queue, so new tasks can take advantage of cache
/// coherence.
pub struct Local<T> {
    id: usize,
    local_queue: LocalQueue<T>,
    core: Arc<QueueCore<T>>,
}

impl<T: TaskCell + Send> Local<T> {
    pub(crate) fn new(id: usize, local_queue: LocalQueue<T>, core: Arc<QueueCore<T>>) -> Local<T> {
        Local {
            id,
            local_queue,
            core,
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
        let address = &*self.core as *const QueueCore<T> as usize;
        let mut task = None;
        let id = self.id;

        let res = unsafe {
            parking_lot_core::park(
                address,
                || {
                    if !self.core.mark_sleep() {
                        return false;
                    }
                    task = self.local_queue.pop();
                    task.is_none()
                },
                || {},
                |_, _| {},
                ParkToken(id),
                None,
            )
        };
        match res {
            ParkResult::Unparked(_) | ParkResult::Invalid => {
                self.core.mark_woken();
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
    let core = Arc::new(QueueCore::new(global, config, None));
    let l = locals
        .into_iter()
        .enumerate()
        .map(|(i, l)| Local::new(i + 1, l, core.clone()))
        .collect();
    let g = Remote::new(core);
    (g, l)
}
