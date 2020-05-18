// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This module implements how task are pushed and polled. Threads are
//! woken up when new tasks arrived and go to sleep when there are no
//! tasks waiting to be handled.

use crate::pool::SchedConfig;
use crate::queue::{Extras, LocalQueue, Pop, TaskCell, TaskInjector, WithExtras};
use fail::fail_point;
use parking_lot_core::{ParkResult, ParkToken, UnparkToken};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;

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

/// Checks if shutdown bit is set.
pub fn is_shutdown(cnt: usize) -> bool {
    cnt & SHUTDOWN_BIT == SHUTDOWN_BIT
}

// 1 bit: shutdown
// 16 bits: running
// 16 bits: active
// 16 bits: backup
trait WorkersInfo {
    fn is_shutdown(self) -> bool;
    fn running_count(self) -> usize;
    fn active_count(self) -> usize;
    fn backup_count(self) -> usize;
}

impl WorkersInfo for u64 {
    fn is_shutdown(self) -> bool {
        self & 1 == 1
    }

    fn running_count(self) -> usize {
        ((self >> 1) & (u16::max_value() as u64)) as usize
    }

    fn active_count(self) -> usize {
        ((self >> 17) & (u16::max_value() as u64)) as usize
    }

    fn backup_count(self) -> usize {
        ((self >> 33) & (u16::max_value() as u64)) as usize
    }
}

/// The core of queues.
///
/// Every thread pool instance should have one and only `QueueCore`. It's
/// saved in an `Arc` and shared between all worker threads and remote handles.
pub(crate) struct QueueCore<T> {
    global_queue: TaskInjector<T>,
    workers_info: AtomicU64,
    backup_countdown: AtomicU8,
    idling: AtomicBool,
    config: SchedConfig,
}

impl<T> QueueCore<T> {
    pub fn new(global_queue: TaskInjector<T>, config: SchedConfig) -> QueueCore<T> {
        let thread_count = config.max_thread_count as u64;
        QueueCore {
            global_queue,
            workers_info: AtomicU64::new(((thread_count << 17) | (thread_count << 1)) as u64),
            backup_countdown: AtomicU8::new(8),
            idling: AtomicBool::new(false),
            config,
        }
    }

    /// Ensures there are enough workers to handle pending tasks.
    ///
    /// If the method is going to wake up any threads, source is used to trace who triggers
    /// the action.
    pub fn ensure_workers(&self, source: usize) {
        let workers_info = self.workers_info.load(Ordering::SeqCst);
        if workers_info.is_shutdown() {
            return;
        }
        let idling = self.idling.load(Ordering::SeqCst);
        if workers_info.running_count() == workers_info.active_count()
            && workers_info.backup_count() > 0
            && !idling
        {
            // println!(
            //     "unpark backup, running: {}, active: {}, backup: {}",
            //     workers_info.running_count(),
            //     workers_info.active_count(),
            //     workers_info.backup_count()
            // );
            // println!("unpark backup");
            self.unpark_one(true, source);
        } else if !idling {
            // println!(
            //     "unpark active, running: {}, active: {}, backup: {}",
            //     workers_info.running_count(),
            //     workers_info.active_count(),
            //     workers_info.backup_count()
            // );
            self.unpark_one(false, source);
        }
    }

    fn unpark_one(&self, backup: bool, source: usize) {
        unsafe {
            parking_lot_core::unpark_one(self.park_address(backup), |_| UnparkToken(source));
        }
    }

    pub fn park_to_backup(&self) -> bool {
        self.backup_countdown
            .compare_and_swap(0, 8, Ordering::SeqCst)
            == 0
    }

    pub fn park_address(&self, backup: bool) -> usize {
        if backup {
            self as *const QueueCore<T> as usize + 1
        } else {
            self as *const QueueCore<T> as usize
        }
    }

    /// Sets the shutdown bit and notify all threads.
    ///
    /// `source` is used to trace who triggers the action.
    pub fn mark_shutdown(&self, source: usize) {
        self.workers_info.fetch_or(1, Ordering::SeqCst);
        unsafe {
            parking_lot_core::unpark_all(self.park_address(false), UnparkToken(source));
            parking_lot_core::unpark_all(self.park_address(true), UnparkToken(source));
        }
    }

    /// Checks if the thread pool is shutting down.
    pub fn is_shutdown(&self) -> bool {
        self.workers_info.load(Ordering::SeqCst).is_shutdown()
    }

    pub fn mark_idling(&self) -> bool {
        !self.idling.compare_and_swap(false, true, Ordering::SeqCst)
    }

    pub fn unmark_idling(&self) {
        self.idling.store(false, Ordering::SeqCst);
    }

    /// Marks the current thread in sleep state.
    ///
    /// It can be marked as sleep only when the pool is not shutting down.
    pub fn mark_sleep(&self, backup: bool) -> bool {
        let mut workers_info = self.workers_info.load(Ordering::SeqCst);
        loop {
            if workers_info.is_shutdown() {
                return false;
            }

            let new_info = if backup {
                ((workers_info.backup_count() as u64 + 1) << 33)
                    | ((workers_info.active_count() as u64 - 1) << 17)
                    | ((workers_info.running_count() as u64 - 1) << 1)
            } else {
                ((workers_info.backup_count() as u64) << 33)
                    | ((workers_info.active_count() as u64) << 17)
                    | ((workers_info.running_count() as u64 - 1) << 1)
            };
            match self.workers_info.compare_exchange_weak(
                workers_info,
                new_info,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if workers_info.running_count() + 1 < workers_info.active_count() {
                        let mut countdown = self.backup_countdown.load(Ordering::Relaxed);
                        while let Err(actual) = self.backup_countdown.compare_exchange_weak(
                            countdown,
                            countdown.saturating_sub(1),
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            countdown = actual;
                        }
                    }
                    return true;
                }
                Err(actual) => workers_info = actual,
            }
        }
    }

    /// Marks current thread as woken up states.
    pub fn mark_woken(&self, backup: bool) {
        let mut workers_info = self.workers_info.load(Ordering::SeqCst);
        loop {
            let new_info = if backup {
                ((workers_info.backup_count() as u64 - 1) << 33)
                    | ((workers_info.active_count() as u64 + 1) << 17)
                    | ((workers_info.running_count() as u64 + 1) << 1)
                    | workers_info.is_shutdown() as u64
            } else {
                ((workers_info.backup_count() as u64) << 33)
                    | ((workers_info.active_count() as u64) << 17)
                    | ((workers_info.running_count() as u64 + 1) << 1)
                    | workers_info.is_shutdown() as u64
            };
            match self.workers_info.compare_exchange_weak(
                workers_info,
                new_info,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => return,
                Err(actual) => workers_info = actual,
            }
        }
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
    core: Arc<QueueCore<T>>,
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
trait AssertSync: Sync {}
impl<T: Send> AssertSync for Remote<T> {}
trait AssertSend: Send {}
impl<T: Send> AssertSend for Remote<T> {}

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
        Remote {
            core: self.core.clone(),
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
        let backup = self.core.park_to_backup();
        let mut task = None;
        let id = self.id;

        let res = unsafe {
            parking_lot_core::park(
                self.core.park_address(backup),
                || {
                    if !self.core.mark_sleep(backup) {
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
                self.core.mark_woken(backup);
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
    let core = Arc::new(QueueCore::new(global, config));
    let l = locals
        .into_iter()
        .enumerate()
        .map(|(i, l)| Local::new(i + 1, l, core.clone()))
        .collect();
    let g = Remote::new(core);
    (g, l)
}
