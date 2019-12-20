// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::queue::TaskCell;
use crate::queue::{TaskInjector, LocalQueue, Pop};
use crate::pool::SchedConfig;
use std::sync::Arc;
use std::time::Instant;
use std::sync::atomic::{AtomicUsize, Ordering};
use parking_lot_core::{UnparkToken, ParkToken, ParkResult};

const SHUTDOWN_BIT: usize = 1;
const WORKER_COUNT_SHIFT: usize = 1;
const WORKER_COUNT_BASE: usize = 2;

pub fn is_shutdown(cnt: usize) -> bool {
    cnt & SHUTDOWN_BIT == SHUTDOWN_BIT
}

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

    pub fn ensure_workers(&self, source: usize) {
        let cnt = self.active_workers.load(Ordering::Relaxed);
        if (cnt >> WORKER_COUNT_SHIFT) >= self.config.max_thread_count
            || is_shutdown(cnt)
        {
            return;
        }

        let addr = self as *const QueueCore<T> as usize;
        unsafe {
            parking_lot_core::unpark_one(addr, |_| UnparkToken(source));
        }
    }

    pub fn stop(&self, source: usize) {
        self.active_workers.fetch_or(SHUTDOWN_BIT, Ordering::SeqCst);
        let addr = self as *const QueueCore<T> as usize;
        unsafe {
            parking_lot_core::unpark_all(addr, UnparkToken(source));
        }
    }

    pub fn is_shutdown(&self) -> bool {
        let cnt = self.active_workers.load(Ordering::SeqCst);
        is_shutdown(cnt)
    }

    pub fn sleep(&self) -> bool {
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

    pub fn wake(&self) {
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
}

impl<T: TaskCell + Send> QueueCore<T> {
    fn push(&self, source: usize, task: T) {
        self.global_queue.push(task);
        self.ensure_workers(source);
    }
}

pub struct Remote<T> {
    core: Arc<QueueCore<T>>,
}

impl<T: TaskCell + Send> Remote<T> {
    pub(crate) fn new(core: Arc<QueueCore<T>>) -> Remote<T> {
        Remote { core }
    }

    pub fn spawn(&self, task: impl Into<T>) {
        self.core.push(0, task.into());
    }

    pub(crate) fn stop(&self) {
        self.core.stop(0);
    }
}

impl<T> Clone for Remote<T> {
    fn clone(&self) -> Remote<T> {
        Remote {
            core: self.core.clone(),
        }
    }
}

trait AssertSync: Sync {}
impl<T: Send> AssertSync for Remote<T> {}

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

    pub fn spawn(&mut self, task: T) {
        self.local_queue.push(task);
    }

    pub fn spawn_remote(&self, task: T) {
        self.core.push(self.id, task);
    }

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

    pub(crate) fn sleep(&mut self) -> Option<Pop<T>> {
        let address = &*self.core as *const QueueCore<T> as usize;
        let mut task = None;
        let mut timeout = Some(Instant::now() + self.core.config.max_idle_time);
        let id = self.id;
        loop {
            let res = unsafe {
                parking_lot_core::park(
                    address,
                    || {
                        if timeout.is_some() && !self.core.sleep() || self.core.is_shutdown() {
                            return false;
                        }
                        task = self.local_queue.pop();
                        task.is_none()
                    },
                    || {},
                    |_, _| {},
                    ParkToken(id),
                    timeout,
                )
            };
            return match res {
                ParkResult::Unparked(_) | ParkResult::Invalid => {
                    self.core.wake();
                    task
                },
                ParkResult::TimedOut => {
                    timeout = None;
                    continue;
                }
            };
        }
    }
}