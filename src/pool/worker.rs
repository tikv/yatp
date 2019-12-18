// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::SchedConfig;
use crate::queue::{LocalQueue, TaskInjector, TaskCell, Pop};
use crate::runner::{LocalSpawn, RemoteSpawn, Runner};
use parking_lot_core::{ParkResult, ParkToken, UnparkToken};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

const SHUTDOWN_BIT: usize = 1;
const WORKER_COUNT_SHIFT: usize = 1;
const WORKER_COUNT_BASE: usize = 2;

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

    fn ensure_workers(&self, source: usize) {
        let cnt = self.active_workers.load(Ordering::Relaxed);
        if (cnt >> WORKER_COUNT_SHIFT) >= self.config.max_thread_count
            || (cnt & SHUTDOWN_BIT) == SHUTDOWN_BIT
        {
            return;
        }

        let addr = self as *const QueueCore<T> as usize;
        unsafe {
            parking_lot_core::unpark_one(addr, |_| UnparkToken(source));
        }
    }

    fn stop(&self, source: usize) {
        self.active_workers.fetch_or(SHUTDOWN_BIT, Ordering::SeqCst);
        let addr = self as *const QueueCore<T> as usize;
        unsafe {
            parking_lot_core::unpark_all(addr, UnparkToken(source));
        }
    }
}

impl<T: TaskCell + Send> QueueCore<T> {
    fn push(&self, source: usize, task: T) {
        self.global_queue.push(task);
        self.ensure_workers(source);
    }
}

pub(crate) struct WorkerThread<T, R> {
    local: Local<T>,
    runner: R,
}

impl<T, R> WorkerThread<T, R> {
    pub fn new(
        id: usize,
        local_queue: LocalQueue<T>,
        core: Arc<QueueCore<T>>,
        runner: R,
    ) -> WorkerThread<T, R> {
        WorkerThread {
            local: Local {
                id,
                local_queue,
                core,
            },
            runner,
        }
    }
}

impl<T, R> WorkerThread<T, R>
    where
        T: TaskCell + Send,
        R: Runner<Spawn = Local<T>>,
    {
    fn sleep(&mut self) -> Option<Pop<T>> {
        let address = &*self.local.core as *const QueueCore<T> as usize;
        let mut task = None;
        let mut timeout = Some(Instant::now() + self.local.core.config.max_idle_time);
        let id = self.local.id;
        loop {
            let res = unsafe {
                parking_lot_core::park(
                    address,
                    || {
                        let mut cnt = self.local.core.active_workers.load(Ordering::SeqCst);
                        if cnt & SHUTDOWN_BIT == SHUTDOWN_BIT {
                            return false;
                        }
                        if timeout.is_some() {
                            loop {
                                match self.local.core.active_workers.compare_exchange_weak(
                                    cnt,
                                    cnt - WORKER_COUNT_BASE,
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                ) {
                                    Ok(_) => break,
                                    Err(n) => cnt = n,
                                }
                                
                                if cnt & SHUTDOWN_BIT == SHUTDOWN_BIT {
                                    return false;
                                }
                            }
                        }
                        task = self.local.local_queue.pop();
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
                    let mut cnt = self.local.core.active_workers.load(Ordering::SeqCst);
                    loop {
                        match self.local.core.active_workers.compare_exchange_weak(
                            cnt,
                            cnt + WORKER_COUNT_BASE,
                            Ordering::SeqCst,
                            Ordering::SeqCst,
                        ) {
                            Ok(_) => break,
                            Err(n) => cnt = n,
                        }
                    }
                    task
                },
                ParkResult::TimedOut => {
                    timeout = None;
                    continue;
                }
            };
        }
    }

    pub fn run(mut self) {
        self.runner.start(&mut self.local);
        while self.local.core.active_workers.load(Ordering::SeqCst) & SHUTDOWN_BIT != SHUTDOWN_BIT {
            let task = match self.local.local_queue.pop() {
                Some(t) => t,
                None => {
                    self.runner.pause(&mut self.local);
                    match self.sleep() {
                        Some(t) => {
                            self.runner.resume(&mut self.local);
                            t
                        }
                        None => continue,
                    }
                }
            };
            self.runner.handle(&mut self.local, task.task_cell);
        }
        self.runner.end(&mut self.local);
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

impl<T> RemoteSpawn for Remote<T>
where
    T: TaskCell + Send,
{
    type TaskCell = T;

    fn spawn(&self, task: T) {
        Remote::spawn(self, task)
    }
}

impl<T> Clone for Remote<T> {
    fn clone(&self) -> Remote<T> {
        Remote {
            core: self.core.clone(),
        }
    }
}

pub struct Local<T> {
    id: usize,
    local_queue: LocalQueue<T>,
    core: Arc<QueueCore<T>>,
}

impl<T: TaskCell + Send> LocalSpawn for Local<T> {
    type TaskCell = T;
    type Remote = Remote<T>;

    fn spawn(&mut self, task: T) {
        self.local_queue.push(task);
    }

    fn spawn_remote(&self, task: T) {
        self.core.push(self.id, task);
    }

    fn remote(&self) -> Remote<T> {
        Remote {
            core: self.core.clone(),
        }
    }
}
