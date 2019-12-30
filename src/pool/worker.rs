// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::{Local, Runner};
use crate::queue::{Pop, TaskCell};
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
    fn slow_pop(&mut self) -> Option<Pop<T>> {
        // Wait some time before going to sleep, which is more expensive.
        let mut spin = SpinWait::new();
        loop {
            if let Some(t) = self.local.pop() {
                return Some(t);
            }
            if !spin.spin() {
                break;
            }
        }
        self.runner.pause(&mut self.local);
        let t = self.local.pop_or_sleep();
        if t.is_some() {
            self.runner.resume(&mut self.local);
        }
        t
    }

    pub fn run(mut self) {
        self.runner.start(&mut self.local);
        while !self.local.core().is_shutdown() {
            let task = match self.local.pop() {
                Some(t) => t,
                None => match self.slow_pop() {
                    Some(t) => t,
                    None => continue,
                },
            };
            self.runner.handle(&mut self.local, task.task_cell);
        }
        self.runner.end(&mut self.local);
    }
}
