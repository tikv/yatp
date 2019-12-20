// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::Local;
use crate::queue::{LocalQueue, TaskCell};
use crate::pool::spawn::QueueCore;
use crate::runner::Runner;
use std::sync::Arc;

pub(crate) struct WorkerThread<T, R> {
    local: Local<T>,
    runner: R,
}

impl<T, R> WorkerThread<T, R> {
    pub fn new(
        local: Local<T>,
        runner: R,
    ) -> WorkerThread<T, R> {
        WorkerThread {
            local,
            runner,
        }
    }
}

impl<T, R> WorkerThread<T, R>
    where
        T: TaskCell + Send,
        R: Runner<TaskCell=T>,
    {
    pub fn run(mut self) {
        self.runner.start(&mut self.local);
        while !self.local.core().is_shutdown() {
            let task = match self.local.pop() {
                Some(t) => t,
                None => {
                    self.runner.pause(&mut self.local);
                    match self.local.sleep() {
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