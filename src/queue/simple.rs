// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::{Consumer, TaskCell, TaskQueue};

use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use std::sync::Arc;

pub struct SimpleQueue<T>(Arc<Injector<T>>);

impl<T: TaskCell> TaskQueue for SimpleQueue<T> {
    type Consumer = SimpleQueueConsumer<T>;
    type TaskCell = T;

    /// Creates a queue with a promise to only use at most `con` consumers
    /// at the same time.
    fn new(con: usize) -> (Self, Vec<Self::Consumer>) {
        unimplemented!()
    }

    /// Pushes a task to the queue.
    ///
    /// If the queue is closed, the method should behave like no-op.
    fn push(&self, task: Self::TaskCell) {
        self.0.push(task);
    }
}

impl<T: TaskCell> Clone for SimpleQueue<T> {
    fn clone(&self) -> Self {
        SimpleQueue(self.0.clone())
    }
}

pub struct SimpleQueueConsumer<T>(Worker<T>);

impl<T: TaskCell> Consumer for SimpleQueueConsumer<T> {
    type TaskCell = T;

    fn pop(&mut self) -> Option<Self::TaskCell> {
        self.0.pop()
    }
}
