// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

mod simple;

/// A cell containing a task and needed extra information.
pub trait TaskCell {
    /// Extra information in the cell.
    type Extras;

    /// Gets mutable extra information.
    fn mut_extras(&mut self) -> &mut Self::Extras;
}

/// A Task queue for thread pool.
///
/// Unlike a general MPMC queues, it's not required to be `Sync` on the
/// consumer side. Thread pool will use one consumer local queue per thread,
/// which make it possible to do extreme optimizations and define complicated
/// data struct.
pub trait TaskQueue: Clone {
    type Consumer: Consumer;
    type TaskCell: TaskCell;

    /// Creates a queue with a promise to only use at most `con` consumers
    /// at the same time.
    fn new(con: usize) -> (Self, Vec<Self::Consumer>);

    /// Pushes a task to the queue.
    ///
    /// If the queue is closed, the method should behave like no-op.
    fn push(&self, task: Self::TaskCell);
}

/// The consumer of a task queue.
pub trait Consumer {
    type TaskCell: TaskCell;

    fn pop(&mut self) -> Option<Self::TaskCell>;
}
