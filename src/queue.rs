// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

/// A Task queue for thread pool.
///
/// Unlike a general MPMC queues, it's not required to be `Sync` on the
/// consumer side. Thread pool will use one consumer local queue per thread,
/// which make it possible to do extreme optimizations and define complicated
/// data struct.
pub trait TaskQueue: Clone {
    type Consumer;
    type TaskCell;

    /// Creates a queue with a promise to only use at most `con` consumers
    /// at the same time.
    fn with_consumers(con: usize) -> Self;

    /// Pushes a task to the queue.
    ///
    /// If the queue is closed, the method should behave like no-op.
    fn push(&self, task: Self::TaskCell);

    /// Closes the queue so that no more tasks can be scheduled.
    fn close(&self);
}
