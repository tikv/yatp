// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! The task queues available for the thread pool.
//!
//! A task queue has two parts: a shared `[TaskInjector]` and several
//! [`LocalQueue`]s. Unlike usual MPMC queues, [`LocalQueue`] is not required
//! to be `Sync`. The thread pool will use one [`LocalQueue`] per thread,
//! which make it possible to do extreme optimizations and define complicated
//! data structs.

use std::time::Instant;

/// A cell containing a task and needed extra information.
pub trait TaskCell {
    /// Extra information in the cell.
    type Extras;

    /// Gets mutable extra information.
    fn mut_extras(&mut self) -> &mut Self::Extras;
}

/// The injector of a task queue.
pub trait TaskInjector: Clone {
    /// The local queue of the task queue.
    type LocalQueue: LocalQueue;
    /// The task cell in the queue.
    type TaskCell: TaskCell;

    /// Creates a queue with a promise to only have at most `num` local queues
    /// at the same time.
    fn new(num: usize) -> (Self, Vec<Self::LocalQueue>);

    /// Pushes a task to the queue.
    fn push(&self, task_cell: Self::TaskCell);
}

/// Popped task cell from a task queue.
pub struct Pop<T> {
    /// The task cell
    pub task_cell: T,

    /// When the task was pushed to the queue.
    pub schedule_time: Instant,

    /// Whether the task comes from the current [`LocalQueue`] instead of being
    /// just stolen from the injector or other [`LocalQueue`]s.
    pub from_local: bool,
}

/// The local queue of a task queue.
pub trait LocalQueue {
    /// The task cell in the queue.
    type TaskCell: TaskCell;

    /// Pushes a task to the local queue.
    fn push(&mut self, task_cell: Self::TaskCell);

    /// Gets a task cell from the queue. Rturns `None` if there is no task cell
    /// available.
    fn pop(&mut self) -> Option<Pop<Self::TaskCell>>;
}
