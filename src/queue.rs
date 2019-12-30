// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! The task queues available for the thread pool.
//!
//! A task queue has two parts: a shared `[TaskInjector]` and several
//! [`LocalQueue`]s. Unlike usual MPMC queues, [`LocalQueue`] is not required
//! to be `Sync`. The thread pool will use one [`LocalQueue`] per thread,
//! which make it possible to do extreme optimizations and define complicated
//! data structs.

pub mod multilevel;

mod extras;
mod work_stealing;

pub use self::extras::Extras;

use std::time::Instant;

/// A cell containing a task and needed extra information.
pub trait TaskCell {
    /// Gets mutable extra information.
    fn mut_extras(&mut self) -> &mut Extras;
}

/// The injector of a task queue.
pub struct TaskInjector<T>(InjectorInner<T>);

enum InjectorInner<T> {
    WorkStealing(work_stealing::TaskInjector<T>),
    Multilevel(multilevel::TaskInjector<T>),
}

impl<T: TaskCell + Send> TaskInjector<T> {
    /// Pushes a task to the queue.
    pub fn push(&self, task_cell: T) {
        match &self.0 {
            InjectorInner::WorkStealing(q) => q.push(task_cell),
            InjectorInner::Multilevel(q) => q.push(task_cell),
        }
    }
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
pub struct LocalQueue<T>(LocalQueueInner<T>);

enum LocalQueueInner<T> {
    WorkStealing(work_stealing::LocalQueue<T>),
    Multilevel(multilevel::LocalQueue<T>),
}

impl<T: TaskCell + Send> LocalQueue<T> {
    /// Pushes a task to the local queue.
    pub fn push(&mut self, task_cell: T) {
        match &mut self.0 {
            LocalQueueInner::WorkStealing(q) => q.push(task_cell),
            LocalQueueInner::Multilevel(q) => q.push(task_cell),
        }
    }

    /// Gets a task cell from the queue. Returns `None` if there is no task cell
    /// available.
    pub fn pop(&mut self) -> Option<Pop<T>> {
        match &mut self.0 {
            LocalQueueInner::WorkStealing(q) => q.pop(),
            LocalQueueInner::Multilevel(q) => q.pop(),
        }
    }
}

/// Creates a task queue that allows given number consumers.
pub fn work_stealing<T>(local_num: usize) -> (TaskInjector<T>, Vec<LocalQueue<T>>) {
    let (injector, locals) = work_stealing::create(local_num);
    (
        TaskInjector(InjectorInner::WorkStealing(injector)),
        locals
            .into_iter()
            .map(|i| LocalQueue(LocalQueueInner::WorkStealing(i)))
            .collect(),
    )
}
