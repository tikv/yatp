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
mod single_level;

pub use self::extras::Extras;

use std::time::Instant;

/// A cell containing a task and needed extra information.
pub trait TaskCell {
    /// Gets mutable extra information.
    fn mut_extras(&mut self) -> &mut Extras;
}

/// A convenient trait that support construct a TaskCell with
/// given extras.
pub trait WithExtras<T> {
    /// Return a TaskCell with the given extras.
    fn with_extras(self, extras: impl FnOnce() -> Extras) -> T;
}

impl<F: TaskCell> WithExtras<F> for F {
    fn with_extras(self, _: impl FnOnce() -> Extras) -> F {
        self
    }
}

/// The injector of a task queue.
pub(crate) struct TaskInjector<T>(InjectorInner<T>);

enum InjectorInner<T> {
    SingleLevel(single_level::TaskInjector<T>),
    Multilevel(multilevel::TaskInjector<T>),
}

impl<T: TaskCell + Send + 'static> TaskInjector<T> {
    /// Pushes a task to the queue.
    pub fn push(&self, task_cell: T) {
        match &self.0 {
            InjectorInner::SingleLevel(q) => q.push(task_cell),
            InjectorInner::Multilevel(q) => q.push(task_cell),
        }
    }

    pub fn default_extras(&self) -> Extras {
        match self.0 {
            InjectorInner::SingleLevel(_) => Extras::single_level(),
            InjectorInner::Multilevel(_) => Extras::multilevel_default(),
        }
    }

    #[cfg(test)]
    pub fn into_single_level(self) -> single_level::TaskInjector<T> {
        match self.0 {
            InjectorInner::SingleLevel(inj) => inj,
            _ => unreachable!(),
        }
    }

    #[cfg(test)]
    pub fn into_multilevel(self) -> multilevel::TaskInjector<T> {
        match self.0 {
            InjectorInner::Multilevel(inj) => inj,
            _ => unreachable!(),
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
pub(crate) struct LocalQueue<T>(LocalQueueInner<T>);

enum LocalQueueInner<T> {
    SingleLevel(single_level::LocalQueue<T>),
    Multilevel(multilevel::LocalQueue<T>),
}

impl<T: TaskCell + Send> LocalQueue<T> {
    /// Pushes a task to the local queue.
    pub fn push(&mut self, task_cell: T) {
        match &mut self.0 {
            LocalQueueInner::SingleLevel(q) => q.push(task_cell),
            LocalQueueInner::Multilevel(q) => q.push(task_cell),
        }
    }

    /// Gets a task cell from the queue. Returns `None` if there is no task cell
    /// available.
    pub fn pop(&mut self) -> Option<Pop<T>> {
        match &mut self.0 {
            LocalQueueInner::SingleLevel(q) => q.pop(),
            LocalQueueInner::Multilevel(q) => q.pop(),
        }
    }

    pub fn default_extras(&self) -> Extras {
        match self.0 {
            LocalQueueInner::SingleLevel(_) => Extras::single_level(),
            LocalQueueInner::Multilevel(_) => Extras::multilevel_default(),
        }
    }

    pub fn local_injector(&self) -> LocalInjector<T> {
        match &self.0 {
            LocalQueueInner::SingleLevel(q) => LocalInjector::SingleLevel(q.local_injector()),
            LocalQueueInner::Multilevel(q) => LocalInjector::Multilevel(q.local_injector()),
        }
    }

    #[cfg(test)]
    pub fn into_single_level(self) -> single_level::LocalQueue<T> {
        match self.0 {
            LocalQueueInner::SingleLevel(q) => q,
            _ => unreachable!(),
        }
    }

    #[cfg(test)]
    pub fn into_multilevel(self) -> multilevel::LocalQueue<T> {
        match self.0 {
            LocalQueueInner::Multilevel(q) => q,
            _ => unreachable!(),
        }
    }
}

pub(crate) enum LocalInjector<T> {
    SingleLevel(single_level::LocalInjector<T>),
    Multilevel(multilevel::LocalInjector<T>),
}

impl<T: TaskCell> LocalInjector<T> {
    pub(crate) fn push(&self, task: T) {
        match self {
            LocalInjector::SingleLevel(inj) => inj.push(task),
            LocalInjector::Multilevel(inj) => inj.push(task),
        }
    }
}

/// Supported available queues.
pub enum QueueType {
    /// A single level work stealing queue.
    SingleLevel,
    /// A multilevel feedback queue.
    ///
    /// More to see: https://en.wikipedia.org/wiki/Multilevel_feedback_queue.
    Multilevel(multilevel::Builder),
}

impl Default for QueueType {
    fn default() -> QueueType {
        QueueType::SingleLevel
    }
}

impl From<multilevel::Builder> for QueueType {
    fn from(b: multilevel::Builder) -> QueueType {
        QueueType::Multilevel(b)
    }
}

pub(crate) fn build<T: Send + 'static>(
    ty: QueueType,
    local_num: usize,
) -> (TaskInjector<T>, Vec<LocalQueueBuilder<T>>) {
    match ty {
        QueueType::SingleLevel => single_level::create(local_num),
        QueueType::Multilevel(b) => b.build(local_num),
    }
}

pub(crate) type LocalQueueBuilder<T> = Box<dyn FnOnce() -> LocalQueue<T> + Send>;
