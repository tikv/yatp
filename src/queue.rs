// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! The task queues available for the thread pool.
//!
//! A task queue has two parts: a shared `[TaskInjector]` and several
//! [`LocalQueue`]s. Unlike usual MPMC queues, [`LocalQueue`] is not required
//! to be `Sync`. The thread pool will use one [`LocalQueue`] per thread,
//! which make it possible to do extreme optimizations and define complicated
//! data structs.

pub mod multilevel;
pub mod priority;

mod custom;
mod extras;
mod single_level;

pub use self::custom::{Builder as CustomBuilder, Config as CustomConfig, TaskQueue};
pub use self::extras::Extras;

use std::time::Instant;

/// A cell containing a task and needed extra information.
pub trait TaskCell: 'static {
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
    Priority(priority::TaskInjector<T>),
    Custom(custom::TaskInjector<T>),
}

impl<T: TaskCell + Send + 'static> TaskInjector<T> {
    /// Pushes a task to the queue.
    #[inline]
    pub fn push(&self, task_cell: T) {
        match &self.0 {
            InjectorInner::SingleLevel(q) => q.push(task_cell),
            InjectorInner::Multilevel(q) => q.push(task_cell),
            InjectorInner::Priority(q) => q.push(task_cell),
            InjectorInner::Custom(q) => q.push(task_cell),
        }
    }

    #[inline]
    pub fn default_extras(&self) -> Extras {
        match &self.0 {
            InjectorInner::SingleLevel(_) => Extras::single_level(),
            InjectorInner::Multilevel(_)
            | InjectorInner::Priority(_)
            | InjectorInner::Custom(_) => Extras::multilevel_default(),
        }
    }

    /// Attempts to evict the lowest-priority task from the queue if the
    /// incoming priority is strictly higher (lower numeric value).
    ///
    /// Returns `Some(task)` on successful eviction, or `None` if:
    /// - The queue is empty.
    /// - The incoming priority is not strictly higher than the lowest queued.
    /// - A concurrent caller already removed the candidate (best-effort).
    /// - The queue is not a priority queue (single-level and multilevel
    ///   queues do not support eviction).
    ///
    /// Callers may retry on `None` if they need stronger guarantees under
    /// contention.
    #[inline]
    pub fn try_evict_lowest(&self, incoming_priority: u64) -> Option<T> {
        match &self.0 {
            InjectorInner::Priority(q) => q.try_evict(incoming_priority),
            _ => None,
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

/// Result of attempting to pop a task from a task queue.
///
/// A queue should return [`PopResult::Empty`] only when it has no queued
/// task at all. If the queue still owns tasks but none of them can run now,
/// return [`PopResult::Pending`] instead. A later push may make work ready
/// before `retry_at`; otherwise the worker must retry no later than `retry_at`.
pub enum PopResult<T> {
    /// A task is ready and should be scheduled immediately.
    Ready(Pop<T>),
    /// The queue has at least one task, but no task can be scheduled now.
    ///
    /// `retry_at` specifies the earliest time when the worker should try to
    /// pop from the queue again.
    Pending {
        /// Earliest time when the worker should retry popping from the queue.
        retry_at: Instant,
    },
    /// The queue has no task.
    Empty,
}

impl<T> PopResult<T> {
    /// Returns `true` if the pop result contains a ready task.
    #[inline]
    pub fn is_ready(&self) -> bool {
        matches!(self, PopResult::Ready(_))
    }

    /// Returns `true` if the queue has tasks but none can run yet.
    #[inline]
    pub fn is_pending(&self) -> bool {
        matches!(self, PopResult::Pending { .. })
    }

    /// Returns `true` if the queue has no task.
    #[inline]
    pub fn is_empty(&self) -> bool {
        matches!(self, PopResult::Empty)
    }

    /// Returns the ready task, panicking if the result is not ready.
    #[inline]
    pub fn unwrap_ready(self) -> Pop<T> {
        match self {
            PopResult::Ready(pop) => pop,
            PopResult::Pending { .. } => panic!("called `PopResult::unwrap_ready()` on `Pending`"),
            PopResult::Empty => panic!("called `PopResult::unwrap_ready()` on `Empty`"),
        }
    }

    /// Returns the retry time, panicking if the result is not pending.
    #[inline]
    pub fn unwrap_pending(self) -> Instant {
        match self {
            PopResult::Pending { retry_at } => retry_at,
            PopResult::Ready(_) => panic!("called `PopResult::unwrap_pending()` on `Ready`"),
            PopResult::Empty => panic!("called `PopResult::unwrap_pending()` on `Empty`"),
        }
    }

    /// Verifies the result is empty, panicking otherwise.
    #[inline]
    pub fn unwrap_empty(self) {
        match self {
            PopResult::Empty => {}
            PopResult::Ready(_) => panic!("called `PopResult::unwrap_empty()` on `Ready`"),
            PopResult::Pending { .. } => panic!("called `PopResult::unwrap_empty()` on `Pending`"),
        }
    }
}

impl<T> From<Option<Pop<T>>> for PopResult<T> {
    #[inline]
    fn from(pop: Option<Pop<T>>) -> PopResult<T> {
        match pop {
            Some(pop) => PopResult::Ready(pop),
            None => PopResult::Empty,
        }
    }
}

/// The local queue of a task queue.
pub(crate) struct LocalQueue<T>(LocalQueueInner<T>);

enum LocalQueueInner<T> {
    SingleLevel(single_level::LocalQueue<T>),
    Multilevel(multilevel::LocalQueue<T>),
    Priority(priority::LocalQueue<T>),
    Custom(custom::LocalQueue<T>),
}

impl<T: TaskCell + Send> LocalQueue<T> {
    /// Pushes a task to the local queue.
    #[inline]
    pub fn push(&mut self, task_cell: T) {
        match &mut self.0 {
            LocalQueueInner::SingleLevel(q) => q.push(task_cell),
            LocalQueueInner::Multilevel(q) => q.push(task_cell),
            LocalQueueInner::Priority(q) => q.push(task_cell),
            LocalQueueInner::Custom(q) => q.push(task_cell),
        }
    }

    /// Gets a task cell from the queue.
    #[inline]
    pub fn pop(&mut self) -> PopResult<T> {
        match &mut self.0 {
            LocalQueueInner::SingleLevel(q) => q.pop().into(),
            LocalQueueInner::Multilevel(q) => q.pop().into(),
            LocalQueueInner::Priority(q) => q.pop().into(),
            LocalQueueInner::Custom(q) => q.pop(),
        }
    }

    /// Forcefully drains all currently queued tasks.
    #[inline]
    pub fn drain(&mut self) {
        match &mut self.0 {
            LocalQueueInner::SingleLevel(q) => while q.pop().is_some() {},
            LocalQueueInner::Multilevel(q) => while q.pop().is_some() {},
            LocalQueueInner::Priority(q) => while q.pop().is_some() {},
            LocalQueueInner::Custom(q) => q.drain(),
        }
    }

    #[inline]
    pub fn default_extras(&self) -> Extras {
        match &self.0 {
            LocalQueueInner::SingleLevel(_) => Extras::single_level(),
            LocalQueueInner::Multilevel(_) => Extras::multilevel_default(),
            LocalQueueInner::Priority(_) => Extras::single_level(),
            LocalQueueInner::Custom(_) => Extras::multilevel_default(),
        }
    }

    /// If there are tasks in the local queue, returns true. Otherwise, pulls
    /// tasks from the global queue and returns whether it succeeds.
    #[inline]
    pub fn has_tasks_or_pull(&mut self) -> bool {
        match &mut self.0 {
            LocalQueueInner::SingleLevel(q) => q.has_tasks_or_pull(),
            LocalQueueInner::Multilevel(q) => q.has_tasks_or_pull(),
            LocalQueueInner::Priority(q) => q.has_tasks_or_pull(),
            LocalQueueInner::Custom(q) => q.has_tasks_or_pull(),
        }
    }
}

/// Supported available queues.
#[derive(Default)]
pub enum QueueType<T = ()> {
    /// A single level work stealing queue.
    #[default]
    SingleLevel,
    /// A multilevel feedback queue.
    ///
    /// More to see: https://en.wikipedia.org/wiki/Multilevel_feedback_queue.
    Multilevel(multilevel::Builder),
    /// A concurrent prioirty queue.
    Priority(priority::Builder),
    /// A custom task queue.
    Custom(CustomBuilder<T>),
}

impl<T> From<multilevel::Builder> for QueueType<T> {
    fn from(b: multilevel::Builder) -> QueueType<T> {
        QueueType::Multilevel(b)
    }
}

impl<T> From<priority::Builder> for QueueType<T> {
    fn from(b: priority::Builder) -> QueueType<T> {
        QueueType::Priority(b)
    }
}

impl<T> From<CustomBuilder<T>> for QueueType<T> {
    fn from(b: CustomBuilder<T>) -> QueueType<T> {
        QueueType::Custom(b)
    }
}

pub(crate) fn build<T: 'static>(
    ty: QueueType<T>,
    local_num: usize,
) -> (TaskInjector<T>, Vec<LocalQueue<T>>) {
    match ty {
        QueueType::SingleLevel => single_level(local_num),
        QueueType::Multilevel(b) => b.build(local_num),
        QueueType::Priority(b) => b.build(local_num),
        QueueType::Custom(b) => b.build(local_num),
    }
}

/// Creates a task queue that allows given number consumers.
fn single_level<T>(local_num: usize) -> (TaskInjector<T>, Vec<LocalQueue<T>>) {
    let (injector, locals) = single_level::create(local_num);
    (
        TaskInjector(InjectorInner::SingleLevel(injector)),
        locals
            .into_iter()
            .map(|i| LocalQueue(LocalQueueInner::SingleLevel(i)))
            .collect(),
    )
}
