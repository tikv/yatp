// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::Local;

/// In the model of yatp, any piece of logic aiming to be executed in a thread
/// pool is called Task. There can be different definitions of Task. Some people
/// may choose `Future` as Task, some may just want callbacks, or even Actor
/// messages. But no matter what a Task is, there should be some role know how
/// to execute it. The role is call `Runner`.
///
/// The life cycle of a Runner is:
/// ```text
///   start
///     |
///     | <---------------
///     |                |
///   reentrant_start    |
///     |                |
///     | <--- resume    | if selected durning scaling up
///     |        |       |
///   handle -> pause    |
///     |                |
///   reentrant_end ------
///     |
///    end
/// ```
///
/// Generally users should use the provided future thread pool or callback
/// thread pool instead. This is only for advance customization.
pub trait Runner {
    /// The local spawn that can be accepted to spawn tasks.
    type TaskCell;

    /// Called when the runner is started.
    ///
    /// It's guaranteed to be the first method to call before anything else.
    fn start(&mut self, _local: &mut Local<Self::TaskCell>) {}

    /// Called when the runner's thread re-participate in scheduling.
    ///
    /// It's used in the scenario of the scale workers. It will treat this
    /// thread as a new thread. It's guaranteed to be called after `start`,
    /// before others else.
    fn reentrant_start(&mut self, _local: &mut Local<Self::TaskCell>) {}

    /// Called when a task needs to be handled.
    ///
    /// It's possible that a task can't be finished in a single execution, in
    /// which case feel free to spawn the task again and return false to
    /// indicate the task has not been finished yet.
    fn handle(&mut self, local: &mut Local<Self::TaskCell>, task_cell: Self::TaskCell) -> bool;

    /// Called when the runner is put to sleep.
    fn pause(&mut self, _local: &mut Local<Self::TaskCell>) -> bool {
        true
    }

    /// Called when the runner is woken up.
    fn resume(&mut self, _local: &mut Local<Self::TaskCell>) {}

    /// Called when the runner is about to be removed from scheduler.
    ///
    /// It's used in the scenario of the scale workers. It will treat this
    /// thread as a dead thread. It's guaranteed that if this thread is not
    /// selected by scaling up, no other methods will be called after this
    /// method, unless the thread pool is shutdown down, in which case `end`
    /// will be the last method called. Otherwise it's regarded as a new
    /// thread, will re-run from `reetrant_start`.
    fn reentrant_end(&mut self, _local: &mut Local<Self::TaskCell>) {}

    /// Called when the runner is about to be destroyed.
    ///
    /// It's guaranteed that no other method will be called after this method.
    fn end(&mut self, _local: &mut Local<Self::TaskCell>) {}
}

/// A builder trait that produce `Runner`.
pub trait RunnerBuilder {
    /// The runner it can build.
    type Runner: Runner;

    /// Builds a runner.
    fn build(&mut self) -> Self::Runner;
}

/// A builder that create new Runner by cloning the old one.
pub struct CloneRunnerBuilder<R>(pub R);

impl<R: Runner + Clone> RunnerBuilder for CloneRunnerBuilder<R> {
    type Runner = R;

    fn build(&mut self) -> R {
        self.0.clone()
    }
}
