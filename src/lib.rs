// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Yatp is a thread pool that tries to be adaptive, responsive and generic.

/// In Yatp model, any pieces of logic aims to be executed in thread pool is
/// called Task. There can be different definitions of Task. Some people may
/// choose `Future` as Task, some may just want callbacks, or even Actor
/// messages. But no matter what a Task is, there should be some role know how
/// to execute it. The role is call `Runner`.
///
/// The life cycle of a Runner is:
/// ```text
///   start
///     |
///     | <--- resume
///     |        |
///   handle -> pause
///     |
///    end
/// ```
///
/// Generally users should use the provided future thread pool or callback
/// thread pool instead. This is only for advance customization.
pub trait Runner {
    /// The task runner can handle.
    type Task;
    type Spawn: LocalSpawn<Task = Self::Task>;

    /// Called when the runner is started.
    ///
    /// It's guaranteed to be the first method to called before anything else.
    fn start(&mut self, _spawn: &mut Self::Spawn) {}

    /// Called when a task needs to be handled.
    ///
    /// It's possible that a task can't be finished in a single execution, in
    /// which case feel free to spawn the task again and return false to
    /// indicate the task has not been finished yet.
    fn handle(&mut self, spawn: &mut Self::Spawn, task: Self::Task) -> bool;

    /// Called when the runner is put to sleep.
    fn pause(&mut self, _spawn: &mut Self::Spawn) -> bool {
        true
    }

    /// Called when the runner is woken up.
    fn resume(&mut self, _spawn: &mut Self::Spawn) {}

    /// Called when the runner is about to be destroyed.
    ///
    /// It's guaranteed that no other method will be called after this method.
    fn end(&mut self, _spawn: &mut Self::Spawn) {}
}

/// Allows spawn a task to the thread pool from a different thread.
pub trait RemoteSpawn: Sync + Send {
    type Task;

    fn spawn(&self, t: impl Into<Self::Task>);
}

/// Allows spawn a task inside the thread pool.
pub trait LocalSpawn {
    type Task;
    type Remote: RemoteSpawn;

    fn spawn(&mut self, t: impl Into<Self::Task>);

    /// Gets a remote instance to allow spawn task back to the pool.
    fn remote(&self) -> Self::Remote;
}
