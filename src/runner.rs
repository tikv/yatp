// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

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

    /// The local spawn that can be accepted to spawn tasks.
    type Spawn: LocalSpawn<Task = Self::Task>;

    /// Called when the runner is started.
    ///
    /// It's guaranteed to be the first method to call before anything else.
    fn start(&mut self, _spawn: &mut Self::Spawn) {}

    /// Called when a task needs to be handled.
    ///
    /// It's possible that a task can't be finished in a single execution, in
    /// which case feel free to spawn the task again and return false to
    /// indicate the task has not been finished yet.
    fn handle(
        &mut self,
        spawn: &mut Self::Spawn,
        task: Self::Task,
        ctx: &<Self::Spawn as LocalSpawn>::TaskContext,
    ) -> bool;

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

/// Allows spawning a task to the thread pool from a different thread.
pub trait RemoteSpawn: Sync + Send {
    type Task;
    type SpawnOption;

    fn spawn_opt(&self, t: impl Into<Self::Task>, opt: &Self::SpawnOption);
}

/// Extentions to `[RemoteSpawn]`.
pub trait RemoteSpawnExt: RemoteSpawn {
    fn spawn(&self, t: impl Into<Self::Task>)
    where
        Self::SpawnOption: Default,
    {
        self.spawn_opt(t, &Default::default())
    }
}

impl<S: RemoteSpawn> RemoteSpawnExt for S {}

/// Allows spawning a task inside the thread pool.
pub trait LocalSpawn {
    type Task;
    type TaskContext;
    type Remote: RemoteSpawn<Task = Self::Task>;

    fn spawn_ctx(&mut self, t: impl Into<Self::Task>, ctx: &Self::TaskContext);

    /// Gets a remote instance to allow spawn task back to the pool.
    fn remote(&self) -> Self::Remote;
}

/// Extentions to `[LocalSpawn]`.
pub trait LocalSpawnExt: LocalSpawn {
    fn spawn(&mut self, t: impl Into<Self::Task>)
    where
        Self::TaskContext: Default,
    {
        self.spawn_ctx(t, &Default::default())
    }
}

impl<S: LocalSpawn> LocalSpawnExt for S {}
