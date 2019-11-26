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

/// A task which can be spawned by [`RemoteSpawn`]. It must contain necessary spawn options.
pub trait RemoteTask<SpawnOptions> {
    /// Gets the spawn options.
    fn spawn_options(&self) -> &SpawnOptions;
}

/// Allows spawning a task to the thread pool from a different thread.
pub trait RemoteSpawn: Sync + Send {
    /// The task it can spawn.
    type Task: RemoteTask<Self::SpawnOptions>;
    /// The spawn options.
    type SpawnOptions;

    /// Spawns a task cell into the thread pool.
    fn spawn(&self, task: impl Into<Self::Task>);
}

/// Allows spawning a task inside the thread pool.
pub trait LocalSpawn {
    /// The task it can spawn.
    type Task;
    /// The context that associated with the task.
    type TaskContext;
    /// The remote handle that can be used in other threads.
    type Remote: RemoteSpawn;

    /// Spawns a task into the thread pool.
    fn spawn_ctx(&mut self, task: impl Into<Self::Task>, ctx: &Self::TaskContext);

    /// Gets a remote instance to allow spawn task back to the pool.
    fn remote(&self) -> Self::Remote;
}

/// Extensions to `[LocalSpawn]`.
pub trait LocalSpawnExt: LocalSpawn {
    /// Spawns a task into the thread pool.
    fn spawn(&mut self, t: impl Into<Self::Task>)
    where
        Self::TaskContext: Default,
    {
        self.spawn_ctx(t, &Default::default())
    }
}

impl<S: LocalSpawn> LocalSpawnExt for S {}

/// A builder trait that produce `Runner`.
pub trait RunnerBuilder {
    /// The runner it can build.
    type Runner: Runner;

    /// Builds a runner.
    fn build(&mut self) -> Self::Runner;
}
