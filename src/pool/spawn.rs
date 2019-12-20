// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::queue::{TaskCell, TaskInjector};
use std::sync::Arc;

/// Allows spawning a task to the thread pool from a different thread.
pub trait RemoteSpawn: Sync + Send {
    /// The task it can spawn.
    type TaskCell;

    /// Spawns a task into the thread pool.
    fn spawn(&self, task_cell: Self::TaskCell);
}

/// Allows spawning a task inside the thread pool.
pub trait LocalSpawn {
    /// The task it can spawn.
    type TaskCell;
    /// The remote handle that can be used in other threads.
    type Remote: RemoteSpawn<TaskCell = Self::TaskCell>;

    /// Spawns a task into the thread pool.
    fn spawn(&mut self, task: Self::TaskCell);

    /// Gets a remote instance to allow spawn task back to the pool.
    fn remote(&self) -> Self::Remote;
}

/// A remote handle that can spawn tasks to the thread pool without owning
/// it.
pub struct Remote<T> {
    injector: Arc<TaskInjector<T>>,
}

impl<T> Remote<T> {
    pub(crate) fn new(injector: Arc<TaskInjector<T>>) -> Remote<T> {
        Remote { injector }
    }
}

impl<T: TaskCell> Remote<T> {
    /// Spawns the tasks into thread pool.
    ///
    /// If the thread pool is shutdown, it becomes no-op.
    pub fn spawn(&self, _t: impl Into<T>) {
        unimplemented!()
    }
}

impl<T> Clone for Remote<T> {
    fn clone(&self) -> Remote<T> {
        Remote {
            injector: self.injector.clone(),
        }
    }
}
