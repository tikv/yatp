// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! The pool implement details.
//!
//! To build your own thread pool while reusing the scheduling design of
//! the crate, you need to implement `Runner` trait.

mod builder;
mod runner;
mod spawn;
mod worker;

pub use self::builder::{Builder, SchedConfig};
pub use self::runner::{CloneRunnerBuilder, Runner, RunnerBuilder};
pub use self::spawn::{build_spawn, Local, Remote};

use crate::queue::{TaskCell, WithExtras};
use std::mem;
use std::sync::Mutex;
use std::thread::JoinHandle;

/// A generic thread pool.
pub struct ThreadPool<T: TaskCell + Send + 'static> {
    remote: Remote<T>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl<T: TaskCell + Send + 'static> ThreadPool<T> {
    /// Spawns the task into the thread pool.
    ///
    /// If the pool is shutdown, it becomes no-op.
    pub fn spawn(&self, t: impl WithExtras<T>) {
        self.remote.spawn(t);
    }

    /// Shutdowns the pool.
    ///
    /// Closes the queue and wait for all threads to exit.
    pub fn shutdown(&self) {
        self.remote.stop();
        let mut threads = mem::replace(&mut *self.threads.lock().unwrap(), Vec::new());
        for j in threads.drain(..) {
            j.join().unwrap();
        }
    }

    /// Get a remote queue for spawning tasks without owning the thread pool.
    pub fn remote(&self) -> &Remote<T> {
        &self.remote
    }
}

impl<T: TaskCell + Send + 'static> Drop for ThreadPool<T> {
    /// Will shutdown the thread pool if it has not.
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests;
