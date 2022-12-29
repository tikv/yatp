// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! The pool implement details.
//!
//! To build your own thread pool while reusing the scheduling design of
//! the crate, you need to implement `Runner` trait.

mod builder;
mod runner;
pub(crate) mod spawn;
mod worker;

pub use self::builder::{Builder, SchedConfig};
pub use self::runner::{CloneRunnerBuilder, Runner, RunnerBuilder};
pub(crate) use self::spawn::WeakRemote;
pub use self::spawn::{build_spawn, Local, Remote};

use crate::queue::{TaskCell, WithExtras};
use std::mem;
use std::sync::Mutex;
use std::thread::{self, JoinHandle};

/// A generic thread pool.
pub struct ThreadPool<T: TaskCell + Send> {
    remote: Remote<T>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl<T: TaskCell + Send> ThreadPool<T> {
    /// Spawns the task into the thread pool.
    ///
    /// If the pool is shutdown, it becomes no-op.
    pub fn spawn(&self, t: impl WithExtras<T>) {
        self.remote.spawn(t);
    }

    /// Scale workers of the thread pool, the adjustable range is `min_thread_count`
    /// to `max_thread_count`, if this value exceeds `max_thread_count` or zero, it
    /// will be adjusted to `max_thread_count`, if this value is between zero and
    /// `min_thread_count`, it will be adjusted to `min_thread_count`.
    ///
    /// Notice:
    /// 1. The effect of scaling may be delayed, eg:
    ///    - Threads run long-term tasks, resulting in inability to scale down in
    ///      time, until they have no task to process and can sleep;
    ///    - Scaling up relies on spawning tasks to the thread pool to unpark new
    ///      threads, so the delay depends on the time interval between scale and
    ///      spawn;
    /// 2. If lots of tasks have been spawned before start, there may be more than
    ///    `core_thread_count` (max up to `max_thread_count`) threads running at a
    ///    moment until they can sleep (no tasks to run), then scheduled based on
    ///    `core_thread_count`;
    pub fn scale_workers(&self, new_thread_count: usize) {
        self.remote.scale_workers(new_thread_count);
    }

    /// Shutdowns the pool.
    ///
    /// Closes the queue and wait for all threads to exit.
    pub fn shutdown(&self) {
        self.remote.stop();
        let mut threads = mem::take(&mut *self.threads.lock().unwrap());
        let curr_id = thread::current().id();
        for j in threads.drain(..) {
            if curr_id != j.thread().id() {
                j.join().unwrap();
            }
        }
    }

    /// Get a remote queue for spawning tasks without owning the thread pool.
    pub fn remote(&self) -> &Remote<T> {
        &self.remote
    }
}

impl<T: TaskCell + Send> Drop for ThreadPool<T> {
    /// Will shutdown the thread pool if it has not.
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(test)]
mod tests;
