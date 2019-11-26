// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![warn(missing_docs)]

//! Yatp is a thread pool that tries to be adaptive, responsive and generic.

mod pool;
mod queue;
mod runner;

pub mod task;

pub use self::pool::{Builder, Remote, ThreadPool};
pub use self::runner::{LocalSpawn, RemoteSpawn, RemoteTask, Runner};
