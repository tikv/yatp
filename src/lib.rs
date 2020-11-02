// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

#![feature(weak_into_raw)]
#![warn(missing_docs)]

//! Yatp is a thread pool that tries to be adaptive, responsive and generic.

pub mod metrics;
pub mod pool;
pub mod queue;
pub mod task;

pub use self::pool::{Builder, Remote, ThreadPool};
