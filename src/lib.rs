// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Yatp is a thread pool that tries to be adaptive, responsive and generic.

mod runner;

pub use self::runner::{Runner, LocalSpawn, RemoteSpawn};