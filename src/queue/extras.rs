// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Instant;

/// The extras for the task cells pushed into a simple task queue.
///
/// [`Default::default`] can be used to create a [`SimpleQueueExtras`] for
/// a task cell.
#[derive(Debug, Default, Clone, Copy)]
pub struct Extras {
    /// The instant when the task cell is pushed to the queue.
    pub schedule_time: Option<Instant>,
}
