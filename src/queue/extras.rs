// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::multilevel::ElapsedTime;

use std::sync::Arc;
use std::time::Instant;
use uuid::Uuid;

/// The extras for the task cells pushed into a task queue.
#[derive(Debug, Clone)]
pub struct Extras {
    /// The instant when the task cell is pushed to the queue.
    pub(crate) schedule_time: Option<Instant>,
    /// The identifier of the task. Only used in the multilevel task queue.
    pub(crate) task_id: Uuid,
    /// The time spent on handling this task. Only used in the multilevel task
    /// queue.
    pub(crate) running_time: Option<Arc<ElapsedTime>>,
    /// The level of queue which this task comes from. Only used in the
    /// multilevel task queue.
    pub(crate) current_level: u8,
    /// If `fixed_level` is `Some`, this task is always pushed to the given
    /// level. Only used in the multilevel task queue.
    pub(crate) fixed_level: Option<u8>,
}

impl Extras {
    /// Creates a default `Extras` for task cells pushed into a simple task
    /// queue.
    pub fn simple_default() -> Extras {
        Extras {
            schedule_time: None,
            task_id: Uuid::nil(),
            running_time: None,
            current_level: 0,
            fixed_level: None,
        }
    }

    /// Creates a default `Extras` for task cells pushed into a multilevel task
    /// queue. It generates a UUID v4 as task id and does not specify the fixed
    /// level.
    pub fn multilevel_default() -> Extras {
        Self::new_multilevel(Uuid::new_v4(), None)
    }

    /// Creates an `Extras` for task cells pushed into a multilevel task
    /// queue with custom settings.
    pub fn new_multilevel(task_id: Uuid, fixed_level: Option<u8>) -> Extras {
        Extras {
            schedule_time: None,
            task_id,
            running_time: None,
            current_level: 0,
            fixed_level,
        }
    }
}
