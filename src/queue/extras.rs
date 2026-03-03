// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use super::multilevel::ElapsedTime;

use rand::prelude::*;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The source of a task, indicating where the task comes from when popped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskSource {
    /// Task popped from the local queue (most efficient path).
    LocalQueue,
    /// Task popped from the global injector queue.
    GlobalQueue,
    /// Task stolen from another worker's local queue.
    OtherWorker,
}

/// Indicates how the worker acquired the task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireState {
    /// Task was popped immediately from the local queue without waiting.
    Immediate,
    /// Task was acquired after the worker thread spun (busy-waited).
    AfterSpin,
    /// Task was acquired after the worker thread was parked (slept).
    AfterPark,
}

/// The extras for the task cells pushed into a queue.
#[derive(Debug, Clone)]
pub struct Extras {
    /// the instant when the task is spawned.
    pub(crate) start_time: Instant,
    /// The instant when the task cell is pushed to the queue.
    pub(crate) schedule_time: Option<Instant>,
    /// The identifier of the task. Only used in the multilevel task queue.
    pub(crate) task_id: u64,
    // The total time spent on handling this task.
    pub(crate) running_time: Option<Arc<ElapsedTime>>,
    /// The total time spent on handling this task. Only used in the multilevel task
    /// queue. This total time will accumulate the execution time acorss multiple tasks
    /// with the same task_id.
    pub(crate) total_running_time: Option<Arc<ElapsedTime>>,
    /// The level of queue which this task comes from. Only used in the
    /// multilevel task queue.
    pub(crate) current_level: u8,
    /// If `fixed_level` is `Some`, this task is always pushed to the given
    /// level. Only used in the multilevel task queue.
    pub(crate) fixed_level: Option<u8>,
    /// Number of execute times
    pub(crate) exec_times: u32,
    /// Extra metadata of this task. User can use this field to store arbitrary data. It is useful
    /// in some case to implement more complext `TaskPriorityProvider` in the priority task queue.
    pub(crate) metadata: Vec<u8>,
    /// The source of the task, indicating where the task comes from when popped.
    /// This field is set when the task is popped from the queue.
    pub(crate) task_source: Option<TaskSource>,
    /// Indicates how the worker acquired the task.
    /// This field is set when the task is popped from the queue.
    pub(crate) acquire_state: Option<AcquireState>,
}

impl Extras {
    /// Creates a default `Extras` for task cells pushed into a single level
    /// task queue.
    pub fn single_level() -> Extras {
        Extras {
            start_time: Instant::now(),
            schedule_time: None,
            task_id: 0,
            running_time: None,
            total_running_time: None,
            current_level: 0,
            fixed_level: None,
            exec_times: 0,
            metadata: Vec::new(),
            task_source: None,
            acquire_state: None,
        }
    }

    /// Creates a default `Extras` for task cells pushed into a multilevel task
    /// queue. It generates a random u64 as task id and does not specify the
    /// fixed level.
    pub fn multilevel_default() -> Extras {
        Self::new_multilevel(thread_rng().next_u64(), None)
    }

    /// Creates an `Extras` for task cells pushed into a multilevel task
    /// queue with custom settings.
    pub fn new_multilevel(task_id: u64, fixed_level: Option<u8>) -> Extras {
        Extras {
            start_time: Instant::now(),
            schedule_time: None,
            task_id,
            running_time: Some(Arc::new(ElapsedTime::default())),
            total_running_time: None,
            current_level: fixed_level.unwrap_or(0),
            fixed_level,
            exec_times: 0,
            metadata: Vec::new(),
            task_source: None,
            acquire_state: None,
        }
    }

    /// Gets the instant when the task is scheduled.
    pub fn schedule_time(&self) -> Option<Instant> {
        self.schedule_time
    }

    /// Gets the identifier of the task.
    pub fn task_id(&self) -> u64 {
        self.task_id
    }

    /// Gets the time spent on handling this task.
    pub fn running_time(&self) -> Option<Duration> {
        self.total_running_time
            .as_ref()
            .map(|elapsed| elapsed.as_duration())
    }

    /// Gets the level of queue which this task comes from.
    pub fn current_level(&self) -> u8 {
        self.current_level
    }

    /// Gets the metadata of this task.
    pub fn metadata(&self) -> &[u8] {
        &self.metadata
    }

    /// Gets the mutable metadata of this task.
    pub fn metadata_mut(&mut self) -> &mut Vec<u8> {
        &mut self.metadata
    }

    /// Set the metadata of this task.
    pub fn set_metadata(&mut self, metadata: Vec<u8>) {
        self.metadata = metadata;
    }

    /// Gets the source of the task.
    pub fn task_source(&self) -> Option<TaskSource> {
        self.task_source
    }

    /// Gets how the worker acquired the task.
    pub fn acquire_state(&self) -> Option<AcquireState> {
        self.acquire_state
    }
}
