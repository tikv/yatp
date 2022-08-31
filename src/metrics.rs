// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Metrics of the thread pool.

use lazy_static::lazy_static;
use prometheus::*;
use std::sync::Mutex;

lazy_static! {
    /// Elapsed time of each level in the multilevel task queue.
    pub static ref MULTILEVEL_LEVEL_ELAPSED: IntCounterVec = IntCounterVec::new(
        new_opts(
            "multilevel_level_elapsed",
            "elapsed time of each level in the multilevel task queue"
        ),
        &["name", "level"]
    )
    .unwrap();

    /// The chance that a level 0 task is scheduled to run.
    pub static ref MULTILEVEL_LEVEL0_CHANCE: GaugeVec = GaugeVec::new(
        new_opts(
            "multilevel_level0_chance",
            "the chance that a level 0 task is scheduled to run"
        ),
        &["name"]
    )
    .unwrap();

    /// The total duration of a task waiting in queue.
    pub static ref TASK_WAIT_DURATION: HistogramVec = HistogramVec::new(
        new_histogram_opts(
            "yatp_task_wait_duration",
            "Bucketed histogram of task wait time in queue",
            exponential_buckets(0.00001, 2.0, 20).unwrap()
        ),
        &["name"]
    )
    .unwrap();

    /// Total execute duration of one task.
    pub static ref TASK_EXEC_DURATION: HistogramVec = HistogramVec::new(
        new_histogram_opts(
            "yatp_task_exec_duration",
            "Bucketed histogram of task total exec time",
            exponential_buckets(0.00001, 2.0, 20).unwrap()
        ),
        &["name"]
    )
    .unwrap();

    /// Task's execution time duration one time slice.
    pub static ref TASK_POLL_DURATION: HistogramVec = HistogramVec::new(
        new_histogram_opts(
            "yatp_task_poll_duration",
            "Bucketed histogram of task exec time of a single poll per level",
            exponential_buckets(0.00001, 2.0, 20).unwrap()
        ),
        &["name", "level"]
    )
    .unwrap();

    /// Histogrm for how many times a task be scheduled before finish.
    pub static ref TASK_EXEC_TIMES: HistogramVec = HistogramVec::new(
        new_histogram_opts(
            "yatp_task_execute_times",
            "Bucketed histogram of task exec times",
            exponential_buckets(1.0, 2.0, 10).unwrap()
        ),
        &["name"]
    )
    .unwrap();

    static ref NAMESPACE: Mutex<Option<String>> = Mutex::new(None);
}

/// Sets the namespace used in the metrics. This function should be called before
/// the metrics are used or any thread pool is created.
///
/// The namespace is missing by default.
pub fn set_namespace(s: Option<impl Into<String>>) {
    *NAMESPACE.lock().unwrap() = s.map(Into::into)
}

fn new_opts(name: &str, help: &str) -> Opts {
    let mut opts = Opts::new(name, help);
    if let Some(ref namespace) = *NAMESPACE.lock().unwrap() {
        opts = opts.namespace(namespace);
    }
    opts
}

fn new_histogram_opts(name: &str, help: &str, buckets: Vec<f64>) -> HistogramOpts {
    let mut opts = HistogramOpts::new(name, help).buckets(buckets);
    if let Some(ref namespace) = *NAMESPACE.lock().unwrap() {
        opts = opts.namespace(namespace);
    }

    opts
}
