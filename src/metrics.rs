// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Metrics of the thread pool.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    /// Elapsed time of each level in the multilevel task queue.
    pub static ref MULTILEVEL_LEVEL_ELAPSED: IntCounterVec = IntCounterVec::new(
        Opts::new(
            "multilevel_level_elapsed",
            "elapsed time of each level in the multilevel task queue"
        ),
        &["name", "level"]
    )
    .unwrap();

    /// The chance that a level 0 task is scheduled to run.
    pub static ref MULTILEVEL_LEVEL0_CHANCE: GaugeVec = GaugeVec::new(
        Opts::new(
            "multilevel_level0_chance",
            "the chance that a level 0 task is scheduled to run"
        ),
        &["name"]
    )
    .unwrap();
}
