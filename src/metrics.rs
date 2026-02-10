// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! Metrics of the thread pool.

use lazy_static::lazy_static;
use prometheus::core::{Collector, Desc, Metric, MetricVec, MetricVecBuilder};
use prometheus::*;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

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

    /// Max enqueue throughput of the global task injector (QueueCore) since last scrape.
    pub static ref QUEUE_CORE_BURST_THROUGHPUT: MaxGaugeVec = MaxGaugeVec::new(
        new_opts(
            "yatp_queue_core_burst_throughput",
            "max enqueue throughput (tasks/sec) of the global task injector since last scrape"
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

/// A gauge that tracks the maximum value since the last scrape.
#[derive(Clone, Debug)]
pub struct MaxGauge {
    gauge: Gauge,
    max_val: Arc<AtomicU64>,
}

impl MaxGauge {
    /// Wraps a `Gauge` to create a `MaxGauge`. The `Gauge` should not be used directly after being wrapped, otherwise
    /// the maximum tracking will be broken.
    pub fn wrap(gauge: Gauge) -> Self {
        let val = gauge.get().to_bits();
        Self {
            gauge,
            max_val: Arc::new(AtomicU64::new(val)),
        }
    }

    /// Observe a value, keeping the maximum since the last scrape.
    pub fn observe(&self, v: f64) {
        if !v.is_finite() {
            return;
        }
        let mut current = self.max_val.load(Ordering::Relaxed);
        loop {
            if v <= f64::from_bits(current) {
                break;
            }
            match self.max_val.compare_exchange_weak(
                current,
                v.to_bits(),
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.gauge.set(v);
                    break;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Get the current maximum value without resetting it.
    pub fn get(&self) -> f64 {
        let val = self.max_val.load(Ordering::Relaxed);
        f64::from_bits(val)
    }

    fn take(&self) -> f64 {
        let val = self.max_val.swap(0f64.to_bits(), Ordering::Relaxed);
        f64::from_bits(val)
    }
}

impl Collector for MaxGauge {
    fn desc(&self) -> Vec<&Desc> {
        self.gauge.desc()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        let val = self.take();
        self.gauge.set(val);
        self.gauge.collect()
    }
}

impl Metric for MaxGauge {
    fn metric(&self) -> proto::Metric {
        let val = self.take();
        self.gauge.set(val);
        self.gauge.metric()
    }
}

/// Builder for `MaxGaugeVec`.
#[derive(Clone, Debug)]
pub struct MaxGaugeVecBuilder;

impl MaxGaugeVecBuilder {
    /// Create a new `MaxGaugeVecBuilder`.
    pub fn new() -> Self {
        Self
    }
}

impl Default for MaxGaugeVecBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricVecBuilder for MaxGaugeVecBuilder {
    type M = MaxGauge;
    type P = Opts;

    fn build(&self, opts: &Opts, vals: &[&str]) -> Result<Self::M> {
        let mut opts = opts.clone();
        for (name, val) in opts.variable_labels.iter().zip(vals.iter()) {
            opts.const_labels.insert(name.clone(), (*val).to_owned());
        }
        opts.variable_labels.clear();

        let gauge = Gauge::with_opts(opts)?;
        Ok(MaxGauge::wrap(gauge))
    }
}

/// A `MetricVec` for `MaxGauge` values, partitioned by label values.
#[derive(Clone, Debug)]
pub struct MaxGaugeVec {
    inner: MetricVec<MaxGaugeVecBuilder>,
}

impl MaxGaugeVec {
    /// Create a new `MaxGaugeVec` with the given label names.
    pub fn new(opts: Opts, label_names: &[&str]) -> Result<Self> {
        let variable_names = label_names.iter().map(|s| (*s).to_owned()).collect();
        let opts = opts.variable_labels(variable_names);
        let inner = MetricVec::create(proto::MetricType::GAUGE, MaxGaugeVecBuilder::new(), opts)?;

        Ok(Self { inner })
    }
}

impl std::ops::Deref for MaxGaugeVec {
    type Target = MetricVec<MaxGaugeVecBuilder>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Collector for MaxGaugeVec {
    fn desc(&self) -> Vec<&Desc> {
        self.inner.desc()
    }

    fn collect(&self) -> Vec<proto::MetricFamily> {
        self.inner.collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_max_gauge_resets_on_metric() {
        let gauge = Gauge::with_opts(Opts::new("test_max_gauge", "test max gauge")).unwrap();
        let max = MaxGauge::wrap(gauge);

        max.observe(1.0);
        max.observe(3.0);
        max.observe(2.0);
        assert_eq!(max.get(), 3.0);

        let _ = max.metric();
        assert_eq!(max.get(), 0.0);
        assert_eq!(max.gauge.get(), 3.0);
    }

    #[test]
    fn test_max_gauge_vec_resets_on_collect() {
        let vec = MaxGaugeVec::new(
            Opts::new("test_max_gauge_vec", "test max gauge vec"),
            &["l1"],
        )
        .unwrap();
        let m = vec.with_label_values(&["v1"]);
        m.observe(5.0);

        let mfs = vec.collect();
        assert_eq!(mfs.len(), 1);
        let mf = &mfs[0];
        let metric = mf.get_metric().get(0).unwrap();
        assert_eq!(metric.get_label().len(), 1);
        assert_eq!(metric.get_label()[0].get_name(), "l1");
        assert_eq!(metric.get_label()[0].get_value(), "v1");
        assert_eq!(metric.get_gauge().get_value(), 5.0);

        assert_eq!(m.get(), 0.0);
    }
}
