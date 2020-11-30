// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::*;
use std::future::*;
use std::pin::Pin;
use std::task::*;

const TASKS_PER_CPU: usize = 50;

struct Backoff(usize);

impl Future for Backoff {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 == 0 {
            Poll::Ready(())
        } else {
            self.0 -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

mod yatp_callback {
    use criterion::*;
    use std::sync::mpsc;
    use yatp::queue::*;
    use yatp::task::callback::{Task, TaskCell};

    pub fn yield_many(b: &mut Bencher<'_>, yield_count: usize) {
        let tasks = super::TASKS_PER_CPU * num_cpus::get();
        let (tx, rx) = mpsc::sync_channel(tasks);
        let pool = yatp::Builder::new("yield_many").build_callback_pool();

        b.iter(move || {
            for _ in 0..tasks {
                let tx = tx.clone();
                let mut num_yield = yield_count;

                pool.spawn(TaskCell {
                    task: Task::new_mut(move |h: &mut yatp::task::callback::Handle<'_>| {
                        if num_yield == 0 {
                            tx.send(()).unwrap();
                        } else {
                            num_yield -= 1;
                            h.set_rerun(true);
                        }
                    }),
                    extras: Extras::single_level(),
                });
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

mod yatp_future {
    use criterion::*;
    use std::sync::mpsc;
    use yatp::task::future::TaskCell;

    fn yield_many(b: &mut Bencher<'_>, pool: yatp::ThreadPool<TaskCell>, yield_count: usize) {
        let tasks = super::TASKS_PER_CPU * num_cpus::get();
        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let tx = tx.clone();

                pool.spawn(async move {
                    let backoff = super::Backoff(yield_count);
                    backoff.await;
                    tx.send(()).unwrap();
                });
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }

    pub fn yield_many_single_level(b: &mut Bencher<'_>, yield_count: usize) {
        let pool = yatp::Builder::new("yield_many").build_future_pool();
        yield_many(b, pool, yield_count)
    }

    pub fn yield_many_multilevel(b: &mut Bencher<'_>, yield_count: usize) {
        let pool = yatp::Builder::new("yield_many").build_multilevel_future_pool();
        yield_many(b, pool, yield_count)
    }
}

mod tokio {
    use criterion::*;
    use std::sync::mpsc;
    use tokio::runtime::*;

    pub fn yield_many(b: &mut Bencher<'_>, yield_count: usize) {
        let tasks = super::TASKS_PER_CPU * num_cpus::get();
        let (tx, rx) = mpsc::sync_channel(tasks);
        let pool = Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .build()
            .unwrap();

        b.iter(move || {
            for _ in 0..tasks {
                let tx = tx.clone();

                pool.spawn(async move {
                    let backoff = super::Backoff(yield_count);
                    backoff.await;
                    tx.send(()).unwrap();
                });
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

mod async_std {
    use criterion::*;
    use std::sync::mpsc;

    pub fn yield_many(b: &mut Bencher<'_>, yield_count: usize) {
        let tasks = super::TASKS_PER_CPU * num_cpus::get();
        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let tx = tx.clone();

                async_std::task::spawn(async move {
                    let backoff = super::Backoff(yield_count);
                    backoff.await;
                    tx.send(()).unwrap();
                });
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

pub fn yield_many(b: &mut Criterion) {
    let mut group = b.benchmark_group("yield_many");
    for i in &[256, 512, 1024] {
        group.bench_with_input(BenchmarkId::new("yatp::future", i), i, |b, i| {
            yatp_future::yield_many_single_level(b, *i)
        });
        group.bench_with_input(BenchmarkId::new("yatp::callback", i), i, |b, i| {
            yatp_callback::yield_many(b, *i)
        });
        group.bench_with_input(
            BenchmarkId::new("yatp::future::multilevel", i),
            i,
            |b, i| yatp_future::yield_many_multilevel(b, *i),
        );
        group.bench_with_input(BenchmarkId::new("tokio", i), i, |b, i| {
            tokio::yield_many(b, *i)
        });
        group.bench_with_input(BenchmarkId::new("async-std", i), i, |b, i| {
            async_std::yield_many(b, *i)
        });
    }
    group.finish();
}

criterion_group!(yield_many_group, yield_many);

criterion_main!(yield_many_group);
