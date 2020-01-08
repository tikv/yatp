// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::*;

mod yatp_callback {
    use criterion::*;
    use std::sync::mpsc;
    use yatp::task::callback::Handle;

    pub fn chained_spawn(b: &mut Bencher<'_>, iter_count: usize) {
        let pool = yatp::Builder::new("chained_spawn").build_callback_pool();

        fn iter(handle: &mut Handle<'_>, done_tx: mpsc::SyncSender<()>, n: usize) {
            if n == 0 {
                done_tx.send(()).unwrap();
            } else {
                handle.spawn(move |h: &mut Handle<'_>| {
                    iter(h, done_tx, n - 1);
                })
            }
        }

        let (done_tx, done_rx) = mpsc::sync_channel(1000);

        b.iter(move || {
            let done_tx = done_tx.clone();
            pool.spawn(move |h: &mut Handle<'_>| {
                iter(h, done_tx, iter_count);
            });

            done_rx.recv().unwrap();
        });
    }
}

mod yatp_future {
    use criterion::*;
    use std::sync::mpsc;
    use yatp::task::future::TaskCell;
    use yatp::Handle;

    pub fn chained_spawn(b: &mut Bencher<'_>, iter_count: usize) {
        let pool = yatp::Builder::new("chained_spawn").build_future_pool();

        fn iter(handle: Handle<TaskCell>, done_tx: mpsc::SyncSender<()>, n: usize) {
            if n == 0 {
                done_tx.send(()).unwrap();
            } else {
                let s2 = handle.clone();
                handle.spawn(async move {
                    iter(s2, done_tx, n - 1);
                });
            }
        }

        let (done_tx, done_rx) = mpsc::sync_channel(1000);

        b.iter(move || {
            let done_tx = done_tx.clone();
            let handle = pool.handle().clone();
            pool.spawn(async move {
                iter(handle, done_tx, iter_count);
            });

            done_rx.recv().unwrap();
        });
    }
}

mod tokio {
    use criterion::*;
    use std::sync::mpsc;
    use tokio::runtime::*;

    pub fn chained_spawn(b: &mut Bencher<'_>, iter_count: usize) {
        let pool = Builder::new()
            .threaded_scheduler()
            .core_threads(num_cpus::get())
            .build()
            .unwrap();

        fn iter(handle: Handle, done_tx: mpsc::SyncSender<()>, n: usize) {
            if n == 0 {
                done_tx.send(()).unwrap();
            } else {
                let s2 = handle.clone();
                handle.spawn(async move {
                    iter(s2, done_tx, n - 1);
                });
            }
        }

        let (done_tx, done_rx) = mpsc::sync_channel(1000);

        b.iter(move || {
            let done_tx = done_tx.clone();
            let handle = pool.handle().clone();
            pool.spawn(async move {
                iter(handle, done_tx, iter_count);
            });

            done_rx.recv().unwrap();
        });
    }
}

mod async_std {
    use criterion::*;
    use std::sync::mpsc;

    pub fn chained_spawn(b: &mut Bencher, iter_count: usize) {
        fn iter(done_tx: mpsc::SyncSender<()>, n: usize) {
            if n == 0 {
                done_tx.send(()).unwrap();
            } else {
                async_std::task::spawn(async move {
                    iter(done_tx, n - 1);
                });
            }
        }

        let (done_tx, done_rx) = mpsc::sync_channel(1000);

        b.iter(move || {
            let done_tx = done_tx.clone();
            async_std::task::spawn(async move {
                iter(done_tx, iter_count);
            });

            done_rx.recv().unwrap();
        });
    }
}

pub fn chained_spawn(b: &mut Criterion) {
    let mut group = b.benchmark_group("chained_spawn");
    for i in &[100, 400, 700, 1000] {
        group.bench_with_input(BenchmarkId::new("yatp::future", i), i, |b, i| {
            yatp_future::chained_spawn(b, *i)
        });
        group.bench_with_input(BenchmarkId::new("yatp::callback", i), i, |b, i| {
            yatp_callback::chained_spawn(b, *i)
        });
        group.bench_with_input(BenchmarkId::new("tokio", i), i, |b, i| {
            tokio::chained_spawn(b, *i)
        });
        group.bench_with_input(BenchmarkId::new("async-std", i), i, |b, i| {
            async_std::chained_spawn(b, *i)
        });
    }
    group.finish();
}

criterion_group!(chained_spawn_group, chained_spawn);

criterion_main!(chained_spawn_group);
