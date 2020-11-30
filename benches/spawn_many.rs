// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use criterion::*;

mod yatp_callback {
    use criterion::*;
    use std::sync::atomic::*;
    use std::sync::*;

    pub fn spawn_many(b: &mut Bencher<'_>, spawn_count: usize) {
        let (tx, rx) = mpsc::sync_channel(1000);
        let rem = Arc::new(AtomicUsize::new(0));
        let pool = yatp::Builder::new("spawn_many").build_callback_pool();

        b.iter(|| {
            rem.store(spawn_count, Ordering::Relaxed);

            for _ in 0..spawn_count {
                let tx = tx.clone();
                let rem = rem.clone();

                pool.spawn(move |_: &mut yatp::task::callback::Handle<'_>| {
                    if 1 == rem.fetch_sub(1, Ordering::Relaxed) {
                        tx.send(()).unwrap();
                    }
                });
            }

            let _ = rx.recv().unwrap();
        });
    }
}

mod yatp_future {
    use criterion::*;
    use std::sync::atomic::*;
    use std::sync::*;
    use yatp::task::future::TaskCell;

    fn spawn_many(b: &mut Bencher<'_>, pool: yatp::ThreadPool<TaskCell>, spawn_count: usize) {
        let (tx, rx) = mpsc::sync_channel(1000);
        let rem = Arc::new(AtomicUsize::new(0));

        b.iter(|| {
            rem.store(spawn_count, Ordering::Relaxed);

            for _ in 0..spawn_count {
                let tx = tx.clone();
                let rem = rem.clone();

                pool.spawn(async move {
                    if 1 == rem.fetch_sub(1, Ordering::Relaxed) {
                        tx.send(()).unwrap();
                    }
                });
            }

            let _ = rx.recv().unwrap();
        });
    }

    pub fn spawn_many_single_level(b: &mut Bencher<'_>, spawn_count: usize) {
        let pool = yatp::Builder::new("spawn_many").build_future_pool();
        spawn_many(b, pool, spawn_count)
    }

    pub fn spawn_many_multilevel(b: &mut Bencher<'_>, spawn_count: usize) {
        let pool = yatp::Builder::new("spawn_many").build_multilevel_future_pool();
        spawn_many(b, pool, spawn_count)
    }
}

mod threadpool {
    use criterion::*;
    use std::sync::atomic::*;
    use std::sync::*;

    pub fn spawn_many(b: &mut Bencher<'_>, spawn_count: usize) {
        let (tx, rx) = mpsc::sync_channel(1000);
        let rem = Arc::new(AtomicUsize::new(0));
        let pool = threadpool::ThreadPool::new(num_cpus::get());

        b.iter(|| {
            rem.store(spawn_count, Ordering::Relaxed);

            for _ in 0..spawn_count {
                let tx = tx.clone();
                let rem = rem.clone();

                pool.execute(move || {
                    if 1 == rem.fetch_sub(1, Ordering::Relaxed) {
                        tx.send(()).unwrap();
                    }
                });
            }

            let _ = rx.recv().unwrap();
        });
    }
}

mod tokio {
    use criterion::*;
    use std::sync::atomic::*;
    use std::sync::*;
    use tokio::runtime::Builder;

    pub fn spawn_many(b: &mut Bencher<'_>, spawn_count: usize) {
        let (tx, rx) = mpsc::sync_channel(1000);
        let rem = Arc::new(AtomicUsize::new(0));
        let pool = Builder::new_multi_thread()
            .worker_threads(num_cpus::get())
            .build()
            .unwrap();

        b.iter(|| {
            rem.store(spawn_count, Ordering::Relaxed);

            for _ in 0..spawn_count {
                let tx = tx.clone();
                let rem = rem.clone();

                pool.spawn(async move {
                    if 1 == rem.fetch_sub(1, Ordering::Relaxed) {
                        tx.send(()).unwrap();
                    }
                });
            }

            let _ = rx.recv().unwrap();
        });
    }
}

mod async_std {
    use criterion::*;
    use std::sync::atomic::*;
    use std::sync::*;

    pub fn spawn_many(b: &mut Bencher<'_>, spawn_count: usize) {
        let (tx, rx) = mpsc::sync_channel(1000);
        let rem = Arc::new(AtomicUsize::new(0));

        b.iter(|| {
            rem.store(spawn_count, Ordering::Relaxed);

            for _ in 0..spawn_count {
                let tx = tx.clone();
                let rem = rem.clone();

                async_std::task::spawn(async move {
                    if 1 == rem.fetch_sub(1, Ordering::Relaxed) {
                        tx.send(()).unwrap();
                    }
                });
            }

            let _ = rx.recv().unwrap();
        });
    }
}

pub fn spawn_many(b: &mut Criterion) {
    let mut group = b.benchmark_group("spawn_many");
    for i in &[1024, 4096, 8192, 16384] {
        group.bench_with_input(BenchmarkId::new("yatp::future", i), i, |b, i| {
            yatp_future::spawn_many_single_level(b, *i)
        });
        group.bench_with_input(BenchmarkId::new("yatp::callback", i), i, |b, i| {
            yatp_callback::spawn_many(b, *i)
        });
        group.bench_with_input(
            BenchmarkId::new("yatp::future::multilevel", i),
            i,
            |b, i| yatp_future::spawn_many_multilevel(b, *i),
        );
        group.bench_with_input(BenchmarkId::new("threadpool", i), i, |b, i| {
            threadpool::spawn_many(b, *i)
        });
        group.bench_with_input(BenchmarkId::new("tokio", i), i, |b, i| {
            tokio::spawn_many(b, *i)
        });
        group.bench_with_input(BenchmarkId::new("async-std", i), i, |b, i| {
            async_std::spawn_many(b, *i)
        });
    }
    group.finish();
}

criterion_group!(spawn_many_group, spawn_many);

criterion_main!(spawn_many_group);
