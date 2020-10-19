// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::*;
use crate::task::callback::Handle;
use futures_timer::Delay;
use rand::seq::SliceRandom;
use std::sync::mpsc;
use std::thread;
use std::time::*;

#[test]
fn test_basic() {
    let pool = Builder::new("test_basic")
        .max_thread_count(4)
        .build_callback_pool();
    let (tx, rx) = mpsc::channel();

    // Task should be executed immediately.
    let t = tx.clone();
    pool.spawn(move |_: &mut Handle<'_>| t.send(1).unwrap());
    assert_eq!(Ok(1), rx.recv_timeout(Duration::from_secs(1)));

    // Tasks should be executed concurrently.
    let mut pairs = vec![];
    for _ in 0..4 {
        let (tx1, rx1) = mpsc::channel();
        let (tx2, rx2) = mpsc::channel();
        pool.spawn(move |_: &mut Handle<'_>| {
            let t = rx1.recv().unwrap();
            tx2.send(t).unwrap();
        });
        pairs.push((tx1, rx2));
    }
    pairs.shuffle(&mut rand::thread_rng());
    for (tx, rx) in pairs {
        let value: u64 = rand::random();
        tx.send(value).unwrap();
        assert_eq!(value, rx.recv_timeout(Duration::from_secs(1)).unwrap());
    }

    // A bunch of tasks should be executed correctly.
    let cases: Vec<_> = (10..1000).collect();
    for id in &cases {
        let t = tx.clone();
        let id = *id;
        pool.spawn(move |_: &mut Handle<'_>| t.send(id).unwrap());
    }
    let mut ans = vec![];
    for _ in 10..1000 {
        let r = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        ans.push(r);
    }
    ans.sort();
    assert_eq!(cases, ans);

    // Shutdown should only wait for at most one tasks.
    for _ in 0..5 {
        let t = tx.clone();
        pool.spawn(move |_: &mut Handle<'_>| {
            thread::sleep(Duration::from_millis(100));
            t.send(0).unwrap();
        });
    }
    pool.shutdown();
    // After dropping this tx, all tx should be destructed.
    drop(tx);
    for _ in 0..4 {
        if rx.try_recv().is_err() {
            break;
        }
    }
    // After the pool is shut down, tasks in the queue should be dropped.
    // So we should get a Disconnected error.
    assert_eq!(Err(mpsc::TryRecvError::Disconnected), rx.try_recv());
}

#[test]
fn test_remote() {
    let pool = Builder::new("test_remote")
        .max_thread_count(4)
        .build_callback_pool();

    // Remote should work just like pool.
    let remote = pool.remote();
    let (tx, rx) = mpsc::channel();
    let t = tx.clone();
    remote.spawn(move |_: &mut Handle<'_>| t.send(1).unwrap());
    assert_eq!(Ok(1), rx.recv_timeout(Duration::from_millis(500)));

    for _ in 0..5 {
        let t = tx.clone();
        remote.spawn(move |_: &mut Handle<'_>| {
            thread::sleep(Duration::from_millis(100));
            t.send(0).unwrap();
        });
    }
    drop(tx);
    // Shutdown should stop processing tasks.
    pool.shutdown();
    // Each thread should only wait for at most one tasks after shutdown.
    for _ in 0..4 {
        if rx.try_recv().is_err() {
            break;
        }
    }
    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Disconnected));
}

#[test]
fn test_shutdown_in_pool() {
    let pool = Builder::new("test_shutdown_in_pool")
        .max_thread_count(4)
        .build_callback_pool();
    let remote = pool.remote().clone();
    let (tx, rx) = mpsc::channel();
    remote.spawn(move |_: &mut Handle<'_>| {
        pool.shutdown();
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test]
fn test_shutdown_with_futures() {
    let pool = Builder::new("test_shutdown_with_futures")
        .max_thread_count(2)
        .build_future_pool();
    let remote = pool.remote().clone();
    let (tx, rx) = mpsc::channel::<()>();

    // One future will be notified at 300ms.
    let tx2 = tx.clone();
    remote.spawn(async move {
        Delay::new(Duration::from_millis(300));
        drop(tx2);
    });

    // Two futures running in the pool.
    thread::sleep(Duration::from_millis(50));
    let tx2 = tx.clone();
    remote.spawn(async move {
        thread::sleep(Duration::from_millis(100));
        drop(tx2);
    });
    let tx2 = tx.clone();
    remote.spawn(async move {
        thread::sleep(Duration::from_millis(100));
        drop(tx2);
    });

    // One future in the queue.
    thread::sleep(Duration::from_millis(50));
    remote.spawn(async move {
        drop(tx);
    });

    pool.shutdown();
    // All tx should be dropped. No future leaks.
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(500)),
        Err(mpsc::RecvTimeoutError::Disconnected)
    );
}
