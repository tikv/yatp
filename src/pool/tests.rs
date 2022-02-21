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

    // One future will be notified after 300ms.
    let tx2 = tx.clone();
    remote.spawn(async move {
        Delay::new(Duration::from_millis(300)).await;
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
    let tx = tx;
    remote.spawn(async move {
        drop(tx);
    });

    // Before the pool is shut down, txs are not all dropped.
    assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Empty));

    drop(remote);
    drop(pool);

    // All txs should be dropped. No future leaks.
    assert_eq!(
        rx.recv_timeout(Duration::from_millis(500)),
        Err(mpsc::RecvTimeoutError::Disconnected)
    );
}

#[test]
fn test_scale_up_workers() {
    let pool = Builder::new("test_scale_up")
        .max_thread_count(4)
        .core_thread_count(2)
        .build_callback_pool();

    let mut sync_txs = vec![];
    // Block all runnable (`core_thread_count`) threads
    for _ in 0..2 {
        let (tx, rx) = mpsc::sync_channel::<()>(0);
        pool.spawn(move |_: &mut Handle<'_>| rx.recv().unwrap());
        sync_txs.push(tx);
    }

    // Make sure all block tasks have been dispatched to `core_thread_count` threads,
    // as the above block tasks may be pulled to one thread at once, then stealed
    // by the other thread, which cause the below task is executed first.
    thread::sleep(Duration::from_secs(1));

    let (tx, rx) = mpsc::channel();
    let tx1 = tx.clone();
    pool.spawn(move |_: &mut Handle<'_>| tx1.send(1).unwrap());
    // No runnable threads, so it should time out
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    // Scale up one worker
    pool.scale_workers(3);
    // Due to the current implementation, the scale up cann't be triggered until a new
    // task has been spawned, so it should still time out
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );
    // Spawn a new task to trigger scale up
    pool.spawn(move |_: &mut Handle<'_>| tx.send(2).unwrap());
    // Spawn should success, and handle the two tasks by the spawned order
    assert_eq!(Ok(1), rx.recv_timeout(Duration::from_secs(1)));
    assert_eq!(Ok(2), rx.recv_timeout(Duration::from_secs(1)));

    // Block the new scaled thread, to prove only scaled one thread
    let (tx, rx) = mpsc::sync_channel::<()>(0);
    pool.spawn(move |_: &mut Handle<'_>| rx.recv().unwrap());
    sync_txs.push(tx);

    let (tx, rx) = mpsc::channel();
    pool.spawn(move |_: &mut Handle<'_>| tx.send(1).unwrap());
    // The scaled thread has been blocked, no more runnable threads, so it should time out
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );

    // Wake up the threads that were previously blocked by the task
    for tx in sync_txs {
        tx.send(()).unwrap();
    }

    pool.shutdown();
}

#[test]
fn test_scale_down_workers() {
    let pool = Builder::new("test_scale_down")
        .max_thread_count(4)
        .core_thread_count(3)
        .build_callback_pool();

    let mut sync_txs = vec![];
    // Block all runnable (`core_thread_count`) threads
    for _ in 0..3 {
        let (tx, rx) = mpsc::sync_channel::<()>(0);
        pool.spawn(move |_: &mut Handle<'_>| rx.recv().unwrap());
        sync_txs.push(tx);
    }

    // Make sure all block tasks have been dispatched to `core_thread_count` threads,
    // as the above block tasks may be pulled by one thread at once, then stealed
    // by other threads, which will cause the below task is executed first.
    thread::sleep(Duration::from_secs(1));

    let (tx, rx) = mpsc::channel();
    pool.spawn(move |_: &mut Handle<'_>| tx.send(1).unwrap());
    // No runnable threads, so it should time out
    assert_eq!(
        rx.recv_timeout(Duration::from_secs(1)),
        Err(mpsc::RecvTimeoutError::Timeout)
    );

    // Scale down two workers
    pool.scale_workers(1);
    // Due to the current implementation, the scale down cann't be triggered until thread
    // finished tasks and go to sleep, so wake up them all to finish tasks first.
    for tx in sync_txs {
        tx.send(()).unwrap();
    }

    // Make sure all threads have gone to sleep before spawn a lot of new tasks
    thread::sleep(Duration::from_secs(1));

    let (tx, rx) = mpsc::channel();
    // A bunch of tasks should be executed correctly, and as there's only one worker, the
    // handled order should be the same as spawn order.
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
    assert_eq!(cases, ans);

    pool.shutdown();
}
