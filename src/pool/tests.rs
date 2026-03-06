// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::*;
use crate::queue::QueueType;
use crate::task::callback;
use crate::task::callback::Handle;
use futures_timer::Delay;
use rand::seq::SliceRandom;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
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

    // Make sure all workers have run and gone to sleep before spawn new tasks
    thread::sleep(Duration::from_secs(1));

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
    // Due to the current implementation, the scale up can't be triggered until a new
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

    // Make sure all workers have run and gone to sleep before spawn new tasks
    thread::sleep(Duration::from_secs(1));

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
    // Due to the current implementation, the scale down can't be triggered until thread
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

// NOTE: This test relies on `parking_lot_core::unpark_filter` visiting waiters in FIFO order.
// This is currently true but is an implementation detail of parking_lot_core. If the queuing
// discipline changes, the assertions on unpark order (via `unparked_ids`) may need adjustment.
#[test]
fn test_ensure_workers_unparks_only_core_threads_across_two_calls() {
    // Setup:
    //   max_thread_count = 10 (enough to cover all thread IDs)
    //   core_thread_count = 5 (so IDs <= 5 are eligible: 3 and 2 qualify; 6 and 9 don't)
    //   active_workers initialized to max_thread_count (10), we'll manually adjust to 4
    let (remote, locals) = build_spawn(
        QueueType::SingleLevel,
        SchedConfig {
            max_thread_count: 10,
            min_thread_count: 1,
            ..SchedConfig::default()
        },
    );
    remote.spawn(move |_: &mut callback::Handle<'_>| {});

    let core = remote.core.clone();

    // Set core_thread_count to 5: IDs <= 5 are "core" threads.
    core.scale_workers(5);

    // We need to park threads in order: 3, 6, 2, 9.
    // The `pop_or_sleep` uses thread's `id` as ParkToken.
    // Locals are created with ids: 1..=10. We pick locals at indices 2,5,1,8 (id=3,6,2,9).
    // We must park them in exactly this order.

    // Track which threads have been unparked.
    let unparked_ids = Arc::new(Mutex::new(Vec::new()));

    // We need to park threads sequentially to control FIFO order.
    // Use barriers to ensure ordering: park 3, then 6, then 2, then 9.
    let park_order: Vec<usize> = vec![3, 6, 2, 9];

    // Drain the locals we need by id. locals are indexed 0..9 with id = index+1.
    // Collect them into a map for convenience.
    let mut locals_by_id: std::collections::HashMap<usize, Local<_>> =
        locals.into_iter().map(|l| (l.id(), l)).collect();

    let mut handles = Vec::new();
    let parked_count = Arc::new(AtomicUsize::new(0));

    for &tid in &park_order {
        let mut local = locals_by_id.remove(&tid).unwrap();
        let unparked_ids = unparked_ids.clone();
        let parked_count_clone = parked_count.clone();

        // Compute expected count before spawning to avoid a race where the
        // spawned thread increments parked_count before we read it.
        let expected = parked_count.load(Ordering::SeqCst) + 1;
        let handle = thread::spawn(move || {
            // Signal that we're about to park.
            parked_count_clone.fetch_add(1, Ordering::SeqCst);
            let _ = local.pop_or_sleep();
            // Record which thread was unparked.
            unparked_ids
                .lock()
                .expect("Failed to lock unparked_ids")
                .push(tid);
        });

        // Wait until this thread is actually parked before parking the next one,
        // to ensure FIFO order in the parking queue.
        while parked_count.load(Ordering::SeqCst) < expected {
            thread::yield_now();
        }
        // Give a little extra time for the thread to enter the park call.
        thread::sleep(Duration::from_millis(50));

        handles.push(handle);
    }

    // Now 4 threads are parked. Each called mark_sleep(), so active_workers decreased by 4.
    // Initial active_workers = 10 << 1 = 20. After 4 mark_sleep calls: 20 - 4*2 = 12 => count = 6.
    // We need active count = 4, so manually adjust.
    // Actually, mark_sleep already decremented: 10 - 4 = 6 active workers.
    // We need active = 4, so decrement 2 more times.
    core.mark_sleep();
    core.mark_sleep();
    // Now active_workers >> 1 = 4, which is < core_thread_count (5), so ensure_workers proceeds.

    // First call: should unpark thread 3 (first in FIFO with id <= 5).
    core.ensure_workers(100);
    thread::sleep(Duration::from_millis(100));

    {
        let ids = unparked_ids.lock().expect("Failed to lock unparked_ids");
        assert_eq!(
            ids.len(),
            1,
            "First ensure_workers should unpark exactly one thread"
        );
        assert_eq!(
            ids[0], 3,
            "First unparked thread should be id=3 (first core thread in FIFO)"
        );
    }

    // After thread 3 wakes, it calls mark_woken(), so active count goes back up by 1 to 5.
    // We need active < core_thread_count (5) again for ensure_workers to proceed.
    // mark_sleep to drop active count.
    core.mark_sleep();
    // Now active = 4 again.

    // Second call: should skip thread 6 (id=6 > 5), then unpark thread 2 (id=2 <= 5).
    core.ensure_workers(200);
    thread::sleep(Duration::from_millis(100));

    {
        let ids = unparked_ids.lock().expect("Failed to lock unparked_ids");
        assert_eq!(
            ids.len(),
            2,
            "Second ensure_workers should unpark one more thread"
        );
        assert_eq!(
            ids[1], 2,
            "Second unparked thread should be id=2 (next core thread in FIFO)"
        );
    }

    // Verify threads 6 and 9 are still parked (not unparked).
    assert_eq!(
        unparked_ids
            .lock()
            .expect("Failed to lock unparked_ids")
            .len(),
        2,
        "Only 2 threads should have been unparked total"
    );

    // Clean up: shutdown to unpark remaining threads (6 and 9).
    core.mark_shutdown(0);
    for h in handles {
        h.join().unwrap();
    }

    // Final verification: all 4 threads eventually woke up after shutdown.
    let final_ids = unparked_ids.lock().expect("Failed to lock unparked_ids");
    assert_eq!(final_ids.len(), 4);
    assert_eq!(
        &final_ids[..2],
        &[3, 2],
        "Core threads 3 and 2 were unparked by ensure_workers"
    );
}
