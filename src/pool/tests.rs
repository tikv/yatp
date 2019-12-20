// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::*;
use crate::queue;
use crate::task::callback::{Handle, Runner};
use std::sync::mpsc;
use std::thread;
use std::time::*;

#[test]
fn test_basic() {
    let r = Runner::new(3);
    let mut builder = Builder::new("test_basic");
    builder.max_thread_count(4);
    let pool = builder.build(queue::simple, CloneRunnerBuilder(r));
    let (tx, rx) = mpsc::channel();

    // Task should be executed immediately.
    let t = tx.clone();
    pool.spawn(move |_: &mut Handle<'_>| t.send(1).unwrap());
    assert_eq!(Ok(1), rx.recv_timeout(Duration::from_millis(10)));

    // Tasks should be executed concurrently.
    for id in 0..4 {
        let t = tx.clone();
        pool.spawn(move |_: &mut Handle<'_>| {
            thread::sleep(Duration::from_millis(100));
            t.send(id).unwrap();
        })
    }
    let timer = Instant::now();
    for _ in 0..4 {
        let r = rx.recv_timeout(Duration::from_millis(120)).unwrap();
        assert!(r >= 0 && r < 4, "unexpected result {}", r);
    }
    assert!(timer.elapsed() < Duration::from_millis(150));

    // A bunch of tasks should be executed correctly.
    for id in 10..1000 {
        let t = tx.clone();
        pool.spawn(move |_: &mut Handle<'_>| t.send(id).unwrap());
    }
    for _ in 10..1000 {
        let r = rx.recv_timeout(Duration::from_millis(10)).unwrap();
        assert!(r >= 10 && r < 1000);
    }

    // Shutdown should only wait for at most one tasks.
    for _ in 0..5 {
        let t = tx.clone();
        pool.spawn(move |_: &mut Handle<'_>| {
            thread::sleep(Duration::from_millis(40));
            t.send(0).unwrap();
        });
    }
    let timer = Instant::now();
    pool.shutdown();
    assert!(timer.elapsed() < Duration::from_millis(50));
    for _ in 0..5 {
        let _ = rx.try_recv();
    }

    // Shutdown should stop processing tasks.
    pool.spawn(move |_: &mut Handle<'_>| tx.send(2).unwrap());
    let res = rx.recv_timeout(Duration::from_millis(10));
    assert_eq!(res, Err(mpsc::RecvTimeoutError::Timeout));
}

#[test]
fn test_remote() {
    let r = Runner::new(3);
    let mut builder = Builder::new("test_remote");
    builder.max_thread_count(4);
    let pool = builder.build(queue::simple, CloneRunnerBuilder(r));

    // Remote should work just like pool.
    let remote = pool.remote();
    let (tx, rx) = mpsc::channel();
    let t = tx.clone();
    remote.spawn(move |_: &mut Handle<'_>| t.send(1).unwrap());
    assert_eq!(Ok(1), rx.recv_timeout(Duration::from_millis(10)));

    // Shutdown should stop processing tasks.
    pool.shutdown();
    remote.spawn(move |_: &mut Handle<'_>| tx.send(2).unwrap());
    let res = rx.recv_timeout(Duration::from_millis(10));
    assert_eq!(res, Err(mpsc::RecvTimeoutError::Timeout));
}
