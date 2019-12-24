// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::pool::*;
use crate::queue;
use crate::task::callback::{Handle, Runner};
use rand::seq::SliceRandom;
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
        assert_eq!(value, rx.recv_timeout(Duration::from_millis(1000)).unwrap());
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
        let r = rx.recv_timeout(Duration::from_millis(1000)).unwrap();
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
    for _ in 0..4 {
        if rx.try_recv().is_err() {
            break;
        }
    }
    assert_eq!(
        Err(mpsc::RecvTimeoutError::Timeout),
        rx.recv_timeout(Duration::from_millis(250))
    );

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
    assert_eq!(Ok(1), rx.recv_timeout(Duration::from_millis(500)));

    // Shutdown should stop processing tasks.
    pool.shutdown();
    remote.spawn(move |_: &mut Handle<'_>| tx.send(2).unwrap());
    let res = rx.recv_timeout(Duration::from_millis(500));
    assert_eq!(res, Err(mpsc::RecvTimeoutError::Timeout));
}
