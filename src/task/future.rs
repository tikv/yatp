// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A [`Future`].

use crate::{LocalSpawn, RemoteSpawn};

use std::cell::UnsafeCell;
use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering::SeqCst};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::{fmt, mem};

/// A [`Future`] task.
pub struct Task<Remote, Extras> {
    status: AtomicU8,
    future: UnsafeCell<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    remote: Remote,
    extras: UnsafeCell<Extras>,
}

/// A [`Future`] task cell.
pub struct TaskCell<Remote, Extras>(Arc<Task<Remote, Extras>>);

// Safety: It is ensured that `future` and `extras` are always accessed by
// only one thread at the same time.
unsafe impl<Remote: Sync, Extras> Sync for Task<Remote, Extras> {}

impl<Remote, Extras> fmt::Debug for TaskCell<Remote, Extras> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "future::TaskCell".fmt(f)
    }
}

// When a future task is created or waken up by a waker, it is marked as
// NOTIFIED. NOTIFIED tasks are ready to be polled. When the runner begins to
// poll the future, it is marked as POLLING. When the runner finishes polling,
// the future can either be ready or pending. If the future is ready, it is
// marked as COMPLETED, or it checks whether it has becomes NOTIFIED. If it is
// NOTIFIED, it should be polled again immediately. Otherwise it is marked as
// IDLE.
const NOTIFIED: u8 = 1;
const IDLE: u8 = 2;
const POLLING: u8 = 3;
const COMPLETED: u8 = 4;

impl<Remote, Extras> TaskCell<Remote, Extras> {
    /// Creates a [`Future`] task cell that is ready to be polled.
    pub fn new<F: Future<Output = ()> + Send + 'static>(
        future: F,
        remote: Remote,
        extras: Extras,
    ) -> Self {
        TaskCell(Arc::new(Task {
            status: AtomicU8::new(NOTIFIED),
            future: UnsafeCell::new(Box::pin(future)),
            remote,
            extras: UnsafeCell::new(extras),
        }))
    }
}

impl<Remote, Extras> crate::queue::TaskCell for TaskCell<Remote, Extras> {
    type Extras = Extras;

    fn mut_extras(&mut self) -> &mut Self::Extras {
        unsafe { &mut *self.0.extras.get() }
    }
}

#[inline]
unsafe fn waker<Remote, Extras>(task: *const Task<Remote, Extras>) -> Waker
where
    Remote: RemoteSpawn<TaskCell = TaskCell<Remote, Extras>>,
{
    Waker::from_raw(RawWaker::new(
        task as *const (),
        &RawWakerVTable::new(
            clone_raw::<Remote, Extras>,
            wake_raw::<Remote, Extras>,
            wake_ref_raw::<Remote, Extras>,
            drop_raw::<Remote, Extras>,
        ),
    ))
}

#[inline]
unsafe fn clone_raw<Remote, Extras>(this: *const ()) -> RawWaker
where
    Remote: RemoteSpawn<TaskCell = TaskCell<Remote, Extras>>,
{
    let task_cell = clone_task(this as *const Task<Remote, Extras>);
    RawWaker::new(
        Arc::into_raw(task_cell.0) as *const (),
        &RawWakerVTable::new(
            clone_raw::<Remote, Extras>,
            wake_raw::<Remote, Extras>,
            wake_ref_raw::<Remote, Extras>,
            drop_raw::<Remote, Extras>,
        ),
    )
}

#[inline]
unsafe fn drop_raw<Remote, Extras>(this: *const ())
where
    Remote: RemoteSpawn<TaskCell = TaskCell<Remote, Extras>>,
{
    drop(task_cell(this as *const Task<Remote, Extras>))
}

unsafe fn wake_impl<Remote, Extras>(task_cell: &TaskCell<Remote, Extras>)
where
    Remote: RemoteSpawn<TaskCell = TaskCell<Remote, Extras>>,
{
    let task = &task_cell.0;
    let mut status = task.status.load(SeqCst);
    loop {
        match status {
            IDLE => {
                match task
                    .status
                    .compare_exchange_weak(IDLE, NOTIFIED, SeqCst, SeqCst)
                {
                    Ok(_) => {
                        task.remote.spawn(clone_task(&**task));
                        break;
                    }
                    Err(cur) => status = cur,
                }
            }
            POLLING => {
                match task
                    .status
                    .compare_exchange_weak(POLLING, NOTIFIED, SeqCst, SeqCst)
                {
                    Ok(_) => break,
                    Err(cur) => status = cur,
                }
            }
            _ => break,
        }
    }
}

#[inline]
unsafe fn wake_raw<Remote, Extras>(this: *const ())
where
    Remote: RemoteSpawn<TaskCell = TaskCell<Remote, Extras>>,
{
    let task_cell = task_cell(this as *const Task<Remote, Extras>);
    wake_impl(&task_cell);
}

#[inline]
unsafe fn wake_ref_raw<Remote, Extras>(this: *const ())
where
    Remote: RemoteSpawn<TaskCell = TaskCell<Remote, Extras>>,
{
    let task_cell = ManuallyDrop::new(task_cell(this as *const Task<Remote, Extras>));
    wake_impl(&task_cell);
}

#[inline]
unsafe fn task_cell<Remote, Extras>(task: *const Task<Remote, Extras>) -> TaskCell<Remote, Extras>
where
    Remote: RemoteSpawn<TaskCell = TaskCell<Remote, Extras>>,
{
    TaskCell(Arc::from_raw(task))
}

#[inline]
unsafe fn clone_task<Remote, Extras>(task: *const Task<Remote, Extras>) -> TaskCell<Remote, Extras>
where
    Remote: RemoteSpawn<TaskCell = TaskCell<Remote, Extras>>,
{
    let task_cell = task_cell(task);
    mem::forget(task_cell.0.clone());
    task_cell
}

/// [`Future`] task runner.
pub struct Runner<Spawn> {
    _phantom: PhantomData<Spawn>,
}

impl<Spawn> Default for Runner<Spawn> {
    fn default() -> Runner<Spawn> {
        Runner {
            _phantom: PhantomData,
        }
    }
}

impl<L, R, Extras> crate::Runner for Runner<L>
where
    L: LocalSpawn<TaskCell = TaskCell<R, Extras>, Remote = R>,
    R: RemoteSpawn<TaskCell = TaskCell<R, Extras>>,
{
    type Spawn = L;

    fn handle(&mut self, _local: &mut L, task_cell: TaskCell<R, Extras>) -> bool {
        let task = task_cell.0;
        unsafe {
            let waker = ManuallyDrop::new(waker(&*task));
            let mut cx = Context::from_waker(&waker);
            loop {
                task.status.store(POLLING, SeqCst);
                if let Poll::Ready(_) = Pin::new(&mut *task.future.get()).poll(&mut cx) {
                    task.status.store(COMPLETED, SeqCst);
                    return true;
                }
                match task.status.compare_exchange(POLLING, IDLE, SeqCst, SeqCst) {
                    Ok(_) => return false,
                    Err(cur) if cur == NOTIFIED => continue,
                    _ => unreachable!(),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RemoteSpawn, Runner as _};

    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::mpsc;

    struct MockLocal {
        runner: Rc<RefCell<Runner<MockLocal>>>,
        task_rx: mpsc::Receiver<TaskCell<MockRemote, ()>>,
        remote: MockRemote,
    }

    impl MockLocal {
        fn new() -> MockLocal {
            let (task_tx, task_rx) = mpsc::sync_channel(10);
            MockLocal {
                runner: Rc::new(RefCell::new(Runner::default())),
                task_rx,
                remote: MockRemote { task_tx },
            }
        }

        /// Run `Runner::handle` once.
        fn handle_once(&mut self) {
            if let Ok(task_cell) = self.task_rx.try_recv() {
                let runner = self.runner.clone();
                runner.borrow_mut().handle(self, task_cell);
            }
        }
    }

    #[derive(Clone)]
    struct MockRemote {
        task_tx: mpsc::SyncSender<TaskCell<MockRemote, ()>>,
    }

    impl LocalSpawn for MockLocal {
        type TaskCell = TaskCell<MockRemote, ()>;
        type Remote = MockRemote;

        fn spawn(&mut self, task_cell: TaskCell<MockRemote, ()>) {
            self.remote.task_tx.send(task_cell).unwrap();
        }

        fn remote(&self) -> Self::Remote {
            self.remote.clone()
        }
    }

    impl RemoteSpawn for MockRemote {
        type TaskCell = TaskCell<MockRemote, ()>;

        fn spawn(&self, task_cell: TaskCell<MockRemote, ()>) {
            self.task_tx.send(task_cell).unwrap();
        }
    }

    struct WakeLater {
        waker_tx: mpsc::SyncSender<Waker>,
        first_poll: bool,
    }

    impl WakeLater {
        fn new(waker_tx: mpsc::SyncSender<Waker>) -> WakeLater {
            WakeLater {
                waker_tx,
                first_poll: true,
            }
        }
    }

    impl Future for WakeLater {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
            if self.first_poll {
                self.first_poll = false;
                self.waker_tx
                    .send(cx.waker().clone())
                    .expect("waker channel disconnected");
                Poll::Pending
            } else {
                Poll::Ready(())
            }
        }
    }

    fn test_wake_impl(f: impl FnOnce(Waker)) {
        let mut local = MockLocal::new();
        let (res_tx, res_rx) = mpsc::channel();
        let (waker_tx, waker_rx) = mpsc::sync_channel(10);

        let fut = async move {
            res_tx.send(1).unwrap();
            WakeLater::new(waker_tx.clone()).await;
            res_tx.send(2).unwrap();
        };
        local.spawn(TaskCell::new(fut, local.remote(), ()));

        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 1);
        assert!(res_rx.try_recv().is_err());

        let waker = waker_rx.recv().unwrap();
        f(waker);
        assert!(res_rx.try_recv().is_err());
        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 2);
    }

    #[test]
    fn test_wake() {
        test_wake_impl(|waker| waker.wake());
    }

    #[test]
    fn test_wake_by_ref() {
        test_wake_impl(|waker| waker.wake_by_ref());
    }

    #[test]
    fn test_waker_clone() {
        test_wake_impl(|waker| waker.clone().wake());
    }

    #[test]
    fn test_wake_by_self() {
        struct PendingOnce {
            first_poll: bool,
        }

        impl PendingOnce {
            fn new() -> PendingOnce {
                PendingOnce { first_poll: true }
            }
        }

        impl Future for PendingOnce {
            type Output = ();
            fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
                if self.first_poll {
                    self.first_poll = false;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            }
        }

        let mut local = MockLocal::new();
        let (res_tx, res_rx) = mpsc::channel();

        let fut = async move {
            res_tx.send(1).unwrap();
            PendingOnce::new().await;
            res_tx.send(2).unwrap();
        };
        local.spawn(TaskCell::new(fut, local.remote(), ()));

        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 1);
        assert_eq!(res_rx.recv().unwrap(), 2);
    }
}
