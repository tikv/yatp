// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A [`Future`].

use crate::pool::{Local, Remote};
use crate::queue::{Extras, WithExtras};

use std::cell::{Cell, UnsafeCell};
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::atomic::{AtomicU8, Ordering::SeqCst};
use std::sync::Arc;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::{fmt, mem};

/// The default repoll limit for a future runner. See `Runner::new` for
/// details.
const DEFAULT_REPOLL_LIMIT: usize = 5;

struct TaskExtras {
    extras: Extras,
    remote: Option<Remote<TaskCell>>,
}

/// A [`Future`] task.
pub struct Task {
    status: AtomicU8,
    extras: UnsafeCell<TaskExtras>,
    future: UnsafeCell<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
}

/// A [`Future`] task cell.
pub struct TaskCell(Arc<Task>);

// Safety: It is ensured that `future` and `extras` are always accessed by
// only one thread at the same time.
unsafe impl Sync for Task {}

impl fmt::Debug for TaskCell {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "future::TaskCell".fmt(f)
    }
}

impl<F> WithExtras<TaskCell> for F
where F: Future<Output=()> + Send + 'static
{
    fn with_extras(self, extras: impl FnOnce() -> Extras) -> TaskCell {
        TaskCell::new(self, extras())
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

impl TaskCell {
    /// Creates a [`Future`] task cell that is ready to be polled.
    pub fn new<F: Future<Output = ()> + Send + 'static>(
        future: F,
        extras: Extras,
    ) -> Self {
        TaskCell(Arc::new(Task {
            status: AtomicU8::new(NOTIFIED),
            future: UnsafeCell::new(Box::pin(future)),
            extras: UnsafeCell::new(TaskExtras {
                extras,
                remote: None,
            }),
        }))
    }
}

impl crate::queue::TaskCell for TaskCell {
    fn mut_extras(&mut self) -> &mut Extras {
        unsafe { &mut (*self.0.extras.get()).extras }
    }
}

#[inline]
unsafe fn waker(task: *const Task) -> Waker {
    Waker::from_raw(RawWaker::new(
        task as *const (),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    ))
}

#[inline]
unsafe fn clone_raw(this: *const ()) -> RawWaker {
    let task_cell = clone_task(this as *const Task);
    RawWaker::new(
        Arc::into_raw(task_cell.0) as *const (),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    )
}

#[inline]
unsafe fn drop_raw(this: *const ()) {
    drop(task_cell(this as *const Task))
}

unsafe fn wake_impl(task_cell: &TaskCell) {
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
                        wake_task(task, false);
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
unsafe fn wake_raw(this: *const ()) {
    let task_cell = task_cell(this as *const Task);
    wake_impl(&task_cell);
}

#[inline]
unsafe fn wake_ref_raw(this: *const ()) {
    let task_cell = ManuallyDrop::new(task_cell(this as *const Task));
    wake_impl(&task_cell);
}

#[inline]
unsafe fn task_cell(task: *const Task) -> TaskCell {
    TaskCell(Arc::from_raw(task))
}

#[inline]
unsafe fn clone_task(task: *const Task) -> TaskCell {
    let task_cell = task_cell(task);
    let extras = {&mut *task_cell.0.extras.get()};
    if extras.remote.is_none() {
        LOCAL.with(|l| {
            extras.remote = Some((&*l.get()).remote());
        })
    }
    mem::forget(task_cell.0.clone());
    task_cell
}

thread_local! {
    static LOCAL: Cell<*mut Local<TaskCell>> = Cell::new(std::ptr::null_mut());
}

unsafe fn wake_task(task: &Arc<Task>, reschedule: bool) {
    LOCAL.with(|ptr| {
        if ptr.get().is_null() {
            (&mut *task.extras.get()).remote.as_ref().unwrap().spawn(clone_task(&**task));
        } else if reschedule {
            (&mut *ptr.get()).spawn_remote(clone_task(&**task));
        } else {
            (&mut *ptr.get()).spawn(clone_task(&**task));
        }
    })
}

struct Scope<'a>(&'a mut Local<TaskCell>);

impl<'a> Scope<'a> {
    fn new(l: &'a mut Local<TaskCell>) -> Scope<'a> {
        LOCAL.with(|c| c.set(l));
        Scope(l)
    }
}

impl<'a> Drop for Scope<'a> {
    fn drop(&mut self) {
        LOCAL.with(|c| c.set(std::ptr::null_mut()));
    }
}

/// [`Future`] task runner.
#[derive(Clone)]
pub struct Runner {
    repoll_limit: usize,
}

impl Default for Runner {
    fn default() -> Runner {
        Runner {
            repoll_limit: DEFAULT_REPOLL_LIMIT,
        }
    }
}

impl Runner {
    /// Creates a [`Future`] task runner.
    ///
    /// `repoll_limit` is the maximum times a [`Future`] is polled again
    /// immediately after polling because of being waken up during polling.
    pub fn new(repoll_limit: usize) -> Self {
        Self { repoll_limit }
    }
}

thread_local! {
    static NEED_RESCHEDULE: Cell<bool> = Cell::new(false);
}

impl crate::pool::Runner for Runner {
    type TaskCell = TaskCell;

    fn handle(&mut self, local: &mut Local<TaskCell>, task_cell: TaskCell) -> bool {
        let _scope = Scope::new(local);
        let task = task_cell.0;
        unsafe {
            let waker = ManuallyDrop::new(waker(&*task));
            let mut cx = Context::from_waker(&waker);
            let mut repoll_times = 0;
            loop {
                task.status.store(POLLING, SeqCst);
                if let Poll::Ready(_) = (&mut *task.future.get()).as_mut().poll(&mut cx) {
                    task.status.store(COMPLETED, SeqCst);
                    return true;
                }
                match task.status.compare_exchange(POLLING, IDLE, SeqCst, SeqCst) {
                    Ok(_) => return false,
                    Err(NOTIFIED) => {
                        let need_reschedule = NEED_RESCHEDULE.with(|r| r.replace(false));
                        if repoll_times >= self.repoll_limit
                            || need_reschedule
                        {
                            wake_task(&task, need_reschedule);
                            return false;
                        } else {
                            repoll_times += 1;
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
}

/// Gives up a time slice to the task scheduler.
///
/// It is only guaranteed to work in yatp.
pub async fn reschedule() {
    Reschedule { first_poll: true }.await
}

struct Reschedule {
    first_poll: bool,
}

impl Future for Reschedule {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if self.first_poll {
            self.first_poll = false;
            NEED_RESCHEDULE.with(|r| {
                r.set(true);
            });
            cx.waker().wake_by_ref();
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pool::{build_spawn, Runner as _};
    use crate::queue;

    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::mpsc;

    struct MockLocal {
        runner: Rc<RefCell<Runner>>,
        remote: Remote<TaskCell>,
        locals: Vec<Local<TaskCell>>,
    }

    impl MockLocal {
        fn new(runner: Runner) -> MockLocal {
            let (remote, locals) = build_spawn(queue::single_level, Default::default());
            MockLocal {
                runner: Rc::new(RefCell::new(runner)),
                remote,
                locals,
            }
        }

        /// Run `Runner::handle` once.
        fn handle_once(&mut self) {
            if let Some(t) = self.locals[0].pop() {
                let runner = self.runner.clone();
                runner.borrow_mut().handle(&mut self.locals[0], t.task_cell);
            }
        }
    }

    impl Default for MockLocal {
        fn default() -> Self {
            MockLocal::new(Default::default())
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
        let mut local = MockLocal::default();
        let (res_tx, res_rx) = mpsc::channel();
        let (waker_tx, waker_rx) = mpsc::sync_channel(10);

        let fut = async move {
            res_tx.send(1).unwrap();
            WakeLater::new(waker_tx.clone()).await;
            res_tx.send(2).unwrap();
        };
        local.remote.spawn(TaskCell::new(
            fut,
            Extras::single_level(),
        ));

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

    #[test]
    fn test_wake_by_self() {
        let mut local = MockLocal::default();
        let (res_tx, res_rx) = mpsc::channel();

        let fut = async move {
            res_tx.send(1).unwrap();
            PendingOnce::new().await;
            res_tx.send(2).unwrap();
        };
        local.remote.spawn(TaskCell::new(
            fut,
            Extras::single_level(),
        ));

        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 1);
        assert_eq!(res_rx.recv().unwrap(), 2);
    }

    #[test]
    fn test_repoll_limit() {
        let mut local = MockLocal::new(Runner::new(2));
        let (res_tx, res_rx) = mpsc::channel();

        let fut = async move {
            res_tx.send(1).unwrap();
            PendingOnce::new().await;
            res_tx.send(2).unwrap();
            PendingOnce::new().await;
            res_tx.send(3).unwrap();
            PendingOnce::new().await;
            res_tx.send(4).unwrap();
        };
        local.remote.spawn(TaskCell::new(
            fut,
            Extras::single_level(),
        ));

        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 1);
        assert_eq!(res_rx.recv().unwrap(), 2);
        assert_eq!(res_rx.recv().unwrap(), 3);
        assert!(res_rx.try_recv().is_err());

        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 4);
    }

    #[test]
    fn test_reschedule() {
        let mut local = MockLocal::default();
        let (res_tx, res_rx) = mpsc::channel();

        let fut = async move {
            res_tx.send(1).unwrap();
            reschedule().await;
            res_tx.send(2).unwrap();
            PendingOnce::new().await;
            res_tx.send(3).unwrap();
        };
        local.remote.spawn(TaskCell::new(
            fut,
            Extras::single_level(),
        ));

        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 1);
        assert!(res_rx.try_recv().is_err());
        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 2);
        assert_eq!(res_rx.recv().unwrap(), 3);
    }
}
