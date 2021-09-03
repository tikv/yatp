// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A [`Future`].

use crate::pool::{Local, WeakRemote};
use crate::queue::{Extras, WithExtras};

use std::cell::{Cell, UnsafeCell};
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::atomic::{
    AtomicU8, AtomicUsize,
    Ordering::{Acquire, Relaxed, Release, SeqCst},
};
use std::sync::{atomic, Arc};
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::{borrow::Cow, ptr};
use std::{fmt, mem};

/// The default repoll limit for a future runner. See `Runner::new` for
/// details.
const DEFAULT_REPOLL_LIMIT: usize = 5;

struct TaskExtras {
    extras: Extras,
    remote: Option<WeakRemote<TaskCell>>,
}

#[repr(C)]
struct RawTask<F> {
    ref_count: AtomicUsize,
    status: AtomicU8,
    extras: UnsafeCell<TaskExtras>,
    poll_fn: unsafe fn(&TaskCell, &mut Context<'_>) -> Poll<()>,
    data: UnsafeCell<F>,
}

impl<F> RawTask<F>
where
    F: Future<Output = ()> + Send + 'static,
{
    unsafe fn poll(task: &TaskCell, cx: &mut Context<'_>) -> Poll<()> {
        let mut typed_ptr: NonNull<RawTask<F>> = task.0.cast();
        Pin::new_unchecked(typed_ptr.as_mut().data.get_mut()).poll(cx)
    }
}

/// A reference counted `RawTask`.
pub struct TaskCell(NonNull<RawTask<()>>);

unsafe impl Send for TaskCell {}
unsafe impl Sync for TaskCell {}

impl TaskCell {
    fn status(&self) -> &AtomicU8 {
        unsafe { &self.0.as_ref().status }
    }

    fn extras(&self) -> &UnsafeCell<TaskExtras> {
        unsafe { &self.0.as_ref().extras }
    }

    unsafe fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        (self.0.as_ref().poll_fn)(self, cx)
    }

    fn into_raw(self) -> *const () {
        let ptr = self.0.as_ptr() as _;
        mem::forget(self);
        ptr
    }

    unsafe fn from_raw(ptr: *const ()) -> Self {
        TaskCell(NonNull::new_unchecked(ptr as _))
    }
}

impl Clone for TaskCell {
    fn clone(&self) -> Self {
        unsafe { self.0.as_ref().ref_count.fetch_add(1, Relaxed) };
        TaskCell(self.0)
    }
}

impl Drop for TaskCell {
    fn drop(&mut self) {
        unsafe {
            if self.0.as_ref().ref_count.fetch_sub(1, Release) != 1 {
                return;
            }
        }
        atomic::fence(Acquire);
        unsafe { ptr::drop_in_place(self.0.as_ptr()) };
    }
}

impl fmt::Debug for TaskCell {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        "future::TaskCell".fmt(f)
    }
}

impl<F> WithExtras<TaskCell> for F
where
    F: Future<Output = ()> + Send + 'static,
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
    pub fn new<F: Future<Output = ()> + Send + 'static>(future: F, extras: Extras) -> Self {
        let inner = Box::new(RawTask {
            ref_count: AtomicUsize::new(1),
            status: AtomicU8::new(NOTIFIED),
            extras: UnsafeCell::new(TaskExtras {
                extras,
                remote: None,
            }),
            poll_fn: RawTask::<F>::poll,
            data: UnsafeCell::new(future),
        });
        unsafe { TaskCell(NonNull::new_unchecked(Box::into_raw(inner) as _)) }
    }
}

impl crate::queue::TaskCell for TaskCell {
    fn mut_extras(&mut self) -> &mut Extras {
        unsafe { &mut (*self.0.as_ref().extras.get()).extras }
    }
}

#[inline]
unsafe fn waker(task: TaskCell) -> Waker {
    Waker::from_raw(RawWaker::new(
        task.into_raw(),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    ))
}

#[inline]
unsafe fn clone_raw(this: *const ()) -> RawWaker {
    let task_cell = clone_task(this);
    RawWaker::new(
        task_cell.into_raw(),
        &RawWakerVTable::new(clone_raw, wake_raw, wake_ref_raw, drop_raw),
    )
}

#[inline]
unsafe fn drop_raw(this: *const ()) {
    drop(TaskCell::from_raw(this))
}

unsafe fn wake_impl(task: Cow<'_, TaskCell>) {
    let mut status = task.status().load(SeqCst);
    loop {
        match status {
            IDLE => {
                match task
                    .status()
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
                    .status()
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
    let task_cell = TaskCell::from_raw(this);
    wake_impl(Cow::Owned(task_cell));
}

#[inline]
unsafe fn wake_ref_raw(this: *const ()) {
    let task_cell = ManuallyDrop::new(TaskCell::from_raw(this));
    wake_impl(Cow::Borrowed(&task_cell));
}

#[inline]
unsafe fn clone_task(task: *const ()) -> TaskCell {
    let task_cell = TaskCell::from_raw(task);
    let extras = &mut *task_cell.extras().get();
    if extras.remote.is_none() {
        LOCAL.with(|l| {
            extras.remote = Some((&*l.get()).weak_remote());
        })
    }
    mem::forget(task_cell.clone());
    task_cell
}

thread_local! {
    /// Local queue reference that is set before polling and unset after polled.
    static LOCAL: Cell<*mut Local<TaskCell>> = Cell::new(std::ptr::null_mut());
}

unsafe fn wake_task(task: Cow<'_, TaskCell>, reschedule: bool) {
    LOCAL.with(|ptr| {
        // `wake_task` is only called when the status of the task is IDLE. Before the
        // status is set to IDLE, the runtime will set `remote` in `TaskExtras`. So we
        // can make sure `remote` is not None.
        let task_remote = (*task.extras().get())
            .remote
            .as_ref()
            .expect("core should exist!!!");
        let out_of_polling = ptr.get().is_null()
            || !ptr::eq(Arc::as_ptr(&(*ptr.get()).core()), task_remote.as_core_ptr());
        if out_of_polling {
            // It's out of polling process, has to be spawn to global queue.
            // It needs to clone to make it safe as it's unclear whether `self`
            // is still used inside method `spawn` after `TaskCell` is dropped.
            if let Some(remote) = task_remote.upgrade() {
                remote.spawn(task.clone().into_owned());
            }
        } else if reschedule {
            // It's requested explicitly to schedule to global queue.
            (*ptr.get()).spawn_remote(task.into_owned());
        } else {
            // Otherwise spawns to local queue for best locality.
            (*ptr.get()).spawn(task.into_owned());
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
        let scope = Scope::new(local);
        let task = TaskCell(task_cell.0);
        unsafe {
            let waker = ManuallyDrop::new(waker(task_cell));
            let mut cx = Context::from_waker(&waker);
            let mut repoll_times = 0;
            loop {
                task.status().store(POLLING, SeqCst);
                if task.poll(&mut cx).is_ready() {
                    task.status().store(COMPLETED, SeqCst);
                    return true;
                }
                let extras = { &mut *task.extras().get() };
                if extras.remote.is_none() {
                    // It's possible to avoid assigning remote in some cases, but it requires
                    // at least one atomic load to detect such situation. So here just assign
                    // it to make things simple.
                    LOCAL.with(|l| {
                        extras.remote = Some((&*l.get()).weak_remote());
                    })
                }
                match task
                    .status()
                    .compare_exchange(POLLING, IDLE, SeqCst, SeqCst)
                {
                    Ok(_) => return false,
                    Err(NOTIFIED) => {
                        let need_reschedule = NEED_RESCHEDULE.with(|r| r.replace(false));
                        if (repoll_times >= self.repoll_limit || need_reschedule)
                            && scope.0.need_preempt()
                        {
                            wake_task(Cow::Owned(task), need_reschedule);
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
    use crate::pool::{build_spawn, Builder, Remote, Runner as _};
    use crate::queue::QueueType;

    use std::sync::mpsc;
    use std::{cell::RefCell, thread};
    use std::{rc::Rc, time::Duration};

    struct MockLocal {
        runner: Rc<RefCell<Runner>>,
        remote: Remote<TaskCell>,
        locals: Vec<Local<TaskCell>>,
    }

    impl MockLocal {
        fn new(runner: Runner) -> MockLocal {
            let (remote, locals) = build_spawn(QueueType::SingleLevel, Default::default());
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
        local.remote.spawn(fut);

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
        local.remote.spawn(fut);

        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 1);
        assert_eq!(res_rx.recv().unwrap(), 2);
    }

    #[test]
    fn test_multi_pools_wake() {
        let pool1 = Builder::new("test_multi_pools_wake_1")
            .max_thread_count(1)
            .build_future_pool();
        let pool2 = Builder::new("test_multi_pools_wake_2")
            .max_thread_count(1)
            .build_future_pool();
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (tx2, rx2) = std::sync::mpsc::channel();
        pool1.spawn(async move {
            let tid = thread::current().id();
            rx.recv().await.unwrap();
            // pool1 has only one thread, so the thread id should not change
            assert_eq!(tid, thread::current().id());
            tx2.send(()).unwrap();
        });
        thread::sleep(Duration::from_millis(500));
        pool2.spawn(async move {
            tx.send(()).unwrap();
        });
        rx2.recv().unwrap();
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_repoll_limit() {
        let _guard = fail::FailScenario::setup();
        fail::cfg("need-preempt", "return(true)").unwrap();
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
        local.remote.spawn(fut);

        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 1);
        assert_eq!(res_rx.recv().unwrap(), 2);
        assert_eq!(res_rx.recv().unwrap(), 3);
        assert!(res_rx.try_recv().is_err());

        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 4);
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_reschedule() {
        let _guard = fail::FailScenario::setup();
        fail::cfg("need-preempt", "return(true)").unwrap();
        let mut local = MockLocal::default();
        let (res_tx, res_rx) = mpsc::channel();

        let fut = async move {
            res_tx.send(1).unwrap();
            reschedule().await;
            res_tx.send(2).unwrap();
            PendingOnce::new().await;
            res_tx.send(3).unwrap();
        };
        local.remote.spawn(fut);

        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 1);
        assert!(res_rx.try_recv().is_err());
        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 2);
        assert_eq!(res_rx.recv().unwrap(), 3);
    }

    #[cfg_attr(not(feature = "failpoints"), ignore)]
    #[test]
    fn test_no_preemptive_task() {
        let _guard = fail::FailScenario::setup();
        fail::cfg("need-preempt", "return(false)").unwrap();
        let mut local = MockLocal::default();
        let (res_tx, res_rx) = mpsc::channel();

        let fut = async move {
            res_tx.send(1).unwrap();
            reschedule().await;
            res_tx.send(2).unwrap();
        };
        local.remote.spawn(fut);

        local.handle_once();
        assert_eq!(res_rx.recv().unwrap(), 1);
        assert_eq!(res_rx.recv().unwrap(), 2);
    }
}
