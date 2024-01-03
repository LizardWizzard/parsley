use futures::channel::oneshot;

use std::{
    cell::{Cell, RefCell},
    future::Future,
    mem,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, RawWaker, RawWakerVTable},
};

use super::SimEnv;

pub struct JoinHandle<T> {
    receiver: oneshot::Receiver<T>,
}

impl<T> From<oneshot::Receiver<T>> for JoinHandle<T> {
    fn from(receiver: oneshot::Receiver<T>) -> Self {
        JoinHandle { receiver }
    }
}

impl<T> crate::JoinHandle<T> for JoinHandle<T> {}

impl<T> Future for JoinHandle<T> {
    type Output = Option<T>;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // this inlines the implementation from futures_lite::future::FutureExt;
        match Future::poll(Pin::new(&mut self.receiver), cx) {
            Poll::Ready(Ok(v)) => Poll::Ready(Some(v)),
            // cancelled or panicked, receiver was dropped
            Poll::Ready(Err(_)) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct SimTask<T>(Option<JoinHandle<T>>);

impl<T> From<JoinHandle<T>> for SimTask<T> {
    fn from(join_handle: JoinHandle<T>) -> Self {
        SimTask(Some(join_handle))
    }
}

impl<T> crate::Task<T> for SimTask<T> {
    type JoinHandle = JoinHandle<T>;

    fn detach(mut self) -> Self::JoinHandle {
        self.0.take().unwrap()
    }
}

pub(crate) struct RawTask {
    f: RefCell<Pin<Box<dyn Future<Output = ()> + 'static>>>,
    scheduled: Cell<bool>,
    env: SimEnv,
}

impl RawTask {
    pub(crate) fn new(
        f: RefCell<Pin<Box<dyn Future<Output = ()> + 'static>>>,
        env: SimEnv,
    ) -> RawTask {
        RawTask {
            f,
            env,
            scheduled: Cell::new(true),
        }
    }

    pub(crate) fn do_poll(&self, cx: &mut Context<'_>) {
        let mut fut = self.f.borrow_mut();
        tracing::debug!("pre_poll task={:p}", self);
        // do that before poll, because poll can immediately call wake
        self.scheduled.set(false);
        let poll = fut.as_mut().poll(cx);
        tracing::debug!("poll task={:p} poll={:?}", self, poll);
    }

    pub(crate) fn mark_scheduled(&self) -> bool {
        self.scheduled.replace(true)
    }
}

#[inline]
unsafe fn task_from_ptr(d: *const ()) -> Rc<RawTask> {
    let ptr = d as *const RawTask;
    unsafe { Rc::from_raw(ptr) }
}

fn raw_waker_clone(d: *const ()) -> RawWaker {
    let ptr = d as *const RawTask;
    tracing::debug!("waker clone task={:p}", ptr);
    unsafe { Rc::increment_strong_count(ptr) };
    RawWaker::new(d, RAW_WAKER_VTABLE)
}

fn raw_waker_wake(d: *const ()) {
    // schedule the task for running through env
    // reference count gets decremented according to [`RawWakerVTable::wake`] documentation
    let task = unsafe { task_from_ptr(d) };
    tracing::debug!("waker wake task={:p}", Rc::as_ptr(&task));
    let task_cloned = Rc::clone(&task);
    task.env.schedule(task_cloned);

    // decrement rc
    drop(task);
}

fn raw_waker_wake_by_ref(d: *const ()) {
    // schedule the task for running through env
    // reference count **does not** gets decremented according to [`RawWakerVTable::wake_by_ref`]
    // documentation
    let task = unsafe { task_from_ptr(d) };
    tracing::debug!("waker wake_by_ref task={:p}", Rc::as_ptr(&task));
    let task_cloned = Rc::clone(&task);
    task.env.schedule(task_cloned);

    // not decrementing the rc
    mem::forget(task);
}

fn raw_waker_drop(d: *const ()) {
    let ptr = d as *const RawTask;
    unsafe { Rc::decrement_strong_count(ptr) };
}

pub(crate) const RAW_WAKER_VTABLE: &RawWakerVTable = &RawWakerVTable::new(
    raw_waker_clone,
    raw_waker_wake,
    raw_waker_wake_by_ref,
    raw_waker_drop,
);
