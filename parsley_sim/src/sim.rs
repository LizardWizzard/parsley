use std::{
    cell::RefCell,
    future::Future,
    mem,
    pin::Pin,
    ptr,
    rc::Rc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

use futures::channel::oneshot;
// TODO should be reduntant, but used for .poll calls, figure out right Pin spell
use futures_lite::FutureExt;
use rand::Rng as RandRng; // need as a trait
use rand_xoshiro::{rand_core::SeedableRng, Xoshiro256PlusPlus};

pub struct SimNet;

impl crate::Net for SimNet {}

pub struct DmaFile;

impl crate::DmaFile for DmaFile {}

pub struct SimFs;

impl crate::Fs for SimFs {
    type DmaFile = DmaFile;
}

pub struct SimTime;

impl crate::Time for SimTime {}

pub struct SimRng;

impl crate::Rng for SimRng {}

pub struct JoinHandle<T> {
    receiver: oneshot::Receiver<T>,
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

impl<T> crate::Task<T> for SimTask<T> {
    type JoinHandle = JoinHandle<T>;

    fn detach(mut self) -> Self::JoinHandle {
        self.0.take().unwrap()
    }
}

struct RawTask {
    _id: usize,
    f: RefCell<Pin<Box<dyn Future<Output = ()> + 'static>>>,
    env: SimEnv,
}

#[inline]
unsafe fn task_from_ptr(d: *const ()) -> Rc<RawTask> {
    let ptr = d as *const RawTask;
    unsafe { Rc::from_raw(ptr) }
}

fn raw_waker_clone(d: *const ()) -> RawWaker {
    let ptr = d as *const RawTask;
    unsafe { Rc::increment_strong_count(ptr) };
    RawWaker::new(d, RAW_WAKER_VTABLE)
}

fn raw_waker_wake(d: *const ()) {
    // schedule the task for running through env
    // reference count gets decremented according to [`RawWakerVTable::wake`] documentation
    let task = unsafe { task_from_ptr(d) };
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
    let task_cloned = Rc::clone(&task);
    task.env.schedule(task_cloned);

    // not decrementing the rc
    mem::forget(task);
}

fn raw_waker_drop(d: *const ()) {
    let ptr = d as *const RawTask;
    unsafe { Rc::decrement_strong_count(ptr) };
}

const RAW_WAKER_VTABLE: &RawWakerVTable = &RawWakerVTable::new(
    raw_waker_clone,
    raw_waker_wake,
    raw_waker_wake_by_ref,
    raw_waker_drop,
);

#[derive(Default)]
struct TaskQueue {
    // in madsim it is built on top of mpsc queue which is built on top of vec
    tasks: Vec<Rc<RawTask>>,
}

impl TaskQueue {
    fn pop_random(&mut self, rng: &mut Xoshiro256PlusPlus) -> Option<Rc<RawTask>> {
        if self.tasks.is_empty() {
            return None;
        }

        let idx = rng.gen_range(0..self.tasks.len());
        Some(self.tasks.swap_remove(idx))
    }

    fn push(&mut self, task: Rc<RawTask>) {
        self.tasks.push(task)
    }
}

// TODO use inner/rcrefcell pattern for ease of cloning, so its easy to pass clone of env everywhere
struct SimEnvInner {
    task_queue: TaskQueue,
    counter: usize,
    // should it be stored in rng type?
    rng: Xoshiro256PlusPlus,
}

impl SimEnvInner {
    fn split_borrow(&mut self) -> (&mut TaskQueue, &mut Xoshiro256PlusPlus) {
        (&mut self.task_queue, &mut self.rng)
    }
}

#[derive(Clone)]
pub struct SimEnv {
    inner: Rc<RefCell<SimEnvInner>>,
}

fn dummy_raw_waker() -> RawWaker {
    RawWaker::new(ptr::null::<()>(), dummy_vtable())
}

fn dummy_vtable() -> &'static RawWakerVTable {
    &RawWakerVTable::new(|_| dummy_raw_waker(), |_| {}, |_| {}, |_| {})
}

impl SimEnv {
    pub fn new(seed: u64) -> Self {
        Self {
            inner: Rc::new(RefCell::new(SimEnvInner {
                task_queue: TaskQueue::default(),
                counter: usize::default(),
                rng: Xoshiro256PlusPlus::seed_from_u64(seed),
            })),
        }
    }

    fn schedule(&self, task: Rc<RawTask>) {
        self.inner.as_ref().borrow_mut().task_queue.push(task);
    }

    fn next_task(&self) -> Option<Rc<RawTask>> {
        let mut inner = self.inner.as_ref().borrow_mut();
        let (task_queue, rng) = inner.split_borrow();

        task_queue.pop_random(rng)
    }

    // TODO figure out a way to accept a function that produces a future when given reference to an env
    pub fn run<T, Fut, Fun>(self, f: Fun) -> T
    where
        Fut: Future<Output = T>,
        // Fun: FnOnce(&mut E) -> Fut, i want to accept a function that is generic over E, so it is not FnOnce(E)
        Fun: FnOnce(Self) -> Fut,
    {
        let root_waker = unsafe { Waker::from_raw(dummy_raw_waker()) };
        let mut root_context = Context::from_waker(&root_waker);

        let mut fut = Box::pin(f(self.clone()));

        loop {
            // TODO glommio spawns the future and watches join handle instead, why?
            //     is it because of the task queue? I think so
            if let Poll::Ready(t) = fut.poll(&mut root_context) {
                return t;
            }
            // poll all futures (ones scheduled for polling) in tasks, see how local_pool in futures uses that

            // NOTE: we have to reborrow from RefCell on each iteration separately.
            // Because wake{_by_ref} calls executed inside poll will also
            // try to borrow inner as mutable via call to `schedule`
            while let Some(task) = self.next_task() {
                let cloned = Rc::clone(&task);
                let waker = unsafe {
                    Waker::from_raw(RawWaker::new(
                        Rc::into_raw(cloned) as *const (),
                        RAW_WAKER_VTABLE,
                    ))
                };
                let mut cx = Context::from_waker(&waker);
                let mut fut = task.as_ref().f.borrow_mut();
                match fut.poll(&mut cx) {
                    Poll::Ready(_) => {}
                    Poll::Pending => {}
                }
            }

            // advance time like its done in madsim
        }
    }
}

impl crate::Env for SimEnv {
    type Net = SimNet;

    type Fs = SimFs;

    type Time = SimTime;

    type Rng = SimRng;

    type JoinHandle<T> = JoinHandle<T>;

    type Task<T> = SimTask<T>;

    fn spawn_local<T: 'static>(&self, f: impl Future<Output = T> + 'static) -> Self::Task<T> {
        // oneshot is sort of a hack so all spawned futures have output type equal to unit
        // also oneshot from futures contains Arc, should be easy to replace it with
        // our own with Option<Waker> and Option<T> for result.
        let (rx, tx) = oneshot::channel();
        let wrapper = async move {
            let value = f.await;
            // TODO any meaningful err?
            let _ = rx.send(value);
        };
        let mut inner = self.inner.as_ref().borrow_mut();
        //////////////////////////////////////////
        let raw_task = Rc::new(RawTask {
            _id: inner.counter,
            f: RefCell::new(Box::pin(wrapper)),
            env: self.clone(),
        });
        inner.counter += 1;
        inner.task_queue.push(Rc::clone(&raw_task));
        // TODO cancellation for dropped task
        SimTask(Some(JoinHandle { receiver: tx }))
    }

    fn fs(&self) -> Self::Fs {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc, task::Poll};

    use futures::future::join;
    use futures_lite::future;

    use crate::{Env as _, Task};

    use super::SimEnv;

    #[test]
    fn simple() {
        let env = SimEnv::new(0);
        let r = env.run(|e| async move {
            let r = Rc::new(RefCell::new(vec![]));
            for i in 0..10 {
                // Will awaiting on join handles brake it?
                let cloned = Rc::clone(&r);
                e.spawn_local(async move { cloned.borrow_mut().push(i) })
                    .detach();
            }

            future::yield_now().await;
            42
        });
        assert_eq!(r, 42)
    }

    #[test]
    fn yield_now() {
        let env = SimEnv::new(0);
        env.run(|_| async move {
            future::yield_now().await;
        });
    }

    #[test]
    fn wake() {
        let env = SimEnv::new(0);
        env.run(|e| async move {
            let waker_slot = Rc::new(RefCell::new(None));
            let cloned = Rc::clone(&waker_slot);
            let jh1 = e
                .spawn_local(async move {
                    future::poll_fn(|cx| {
                        let mut this = cloned.borrow_mut();
                        match this.as_mut() {
                            // waker is already there, we were woken by another task triggering it
                            Some(_) => {
                                return Poll::Ready(());
                            }
                            // we're polled first time, place waker, return pending, wait till other task comes
                            None => {
                                *this = Some(cx.waker().clone());
                                return Poll::Pending;
                            }
                        }
                    })
                    .await
                })
                .detach();

            let jh2 = e
                .spawn_local(async move {
                    future::poll_fn(|_cx| {
                        let mut this = waker_slot.borrow_mut();
                        match this.as_mut() {
                            // waker is already there, we were woken by another task triggering it
                            Some(w) => {
                                w.wake_by_ref();
                                return Poll::Ready(());
                            }
                            // we're polled first time, place waker, return pending, wait till other task comes
                            None => panic!("waker is missing, polled in wrong order"),
                        }
                    })
                    .await
                })
                .detach();

            join(jh1, jh2).await;
        });
    }

    // TODO test wake, wake by ref.
}
