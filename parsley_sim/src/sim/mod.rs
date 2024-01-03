use std::{
    cell::RefCell,
    future::Future,
    ptr,
    rc::Rc,
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
    time::Duration,
};

use futures::channel::oneshot;
// TODO should be reduntant, but used for .poll calls, figure out right Pin spell
use futures_lite::FutureExt;
use rand::Rng as RandRng; // need as a trait
use rand_xoshiro::{rand_core::SeedableRng, Xoshiro256PlusPlus};
use tracing::Level;

use crate::Task;

use self::{
    fs::SimFs,
    task::{JoinHandle, RawTask, SimTask, RAW_WAKER_VTABLE},
    time::SimTime,
};

mod fs;
mod task;
mod time;

pub struct SimNet;

impl crate::Net for SimNet {}

#[derive(Clone)] // TODO sort it out
pub struct SimRng;

impl crate::Rng for SimRng {}

#[derive(Default)]
struct TaskQueue {
    // in madsim it is built on top of mpsc queue which is built on top of vec
    // think about intrusive linked list
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

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

struct SimEnvInner {
    task_queue: TaskQueue,
    time: SimTime,
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
        // if initialization fails it likely means that whandler was already installed
        // this happens for example when you're running multiple tests each calling to SimEnv new
        let _ = tracing_subscriber::fmt()
            .with_max_level(Level::DEBUG)
            .try_init();

        Self {
            inner: Rc::new(RefCell::new(SimEnvInner {
                task_queue: TaskQueue::default(),
                rng: Xoshiro256PlusPlus::seed_from_u64(seed),
                time: SimTime::new(),
            })),
        }
    }

    fn schedule(&self, task: Rc<RawTask>) {
        if !task.mark_scheduled() {
            tracing::debug!("schedule task={:p}", Rc::as_ptr(&task));
            self.inner.as_ref().borrow_mut().task_queue.push(task);
        } else {
            tracing::debug!("skipping schedule task={:p}", Rc::as_ptr(&task));
        }
    }

    fn next_task(&self) -> Option<Rc<RawTask>> {
        let mut inner = self.inner.as_ref().borrow_mut();
        let (task_queue, rng) = inner.split_borrow();

        task_queue.pop_random(rng)
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
            let _ = rx.send(value);
        };
        let mut inner = self.inner.as_ref().borrow_mut();
        let raw_task = Rc::new(RawTask::new(RefCell::new(Box::pin(wrapper)), self.clone()));
        tracing::debug!("spawn_local task={:p}", Rc::as_ptr(&raw_task));

        inner.task_queue.push(Rc::clone(&raw_task));

        // TODO cancellation for dropped task
        SimTask::from(JoinHandle::from(tx))
    }

    fn run<T: 'static>(&self, f: impl Future<Output = T> + 'static) -> T {
        let root_waker = unsafe { Waker::from_raw(dummy_raw_waker()) };
        let mut root_context = Context::from_waker(&root_waker);

        let mut root_join_handle = self.spawn_local(f).detach();

        loop {
            // glommio spawns the future into the task queue and watches join handle instead
            // most probably this is needed for the task to be accounted in task queue
            if let Poll::Ready(t) = root_join_handle.poll(&mut root_context) {
                // Just copy glommio behavior. In glommio awaiting a join handle returns None
                // if task panicked or the task it refers to was cancelled.
                // Root task cant be cancelled and if it had panicked we're doomed anyway.
                return t.unwrap();
            }

            // at this point if we're not on the first iteration of the loop
            // we've run all tasks thus queue is empty (the check is needed for first iteration)
            // root task is not finished, we checked join handle above
            // if there are armed timers fast forward to the nearest one
            // otherwise we've deadlocked and no progress can be made.
            if !self.time().step_to_next() && self.inner.borrow().task_queue.is_empty() {
                panic!("all tasks are asleep, no progress can be made (deadlock)")
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
                task.do_poll(&mut cx);

                // advance the clock
                let step = Duration::from_nanos(self.inner.borrow_mut().rng.gen_range(50..100));
                self.time().step(step);
            }
        }
    }

    fn fs(&self) -> Self::Fs {
        todo!()
    }

    fn time(&self) -> Self::Time {
        self.inner.borrow().time.clone()
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
        let r = env.clone().run(async move {
            let r = Rc::new(RefCell::new(vec![]));
            for i in 0..10 {
                // Will awaiting on join handles brake it?
                let cloned = Rc::clone(&r);
                env.spawn_local(async move { cloned.borrow_mut().push(i) })
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
        env.run(async move {
            future::yield_now().await;
        });
    }

    #[test]
    fn wake() {
        let env = SimEnv::new(0);
        env.clone().run(async move {
            let waker_slot = Rc::new(RefCell::new(None));
            let cloned = Rc::clone(&waker_slot);
            let jh1 = env
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

            let jh2 = env
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
}
