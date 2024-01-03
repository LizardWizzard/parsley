use std::{
    cell::RefCell,
    cmp::Reverse,
    collections::BinaryHeap,
    future::Future,
    rc::Rc,
    task::{Poll, Waker},
    time::{Duration, SystemTime},
};

use crate::Time;

struct Timer {
    waker: Waker,
    expires_at: SystemTime,
}

impl PartialEq for Timer {
    fn eq(&self, other: &Self) -> bool {
        self.waker.will_wake(&other.waker) && self.expires_at == other.expires_at
    }
}

impl Eq for Timer {}

impl PartialOrd for Timer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.expires_at.partial_cmp(&other.expires_at)
    }
}

impl Ord for Timer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.expires_at.cmp(&other.expires_at)
    }
}

// Note: in sim env we dont have a distinction between Instant and SystemTime (yet).
// There is just one time source in whole simulation. In the future
// we can consider messing with system time on different nodes.
// When this happens we may need to add the separation.
pub struct SimInstant {
    at: SystemTime,
}

struct Sleep<'a> {
    time: &'a SimTime,
    expires_at: SystemTime,
    timer_armed: bool,
}

impl<'a> Sleep<'a> {
    fn new(time: &'a SimTime, expires_at: SystemTime) -> Self {
        Sleep {
            expires_at,
            timer_armed: false,
            time,
        }
    }
}

impl<'a> Future for Sleep<'a> {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        if !self.timer_armed {
            self.timer_armed = true;

            let mut inner = self.time.inner.borrow_mut();
            let timer = Timer {
                waker: cx.waker().clone(),
                expires_at: self.expires_at,
            };
            inner.add_timer(timer);
            return Poll::Pending;
        }

        if self.expires_at <= self.time.inner.borrow().current {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

pub(crate) struct SimTimeInner {
    current: SystemTime,
    // Timers is a Min Heap, so the timer which is the soonest to expire is always first
    timers: BinaryHeap<Reverse<Timer>>,
}

impl SimTimeInner {
    pub(crate) fn new() -> Self {
        let base = SystemTime::UNIX_EPOCH + Duration::from_secs(60 * 60 * 24 * 365 * 30);
        Self {
            current: base,
            timers: BinaryHeap::new(),
        }
    }

    pub(crate) fn advance(&mut self, dur: Duration) {
        self.current += dur
    }

    pub(crate) fn advance_to_next(&mut self) -> bool {
        // TODO assert that timer is there?
        if let Some(t) = self.timers.peek() {
            self.current = t.0.expires_at + Duration::from_nanos(50);
            true
        } else {
            false
        }
    }

    pub(crate) fn process_ready(&mut self) {
        while self
            .timers
            .peek()
            .map(|t| t.0.expires_at <= self.current)
            .unwrap_or(false)
        {
            let expired_timer = self
                .timers
                .pop()
                .expect("Checked above that head is expired");
            expired_timer.0.waker.wake();
        }
    }

    fn add_timer(&mut self, timer: Timer) {
        self.timers.push(Reverse(timer))
    }
}

#[derive(Clone)]
pub struct SimTime {
    pub(crate) inner: Rc<RefCell<SimTimeInner>>,
}

impl SimTime {
    pub fn new() -> SimTime {
        SimTime {
            inner: Rc::new(RefCell::new(SimTimeInner::new())),
        }
    }

    pub(crate) fn step(&self, dur: Duration) {
        let mut inner = self.inner.borrow_mut();
        inner.advance(dur);
        inner.process_ready()
    }

    pub(crate) fn step_to_next(&self) -> bool {
        let mut inner = self.inner.borrow_mut();
        if !inner.advance_to_next() {
            return false;
        }
        inner.process_ready();
        true
    }
}

impl Time for SimTime {
    type Instant = SimInstant;

    async fn sleep(&self, d: Duration) {
        let expires_at = self.inner.borrow().current + d;
        Sleep::new(self, expires_at).await
    }

    fn now(&self) -> Self::Instant {
        SimInstant {
            at: self.inner.borrow_mut().current,
        }
    }

    fn elapsed(&self, since: Self::Instant) -> Duration {
        self.inner
            .borrow_mut()
            .current
            .duration_since(since.at)
            .expect("guaranteed to be earlier")
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use futures_lite::future;

    use crate::{sim::SimEnv, Env, Task, Time};

    use super::SimTime;

    #[test]
    fn smoke_elapsed() {
        let time = SimTime::new();
        let t0 = time.now();
        let dur = Duration::from_nanos(100);
        time.inner.borrow_mut().advance(dur);

        assert_eq!(time.elapsed(t0), dur)
    }

    #[test]
    fn simple_sleep() {
        let env = SimEnv::new(0);
        env.clone().run(async move {
            let real_t0 = Instant::now();
            let t0 = env.time().now();
            env.time().sleep(Duration::from_millis(10)).await;
            // env time advanced exactly sleep duration time
            assert_eq!(
                env.time().elapsed(t0),
                Duration::from_millis(10) + Duration::from_nanos(50)
            );
            // real wall clock time moved significantly less
            assert!(
                real_t0.elapsed() < Duration::from_millis(1),
                "{:?}",
                real_t0.elapsed()
            );
        });
    }

    #[test]
    fn sleep_join() {
        let env = SimEnv::new(0);
        env.clone().run(async move {
            let env_clone = env.clone();
            let jh = env
                .clone()
                .spawn_local(async move {
                    env_clone.time().sleep(Duration::from_millis(10)).await;
                })
                .detach();

            let real_t0 = Instant::now();
            let t0 = env.time().now();

            jh.await;

            // env time advanced exactly sleep duration time
            assert_eq!(
                env.time().elapsed(t0),
                Duration::from_millis(10) + Duration::from_nanos(100)
            );
            // real wall clock time moved significantly less
            assert!(
                real_t0.elapsed() < Duration::from_millis(1),
                "{:?}",
                real_t0.elapsed()
            );
        });
    }

    #[test]
    fn sleep_many() {
        let env = SimEnv::new(0);
        env.clone().run(async move {
            future::yield_now().await;
            env.time().sleep(Duration::from_millis(10)).await;
            future::yield_now().await;
            env.time().sleep(Duration::from_millis(10)).await;
        })
    }
}
