#![feature(async_fn_in_trait)]
#![feature(extract_if)]
#![feature(waker_getters)]

use std::{future::Future, ops::Deref, path::Path, time::Duration};

use error::Error;

pub mod error;
pub mod real;
pub mod sim;

pub trait Net {
    // TcpStream, Udp, ...
}

pub trait DmaBuffer {}

pub trait ReadResult: Deref<Target = [u8]> {}

pub trait DmaFile {
    type DmaBuffer: DmaBuffer;
    type ReadResult: ReadResult;

    async fn write_at(&self, buf: Self::DmaBuffer, pos: u64) -> Result<usize, Error>;
    /// direct io requires aligment, glommio has read_at that aligns provided positions at a cost
    async fn read_at_aligned(&self, pos: u64, size: usize) -> Result<Self::ReadResult, Error>;

    async fn fdatasync(&self) -> Result<(), Error>;

    async fn pre_allocate(&self, size: u64, keep_size: bool) -> Result<(), Error>;

    // TODO
    // truncate
    // rename
    // other apis
}

pub trait Fs {
    type DmaFile: DmaFile;

    async fn file_open(&self, path: impl AsRef<Path>) -> Result<Self::DmaFile, Error>;

    async fn file_create(&self, path: impl AsRef<Path>) -> Result<Self::DmaFile, Error>;
}

pub trait Time {
    type Instant;

    async fn sleep(&self, d: Duration);

    fn now(&self) -> Self::Instant;

    fn elapsed(&self, since: Self::Instant) -> Duration;
}

pub trait Rng {}

pub trait Task<T> {
    type JoinHandle: JoinHandle<T>;

    fn detach(self) -> Self::JoinHandle;
    // TODO async fn cancel()
}

pub trait JoinHandle<T> {}

pub trait Env: Clone {
    type Net: Net;
    type Fs: Fs;
    type Time: Time;
    // https://rust-random.github.io/book/guide-seeding.html#a-simple-number
    // example on using master rng to generate seeds
    type Rng: Rng;

    type Task<T>: Task<T>;
    type JoinHandle<T>: JoinHandle<T>;

    fn run<T: 'static>(&self, f: impl Future<Output = T> + 'static) -> T;

    // TODO other runtime primitives, like channels
    // TODO how to model multiple glommio executors? several envs?
    // TODO give tasks names
    fn spawn_local<T: 'static>(&self, f: impl Future<Output = T> + 'static) -> Self::Task<T>;

    fn fs(&self) -> Self::Fs;

    fn time(&self) -> Self::Time;

    // TODO spawn into glommio queue.
}

#[cfg(test)]
mod tests {
    use futures_lite::future;

    use crate::{real::RealEnv, sim::SimEnv, Env};

    fn run(env: impl Env) {
        let r = env.run(async {
            future::yield_now().await;
            42
        });
        assert_eq!(r, 42)
    }

    #[test]
    fn dummy_run_with_sim_env() {
        let sim_env = SimEnv::new(0);
        run(sim_env)
    }

    #[test]
    fn dummy_run_with_real_env() {
        let real_env = RealEnv;
        run(real_env)
    }
}
