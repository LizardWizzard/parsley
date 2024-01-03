use glommio::task::JoinHandle as GlommioJoinHandle;
use glommio::{LocalExecutor, Task as GlommioTask};
use std::future::Future;
use std::time::Duration;

use crate::error::Error;

impl<T> crate::JoinHandle<T> for GlommioJoinHandle<T> {}

impl<T> crate::Task<T> for GlommioTask<T> {
    type JoinHandle = GlommioJoinHandle<T>;

    fn detach(self) -> Self::JoinHandle {
        GlommioTask::detach(self)
    }
}

pub struct GlommioNet;

impl crate::Net for GlommioNet {}

impl crate::DmaBuffer for glommio::io::DmaBuffer {}

impl crate::ReadResult for glommio::io::ReadResult {}

impl crate::DmaFile for glommio::io::DmaFile {
    type DmaBuffer = glommio::io::DmaBuffer;

    type ReadResult = glommio::io::ReadResult;

    async fn write_at(&self, buf: Self::DmaBuffer, pos: u64) -> Result<usize, crate::error::Error> {
        glommio::io::DmaFile::write_at(&self, buf, pos)
            .await
            .map_err(Error::from)
    }

    async fn read_at_aligned(
        &self,
        pos: u64,
        size: usize,
    ) -> Result<Self::ReadResult, crate::error::Error> {
        glommio::io::DmaFile::read_at_aligned(&self, pos, size)
            .await
            .map_err(Error::from)
    }

    async fn fdatasync(&self) -> Result<(), crate::error::Error> {
        glommio::io::DmaFile::fdatasync(&self)
            .await
            .map_err(Error::from)
    }

    async fn pre_allocate(&self, size: u64, keep_size: bool) -> Result<(), crate::error::Error> {
        glommio::io::DmaFile::pre_allocate(&self, size, keep_size)
            .await
            .map_err(Error::from)
    }
}

pub struct GlommioFs;

impl crate::Fs for GlommioFs {
    type DmaFile = glommio::io::DmaFile;

    async fn file_open(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self::DmaFile, crate::error::Error> {
        glommio::io::DmaFile::open(path).await.map_err(Error::from)
    }

    async fn file_create(
        &self,
        path: impl AsRef<std::path::Path>,
    ) -> Result<Self::DmaFile, crate::error::Error> {
        glommio::io::DmaFile::create(path)
            .await
            .map_err(Error::from)
    }
}

pub struct GlommioTime;

impl crate::Time for GlommioTime {
    type Instant = std::time::Instant;

    async fn sleep(&self, d: Duration) {
        glommio::timer::sleep(d).await
    }

    fn now(&self) -> Self::Instant {
        std::time::Instant::now()
    }

    fn elapsed(&self, since: Self::Instant) -> Duration {
        since.elapsed()
    }
}

pub struct RealRng;

impl crate::Rng for RealRng {}

#[derive(Clone, Copy)]
pub struct RealEnv;

impl crate::Env for RealEnv {
    type Net = GlommioNet;

    type Fs = GlommioFs;

    type Time = GlommioTime;

    type Rng = RealRng;

    type JoinHandle<T> = GlommioJoinHandle<T>;

    type Task<T> = GlommioTask<T>;

    fn spawn_local<T: 'static>(&self, f: impl Future<Output = T> + 'static) -> Self::Task<T> {
        glommio::spawn_local(f)
    }

    fn run<T>(&self, f: impl Future<Output = T> + 'static) -> T {
        // TODO customize parameters via assoc type Config?
        let ex = LocalExecutor::default();
        ex.run(f)
    }

    fn fs(&self) -> Self::Fs {
        GlommioFs
    }

    fn time(&self) -> Self::Time {
        GlommioTime
    }
}
