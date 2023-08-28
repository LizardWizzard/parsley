use glommio::task::JoinHandle as GlommioJoinHandle;
use glommio::Task as GlommioTask;
use std::future::Future;

impl<T> crate::JoinHandle<T> for GlommioJoinHandle<T> {}

impl<T> crate::Task<T> for GlommioTask<T> {
    type JoinHandle = GlommioJoinHandle<T>;

    fn detach(self) -> Self::JoinHandle {
        GlommioTask::detach(self)
    }
}

struct GlommioNet;

impl crate::Net for GlommioNet {}

impl crate::DmaFile for glommio::io::DmaFile {}

struct GlommioFs;

impl crate::Fs for GlommioFs {
    type DmaFile = glommio::io::DmaFile;
}

struct GlommioTime;

impl crate::Time for GlommioTime {}

struct RealRng;

impl crate::Rng for RealRng {}

struct RealEnv;

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

    fn fs(&self) -> Self::Fs {
        todo!()
    }
}
