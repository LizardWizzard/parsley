use std::future::Future;

pub mod real;
pub mod sim;

pub trait Net {
    // TcpStream, Udp, ...
}

pub trait DmaFile {}

pub trait Fs {
    type DmaFile: DmaFile;
}

pub trait Time {}

pub trait Rng {}

pub trait Task<T> {
    type JoinHandle: JoinHandle<T>;

    fn detach(self) -> Self::JoinHandle;
    // TODO async fn cancel()
}

pub trait JoinHandle<T> {}

pub trait Env {
    type Net: Net;
    type Fs: Fs;
    type Time: Time;
    // https://rust-random.github.io/book/guide-seeding.html#a-simple-number
    // example on using master rng to generate seeds
    type Rng: Rng;

    type Task<T>: Task<T>;
    type JoinHandle<T>: JoinHandle<T>;
    // TODO other runtime primitives, like channels
    // TODO how to model multiple glommio executors? several envs?
    // TODO give tasks names
    fn spawn_local<T: 'static>(&self, f: impl Future<Output = T> + 'static) -> Self::Task<T>;

    fn fs(&self) -> Self::Fs;

    // TODO spawn into glommio queue.
}

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
