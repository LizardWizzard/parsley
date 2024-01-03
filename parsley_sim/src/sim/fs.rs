use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
    io,
    ops::Deref,
    path::PathBuf,
    rc::Rc,
    time::Duration,
};

use crate::{error::Error, Time};

use super::{SimRng, SimTime};

const DIRECT_IO_ALIGNMENT: u64 = 512;

async fn sleep_io_latency(time: &SimTime, _rng: &SimRng) {
    time.sleep(Duration::from_millis(2)).await; // TODO randomize
}

pub struct DmaBuffer(Vec<u8>);

impl crate::DmaBuffer for DmaBuffer {}

pub struct ReadResult(Vec<u8>);

impl Deref for ReadResult {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl crate::ReadResult for ReadResult {}

pub struct DmaFile {
    id: FileId,
    driver: SimFs,
}

impl crate::DmaFile for DmaFile {
    type DmaBuffer = DmaBuffer;

    type ReadResult = ReadResult;

    async fn write_at(&self, buf: Self::DmaBuffer, pos: u64) -> Result<usize, Error> {
        self.driver.dma_file_write_at(&self.id, buf, pos).await
    }

    async fn read_at_aligned(&self, pos: u64, size: usize) -> Result<Self::ReadResult, Error> {
        self.driver.read_at_aligned(&self.id, pos, size).await
    }

    async fn fdatasync(&self) -> Result<(), Error> {
        self.driver.fdatasync(self.id).await
    }

    async fn pre_allocate(&self, size: u64, keep_size: bool) -> Result<(), Error> {
        self.driver.pre_allocate(size, keep_size).await
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct FileId(u32);

#[derive(Default)]
struct FileState {
    path: PathBuf,
    data: Vec<u8>,
}

struct SimFsInner {
    // Keep files there. Reference them by ids, because of links etc.
    // Make separate path_to_id mapping, and probably separate fd_to_id
    state: HashMap<FileId, FileState>,
    path_to_id: HashMap<PathBuf, FileId>,
    next_id: FileId,
}

#[derive(Clone)]
pub struct SimFs {
    inner: Rc<RefCell<SimFsInner>>,
    time: SimTime,
    rng: SimRng,
}

impl SimFs {
    async fn dma_file_write_at(
        &self,
        file_id: &FileId,
        buf: DmaBuffer,
        pos: u64,
    ) -> Result<usize, Error> {
        assert!(pos % DIRECT_IO_ALIGNMENT == 0);
        assert!(buf.0.len() as u64 % DIRECT_IO_ALIGNMENT == 0);

        sleep_io_latency(&self.time, &self.rng).await;

        // TODO split to atomic operations and await between them so this is observable
        // TODO incomplete write, write less then a buffer. Is it possible?
        // TODO inject errors

        match self.inner.borrow_mut().state.get_mut(file_id) {
            Some(file_state) => {
                file_state.data[pos as usize..pos as usize + buf.0.len()].copy_from_slice(&buf.0);
                Ok(buf.0.len())
            }
            None => Err(Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid file descriptor",
            )),
        }
    }

    async fn read_at_aligned(
        &self,
        file_id: &FileId,
        pos: u64,
        size: usize,
    ) -> Result<ReadResult, Error> {
        assert!(pos % DIRECT_IO_ALIGNMENT == 0);
        assert!(size as u64 % DIRECT_IO_ALIGNMENT == 0);

        sleep_io_latency(&self.time, &self.rng).await;

        match self.inner.borrow_mut().state.get_mut(file_id) {
            Some(file_state) => Ok(ReadResult(
                file_state.data[pos as usize..pos as usize + size].to_vec(),
            )),
            None => Err(Error::new(
                io::ErrorKind::InvalidInput,
                "Invalid file descriptor",
            )),
        }
    }

    async fn fdatasync(&self, _id: FileId) -> Result<(), Error> {
        sleep_io_latency(&self.time, &self.rng).await;

        // TODO eio?

        Ok(())
    }

    async fn pre_allocate(&self, _size: u64, _keep_size: bool) -> Result<(), Error> {
        sleep_io_latency(&self.time, &self.rng).await;

        // TODO enospc?

        Ok(())
    }
}

impl crate::Fs for SimFs {
    type DmaFile = DmaFile;

    async fn file_open(&self, path: impl AsRef<std::path::Path>) -> Result<Self::DmaFile, Error> {
        let inner = self.inner.borrow_mut();
        let path = path.as_ref();
        let id = inner.path_to_id.get(path).ok_or(Error::new(
            io::ErrorKind::NotFound,
            "No such file or directory",
        ))?;

        assert!(
            inner.state.contains_key(id),
            "if id is found in path_to_id mapping it is guaranteed to be there"
        );

        sleep_io_latency(&self.time, &self.rng).await;

        Ok(DmaFile {
            id: *id,
            driver: self.clone(),
        })
    }

    // TODO implement open options
    async fn file_create(&self, path: impl AsRef<std::path::Path>) -> Result<Self::DmaFile, Error> {
        let mut inner = self.inner.borrow_mut();
        let id = inner.next_id;
        inner.next_id = FileId(inner.next_id.0 + 1);
        match inner.path_to_id.entry(path.as_ref().to_path_buf()) {
            Entry::Occupied(_) => {
                return Err(Error::new(
                    io::ErrorKind::AlreadyExists,
                    "File already exists",
                ))
            }
            Entry::Vacant(v) => {
                v.insert(id);
            }
        }

        inner.state.insert(id, FileState::default());

        Ok(DmaFile {
            id,
            driver: self.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::{real::RealEnv, sim::SimEnv, Env, Fs};

    fn test_read_write_file(env: impl Env + 'static) {
        env.clone().run(async move {
            let file = env.fs().file_create("foo").await.unwrap();
        });
    }

    #[test]
    fn read_write_file_with_sim_env() {
        let sim_env = SimEnv::new(0);
        test_read_write_file(sim_env)
    }

    #[test]
    fn read_write_file_with_real_env() {
        // TODO test directory?
        let real_env = RealEnv;
        test_read_write_file(real_env)
    }
}
