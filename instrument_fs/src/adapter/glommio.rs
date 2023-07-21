use std::os::unix::prelude::AsRawFd;
use std::{cell::Ref, path::Path, rc::Rc};

use glommio::io::{CloseResult, Directory, DmaBuffer, DmaFile, ReadResult};

use crate::{EitherPathOrFd, Event};
use crate::{FileRange, Instrument, WriteEvent};

type Result<T> = std::result::Result<T, glommio::GlommioError<()>>;

/// Wraps glommio's Directory
/// Ideally Instrument should be defaulted to NoopInstrument but
/// Default are not allowed outside of types, e.g. in functions
/// So having a default on the struct without being able to reference
/// it in functions is useless because struct wont be cosntructed directly
/// TODO check how NoopInstrument is optimized and if it is not gate instrument related stuff behind feature gate,
/// or use with_instrument builder style, or use const fn is_noop
/// or use a feature gate
pub struct InstrumentedDirectory<I: Instrument + Clone> {
    dir: Directory,
    pub instrument: I,
}

impl<I: Instrument + Clone> InstrumentedDirectory<I> {
    pub fn try_clone(&self) -> Result<InstrumentedDirectory<I>> {
        let dir = self.dir.try_clone()?;

        self.instrument
            .apply_event(Event::Dup(self.dir.as_raw_fd(), dir.as_raw_fd()))
            .unwrap();

        Ok(InstrumentedDirectory {
            dir,
            instrument: self.instrument.clone(),
        })
    }

    pub async fn open<P: AsRef<Path>>(path: P, instrument: I) -> Result<InstrumentedDirectory<I>> {
        let dir = Directory::open(&path).await?;
        instrument
            .apply_event(Event::Open(path.as_ref().to_owned(), dir.as_raw_fd()))
            .unwrap();

        Ok(InstrumentedDirectory { dir, instrument })
    }

    pub async fn open_file<P: AsRef<Path>>(&self, path: P) -> Result<InstrumentedDmaFile<I>> {
        let file = self.dir.open_file(&path).await?;

        self.instrument
            .apply_event(Event::Open(path.as_ref().to_owned(), file.as_raw_fd()))
            .unwrap();

        Ok(InstrumentedDmaFile {
            file,
            instrument: self.instrument.clone(),
        })
    }

    pub async fn create<P: AsRef<Path>>(
        path: P,
        instrument: I,
    ) -> Result<InstrumentedDirectory<I>> {
        let dir = Directory::create(&path).await?;

        instrument
            .apply_event(Event::CreateDir(path.as_ref().to_owned()))
            .unwrap();

        Ok(InstrumentedDirectory { dir, instrument })
    }

    pub async fn create_file<P: AsRef<Path>>(&self, path: P) -> Result<InstrumentedDmaFile<I>> {
        let file = self.dir.create_file(&path).await.unwrap();

        let path = file.path().unwrap().to_owned();
        self.instrument
            .apply_event(Event::Create(path.clone()))
            .unwrap();
        self.instrument
            .apply_event(Event::Open(path, file.as_raw_fd()))
            .unwrap();

        Ok(InstrumentedDmaFile {
            file,
            instrument: self.instrument.clone(),
        })
    }

    pub fn sync_read_dir(&self) -> Result<std::fs::ReadDir> {
        self.dir.sync_read_dir()
    }

    pub async fn sync(&self) -> Result<()> {
        self.instrument
            .apply_event(Event::Fdatasync(self.dir.as_raw_fd()))
            .unwrap();

        self.dir.sync().await
    }

    pub async fn close(self) -> Result<()> {
        self.instrument
            .apply_event(Event::Close(self.dir.as_raw_fd()))
            .unwrap();

        self.dir.close().await
    }

    pub fn path(&self) -> Option<Ref<'_, Path>> {
        self.dir.path()
    }
}

pub struct InstrumentedDmaFile<I: Instrument + Clone> {
    file: DmaFile,
    instrument: I,
}

impl<I: Instrument + Clone> InstrumentedDmaFile<I> {
    pub fn is_same(&self, other: &InstrumentedDmaFile<I>) -> bool {
        self.file.is_same(&other.file)
    }

    pub fn alloc_dma_buffer(&self, size: usize) -> DmaBuffer {
        self.file.alloc_dma_buffer(size)
    }

    pub async fn create<P: AsRef<Path>>(path: P, instrument: I) -> Result<InstrumentedDmaFile<I>> {
        let file = DmaFile::create(path).await?;

        let path = file.path().unwrap().to_owned();
        instrument.apply_event(Event::Create(path.clone())).unwrap();
        instrument
            .apply_event(Event::Open(path, file.as_raw_fd()))
            .unwrap();

        Ok(InstrumentedDmaFile { file, instrument })
    }

    pub async fn open<P: AsRef<Path>>(path: P, instrument: I) -> Result<InstrumentedDmaFile<I>> {
        let file = DmaFile::open(&path).await?;

        instrument
            .apply_event(Event::Open(path.as_ref().to_owned(), file.as_raw_fd()))
            .unwrap();

        Ok(InstrumentedDmaFile { file, instrument })
    }

    pub async fn write_at(&self, buf: DmaBuffer, pos: u64) -> Result<usize> {
        // TODO 2 step write
        self.instrument
            .apply_event(Event::Write(WriteEvent {
                fd: self.file.as_raw_fd(),
                file_range: FileRange::from_pos_and_buf_len(pos, buf.len() as u64),
            }))
            .unwrap();

        self.file.write_at(buf, pos).await
    }

    // No need to instrument reads unless we want to warn about dirty reads or inject failures in to reads

    pub async fn read_at_aligned(&self, pos: u64, size: usize) -> Result<ReadResult> {
        self.file.read_at_aligned(pos, size).await
    }

    pub async fn read_at(&self, pos: u64, size: usize) -> Result<ReadResult> {
        self.file.read_at(pos, size).await
    }

    // ScheduledSource is not exported in glommio/src/io/mod.rs
    // pub fn read_many<V, S>(
    //     self: &Rc<DmaFile>,
    //     iovs: S,
    //     buffer_limit: MergedBufferLimit,
    //     read_amp_limit: ReadAmplificationLimit,
    // ) -> ReadManyResult<V, impl Stream<Item = (ScheduledSource, ReadManyArgs<V>)>>
    // where
    //     V: IoVec + Unpin,
    //     S: Stream<Item = V> + Unpin {
    //     }

    pub async fn fdatasync(&self) -> Result<()> {
        self.instrument
            .apply_event(Event::Fdatasync(self.file.as_raw_fd()))
            .unwrap();

        self.file.fdatasync().await
    }

    pub async fn pre_allocate(&self, size: u64) -> Result<()> {
        self.file.pre_allocate(size).await
    }

    pub async fn hint_extent_size(&self, size: usize) -> Result<i32> {
        self.file.hint_extent_size(size).await
    }

    pub async fn truncate(&self, size: u64) -> Result<()> {
        self.instrument
            .apply_event(Event::SetLen(self.file.as_raw_fd(), size))
            .unwrap();

        self.file.truncate(size).await
    }

    pub async fn rename<P: AsRef<Path>>(&self, new_path: P) -> Result<()> {
        self.instrument
            .apply_event(Event::Rename {
                from: EitherPathOrFd::Fd(self.file.as_raw_fd()),
                to: new_path.as_ref().to_owned(),
            })
            .unwrap();

        self.file.rename(new_path).await
    }

    pub async fn remove(&self) -> Result<()> {
        self.file.remove().await
    }

    pub async fn file_size(&self) -> Result<u64> {
        self.file.file_size().await
    }

    pub async fn close(self) -> Result<()> {
        self.file.close().await
    }

    pub fn path(&self) -> Option<Ref<'_, Path>> {
        self.file.path()
    }

    pub async fn close_rc(self: Rc<InstrumentedDmaFile<I>>) -> Result<CloseResult> {
        match Rc::try_unwrap(self) {
            Err(_) => Ok(CloseResult::Unreferenced),
            Ok(file) => file.close().await.map(|_| CloseResult::Closed),
        }
    }

    pub fn ensure_durable(&self, up_to: Option<u64>) {
        let r = self.instrument.apply_event(Event::EnsureFileDurable {
            target: EitherPathOrFd::Fd(self.file.as_raw_fd()),
            up_to,
        });

        if let Err(e) = r {
            eprintln!("Error: {}", e);
            panic!("Durability expectation violation")
        }
    }
}
