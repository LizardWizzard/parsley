use std::{
    array::TryFromSliceError, convert::TryFrom, future::Future, io, ops::Range, path::PathBuf,
    rc::Rc,
};

use super::{RecordMarker, WalWritable};
use crc32fast::Hasher;
use glommio::{
    io::{Directory, DmaFile},
    GlommioError,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WalReadError {
    #[error("corrupted segment, unexpected eof")]
    UnexpectedEof,

    #[error("corrupted segment, unexpected eof (length decode failed)")]
    LengthDecodeFailed(#[from] TryFromSliceError),

    #[error("segment checksum mismatch. expected {expected:x?} found {actual:x?}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("unknown record marker {invalid}")]
    InvalidRecordMarker { invalid: u8 },

    #[error("glommio error")]
    GlommioError(#[from] GlommioError<()>),

    #[error("io error")]
    IoError(#[from] io::Error),
}

pub type WalReadResult<T> = Result<T, WalReadError>;

pub fn read_buf(buf: &[u8], range: Range<usize>) -> WalReadResult<&[u8]> {
    let expected_len = range.len();
    let slice = &buf.get(range).ok_or(WalReadError::UnexpectedEof)?;
    if slice.len() != expected_len {
        Err(WalReadError::UnexpectedEof)?
    }
    Ok(slice)
}

// TODO customize io::Read
pub fn checksummed_read_buf<'a>(
    buf: &'a [u8],
    hasher: &mut Hasher,
    range: Range<usize>,
) -> WalReadResult<&'a [u8]> {
    let slice = read_buf(buf, range)?;
    hasher.update(slice);
    Ok(slice)
}

pub trait WalConsumer {
    type Record<'a>: WalWritable<'a>;
    type ConsumeFut<'a>: Future<Output = WalReadResult<()>>;

    fn consume<'a>(&self, record: Self::Record<'a>) -> Self::ConsumeFut<'a>;
}

#[derive(Debug)]
pub struct ReadFinishResult {
    // last segment being read
    pub last_segment: PathBuf,
    // position inside the last segment
    pub last_segment_pos: u64,
}

// cannot implement Stream because Stream::Item doesnt have a lifetime
// and even if it did, we cannot give away references to current buf
// TODO switch to using DmaStreamReader because it supports readahead
pub(crate) struct WalSegmentStreamReader<Consumer: WalConsumer> {
    dma_file: Rc<DmaFile>,
    buf_size: u64,
    file_pos: u64,
    consumer: Consumer,
}

impl<Consumer: WalConsumer> WalSegmentStreamReader<Consumer> {
    pub(crate) fn new(dma_file: DmaFile, buf_size: u64, consumer: Consumer) -> Self {
        Self {
            dma_file: Rc::new(dma_file),
            buf_size,
            file_pos: 0,
            consumer,
        }
    }

    pub(crate) async fn pipe_to_consumer(
        mut self,
    ) -> WalReadResult<((u64, Consumer), WalReadResult<()>)> {
        let dma_file = Rc::clone(&self.dma_file);
        let (stop_pos, result) = self.pipe_to_consumer_inner().await?;
        dma_file.close_rc().await?;
        Ok(((stop_pos, self.consumer), result))
    }

    // NOTE since for now records larger than buf are not supported this is simpler
    async fn pipe_to_consumer_inner(&mut self) -> WalReadResult<(u64, WalReadResult<()>)> {
        loop {
            // position inside current buffer
            let mut buf_pos = 0;
            // reading from file at a current offset
            // NOTE read error is fatal, so it should interrupt reading so we propagate it up
            let read_result = self
                .dma_file
                .read_at_aligned(self.file_pos, self.buf_size as usize)
                .await?;
            // TODO deserialize from are not fatal, they should be forwarded up to the segment iterator
            // because only it can decide, if there are more segments it is fatal,
            // if this is the last segment this is just abnormal shutdown and we can continue writing
            if read_result.is_empty() {
                return Ok((self.file_pos, Ok(())));
            }

            while buf_pos < (read_result.len() - 1) as u64 {
                match RecordMarker::try_from(read_result[buf_pos as usize])? {
                    RecordMarker::Data => {
                        dbg!("data at", self.file_pos + buf_pos);
                        // consume marker
                        buf_pos += 1;

                        // If checksum validation is failed we must return self.file_pos + buf_pos without counting the marker.
                        // Because deserialization error is fatal only in case this segment is not the latest
                        // one we propagate it up to the segment iterator so it can decide.
                        // Otherwise deserialization error is not fatal and means abnormal shutdown
                        // so writer can continue writing at self.file_pos + buf_pos
                        match Consumer::Record::<'_>::deserialize_from(
                            &read_result[buf_pos as usize..],
                        ) {
                            Ok((consumed_pos, record)) => {
                                buf_pos += consumed_pos;
                                self.consumer.consume(record).await?;
                            }
                            Err(e) => {
                                // note that - 1 for previuosly consumed marker
                                return Ok((self.file_pos + buf_pos - 1, Err(e)));
                            }
                        }
                    }
                    RecordMarker::Padding => {
                        dbg!("padding at", buf_pos);
                        // FIXME if buffers size when reading is different from the one used for writing
                        // paddings screw it up because padding means empty until buf size.
                        // Two options, write padding size, or have write buf size in wal segment header
                        // Or have WalConfig with number of segments per file and a segment size, and use segment size for buffer size
                        // (might be inefficient for writing because if buffer is large flush cost can be high)
                        break; // break the while loop
                    }
                    RecordMarker::Shutdown => {
                        dbg!("shutdown at", self.file_pos + buf_pos);
                        // TODO? technically we can avoid this shutdown marker
                        // because probably there won't be any segments after this one with shutdown record
                        // but in case of a bug we can detect the case when there are files which contain data after shutdown record
                        // this also assumes that shutdown record is overwritten during startup

                        // do not increment to overwrite shutdown marker
                        return Ok((self.file_pos + buf_pos, Ok(())));
                    }
                }
            }

            self.file_pos += self.buf_size;
        }
    }
}

pub struct WalReader {
    buf_size: u64,
    wal_dir: Directory,
}

impl WalReader {
    pub fn new(buf_size: u64, wal_dir: Directory) -> Self {
        Self { buf_size, wal_dir }
    }

    fn get_segment_paths(&self) -> WalReadResult<Vec<PathBuf>> {
        let mut segment_files = vec![];
        for segment_file_dir_entry in self.wal_dir.sync_read_dir()? {
            segment_files.push(segment_file_dir_entry?.path());
        }
        // TODO sort based on lsn in file name
        Ok(segment_files)
    }

    pub async fn pipe_to_consumer<Consumer: WalConsumer>(
        &self,
        consumer: Consumer,
    ) -> WalReadResult<(Option<ReadFinishResult>, Consumer)> {
        let segment_paths = self.get_segment_paths()?;
        if segment_paths.is_empty() {
            return Ok((None, consumer));
        }
        // do this trick to avoid use of moved value
        let mut consumer = Some(consumer);
        let mut stop_pos = 0;
        for (idx, segment_path) in segment_paths.iter().enumerate() {
            dbg!(&segment_path);
            let segment_file = DmaFile::open(&segment_path).await?;

            let segment_reader =
                WalSegmentStreamReader::new(segment_file, self.buf_size, consumer.take().unwrap());
            let ((new_stop_pos, new_consumer), result) = segment_reader.pipe_to_consumer().await?;

            // we encountered an error for segment other than the last one
            // this shouldnt happen
            if result.is_err() && idx != segment_paths.len() - 1 {
                return Err(result.unwrap_err());
            }

            consumer = Some(new_consumer);
            stop_pos = new_stop_pos;
        }

        let consumer = consumer.take().unwrap();
        Ok((
            Some(ReadFinishResult {
                last_segment: segment_paths.last().unwrap().to_path_buf(),
                last_segment_pos: stop_pos,
            }),
            consumer,
        ))
        // do not forget to adjust if there is a shutdown marker
    }
}
