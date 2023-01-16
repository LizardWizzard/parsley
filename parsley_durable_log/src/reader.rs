use std::{array::TryFromSliceError, fmt::Debug, future::Future, io, path::PathBuf, rc::Rc};

use super::{LogWritable, RecordMarker};
use glommio::GlommioError;
use instrument_fs::{
    adapter::glommio::{InstrumentedDirectory, InstrumentedDmaFile},
    Instrument,
};
use parsley_io_util::UnexpectedEofError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WalReadError {
    #[error("Corrupted segment: {0}")]
    UnexpectedEof(#[from] UnexpectedEofError),

    #[error("Corrupted segment, unexpected eof (length decode failed)")]
    LengthDecodeFailed(#[from] TryFromSliceError),

    #[error("Record checksum mismatch. Expected {expected:x?} found {actual:x?}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("Unknown record marker {invalid} at offset {offset}")]
    InvalidRecordMarker { invalid: u8, offset: u64 },

    #[error("Invalid log file name at: {at}")]
    InvalidLogFileName { at: PathBuf },

    #[error("Glommio error")]
    Glommio(#[from] GlommioError<()>),

    #[error("Io error")]
    Io(#[from] io::Error),
}

pub type WalReadResult<T> = Result<T, WalReadError>;

pub trait WalConsumer {
    type Record<'a>: LogWritable<'a>;
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
pub(crate) struct WalSegmentStreamReader<Consumer: WalConsumer, I: Instrument + Clone> {
    dma_file: Rc<InstrumentedDmaFile<I>>,
    buf_size: u64,
    file_pos: u64,
    consumer: Consumer,
}

impl<Consumer: WalConsumer, I: Instrument + Clone> WalSegmentStreamReader<Consumer, I> {
    pub(crate) fn new(dma_file: InstrumentedDmaFile<I>, buf_size: u64, consumer: Consumer) -> Self {
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
        let mut buf_no = 0;
        loop {
            crate::debug_print!("reading buf_no {buf_no}");
            // position inside current buffer
            let mut buf_pos = 0;
            // reading from file at a current offset
            // NOTE read error is fatal, so it should interrupt reading so we propagate it up
            let read_result = self
                .dma_file
                .read_at_aligned(self.file_pos, self.buf_size as usize)
                .await?;
            buf_no += 1;
            // TODO deserialize from are not fatal, they should be forwarded up to the segment iterator
            // because only it can decide, if there are more segments it is fatal,
            // if this is the last segment this is just abnormal shutdown and we can continue writing
            if read_result.is_empty() {
                return Ok((self.file_pos, Ok(())));
            }

            while buf_pos < (read_result.len() - 1) as u64 {
                match RecordMarker::try_from_u8(
                    read_result[buf_pos as usize],
                    self.file_pos + buf_pos,
                ) {
                    Ok(RecordMarker::Data) => {
                        crate::debug_print!(
                            "data at file_pos {} buf_pos {} absolute_pos {}",
                            self.file_pos,
                            buf_pos,
                            self.file_pos + buf_pos
                        );
                        // consume marker
                        buf_pos += 1;

                        // If checksum validation is failed we must return self.file_pos + buf_pos without counting the marker.
                        // Because deserialization error is fatal only in case this segment is not the latest
                        // one we propagate it up to the segment iterator so it can decide.
                        // Otherwise deserialization error is not fatal and means abnormal shutdown
                        // so writer can continue writing at self.file_pos + buf_pos
                        match Consumer::Record::<'_>::decode_from(&read_result[buf_pos as usize..])
                        {
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
                    Ok(RecordMarker::Padding) => {
                        crate::debug_print!(
                            "padding at file_pos {} buf_pos {} absolute_pos {}",
                            self.file_pos,
                            buf_pos,
                            self.file_pos + buf_pos
                        );
                        // FIXME if buffers size when reading is different from the one used for writing
                        // paddings screw it up because padding means empty until buf size.
                        // Two options, write padding size, or have write buf size in wal segment header
                        // Or have WalConfig with number of segments per file and a segment size, and use segment size for buffer size
                        // (might be inefficient for writing because if buffer is large flush cost can be high)
                        break; // break the while loop
                    }
                    Ok(RecordMarker::Shutdown) => {
                        crate::debug_print!(
                            "shutdown at file_pos {} buf_pos {} absolute_pos {}",
                            self.file_pos,
                            buf_pos,
                            self.file_pos + buf_pos
                        );
                        // TODO? technically we can avoid this shutdown marker
                        // because probably there won't be any segments after this one with shutdown record
                        // but in case of a bug we can detect the case when there are files which contain data after shutdown record
                        // this also assumes that shutdown record is overwritten during startup

                        // do not increment to overwrite shutdown marker
                        return Ok((self.file_pos + buf_pos, Ok(())));
                    }
                    Err(e) => {
                        // we reached data that does not match marker
                        // it either means corruption if this is not the last
                        // segment, or it is the tip of the log
                        // forward error up to make the decision
                        return Ok((self.file_pos + buf_pos, Err(e)));
                    }
                }
            }

            self.file_pos += self.buf_size;
        }
    }
}

pub struct WalReader<I: Instrument + Clone> {
    buf_size: u64,
    wal_dir: InstrumentedDirectory<I>,
    instrument: I,
}

impl<I: Instrument + Clone> WalReader<I> {
    pub fn new(buf_size: u64, wal_dir: InstrumentedDirectory<I>, instrument: I) -> Self {
        Self {
            buf_size,
            wal_dir,
            instrument,
        }
    }

    fn get_segment_paths(&self) -> WalReadResult<Vec<(usize, PathBuf)>> {
        let mut segment_files = vec![];
        for segment_file_dir_entry in self.wal_dir.sync_read_dir()? {
            let segment_file_dir_entry = segment_file_dir_entry?;
            let err = WalReadError::InvalidLogFileName {
                at: segment_file_dir_entry.path(),
            };
            let segno = match segment_file_dir_entry
                .path()
                .file_stem()
                .map(|s| s.to_string_lossy())
                .map(|s| s.parse::<usize>())
            {
                Some(r) => r.map_err(|_| err),
                None => Err(err),
            }?;

            segment_files.push((segno, segment_file_dir_entry.path()));
        }
        segment_files.sort_by_key(|(segno, _)| *segno);

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
        for (segno, segment_path) in segment_paths.iter() {
            crate::debug_print!("reading {}", segment_path.display());
            let segment_file =
                InstrumentedDmaFile::open(&segment_path, self.instrument.clone()).await?;

            let segment_reader =
                WalSegmentStreamReader::new(segment_file, self.buf_size, consumer.take().unwrap());
            let ((new_stop_pos, new_consumer), result) = segment_reader.pipe_to_consumer().await?;

            // we encountered an error for segment other than the last one
            // this shouldnt happen
            if result.is_err() && *segno != segment_paths.len() - 1 {
                return Err(result.unwrap_err());
            }

            consumer = Some(new_consumer);
            stop_pos = new_stop_pos;
        }

        let consumer = consumer.take().unwrap();
        Ok((
            Some(ReadFinishResult {
                last_segment: segment_paths.last().unwrap().1.to_path_buf(),
                last_segment_pos: stop_pos,
            }),
            consumer,
        ))
        // do not forget to adjust if there is a shutdown marker
    }
}
