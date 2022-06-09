#![feature(generic_associated_types)] // for usage of generic lifetime parameters on associated Future types
#![feature(drain_filter)]

pub mod reader;
pub mod writer;

use self::reader::WalReadError;

const DEBUG: bool = false;

macro_rules! debug_print {
    () => {
        if crate::DEBUG {
            println!()
        }
    };
    ($($arg:tt)*) => {{
        if crate::DEBUG {
            println!($($arg)*);
        }
    }};
}

pub(crate) use debug_print;

pub trait WalWritable<'rec> {
    fn size(&self) -> u64;
    fn serialize_into(&self, buf: &mut [u8]);
    fn deserialize_from<'buf>(buf: &'buf [u8]) -> Result<(u64, Self), WalReadError>
    where
        Self: Sized,
        'buf: 'rec;
}

#[derive(Copy, Clone)]
#[repr(u8)]
pub enum RecordMarker {
    Data = 1, // do not use zero as a marker to avoid confusion with uninitialized data
    Padding = 2,
    Shutdown = 3, // shutdown is intended as a marker of clean shutdown, if there is no shutdown marker, it is an abnormal shutdown
}

impl RecordMarker {
    pub fn try_from_u8(v: u8, offset_for_error: u64) -> Result<Self, WalReadError> {
        println!();
        match v {
            x if x == RecordMarker::Data as u8 => Ok(RecordMarker::Data),
            x if x == RecordMarker::Padding as u8 => Ok(RecordMarker::Padding),
            x if x == RecordMarker::Shutdown as u8 => Ok(RecordMarker::Shutdown),
            invalid => Err(WalReadError::InvalidRecordMarker {
                invalid,
                offset: offset_for_error,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        reader::{checksummed_read_buf, read_buf, WalConsumer, WalReadError, WalReadResult},
        WalWritable,
    };
    use crate::{reader::WalReader, writer::WalWriter};
    use crc32fast::Hasher;
    use futures::{future::join_all, Future};
    use glommio::{io::Directory, LocalExecutor};
    use std::{
        cell::RefCell, convert::TryInto, fmt::Display, fs, io::Write, mem, path::PathBuf, rc::Rc,
        str::FromStr, task::Poll, time::Duration,
    };

    const RECORD_HEADER_SIZE: u64 = (mem::size_of::<u64>() * 2) as u64;
    const SIZE_OF_SIZE: u64 = mem::size_of::<u64>() as u64;
    const CHECKSUM_SIZE: u64 = mem::size_of::<u32>() as u64;

    pub struct KVWalRecord<'rec> {
        key: &'rec [u8],
        value: &'rec [u8],
    }

    impl<'rec> WalWritable<'rec> for KVWalRecord<'rec> {
        fn size(&self) -> u64 {
            RECORD_HEADER_SIZE + (self.key.len() + self.value.len()) as u64 + CHECKSUM_SIZE
        }

        fn serialize_into(&self, mut buf: &mut [u8]) {
            let initial_pos = buf.len();

            let mut checksum_hasher = Hasher::new();
            // unwraps are ok? since caller already checked that there is enough space
            let key_len = (self.key.len() as u64).to_be_bytes();
            buf.write_all(&key_len).unwrap();

            let value_len = (self.value.len() as u64).to_be_bytes();
            buf.write_all(&value_len).unwrap();

            buf.write_all(self.key).unwrap();
            buf.write_all(self.value).unwrap();

            checksum_hasher.update(&key_len);
            checksum_hasher.update(&value_len);

            checksum_hasher.update(self.key);
            checksum_hasher.update(self.value);
            buf.write_all(&checksum_hasher.finalize().to_be_bytes())
                .unwrap();
            debug_assert_eq!(initial_pos - buf.len(), self.size() as usize);
            println!("record {:?} serialized", self.key[0]);
        }

        fn deserialize_from<'buf>(buf: &'buf [u8]) -> Result<(u64, Self), WalReadError>
        where
            Self: Sized,
            'buf: 'rec,
        {
            let mut checksum_hasher = Hasher::new();
            let mut pos = 0;
            let key_size = u64::from_be_bytes(
                checksummed_read_buf(
                    &buf,
                    &mut checksum_hasher,
                    pos as usize..(pos + SIZE_OF_SIZE) as usize,
                )?
                .try_into()?,
            );
            pos += SIZE_OF_SIZE;

            let value_size = u64::from_be_bytes(
                checksummed_read_buf(
                    &buf,
                    &mut checksum_hasher,
                    pos as usize..(pos + SIZE_OF_SIZE) as usize,
                )?
                .try_into()?,
            );
            pos += SIZE_OF_SIZE;

            let key = checksummed_read_buf(
                &buf,
                &mut checksum_hasher,
                pos as usize..(pos + key_size) as usize,
            )?;
            pos += key_size;

            let value = checksummed_read_buf(
                &buf,
                &mut checksum_hasher,
                pos as usize..(pos + value_size) as usize,
            )?;
            pos += value_size;

            let actual_checksum = u32::from_be_bytes(
                read_buf(&buf, pos as usize..(pos + CHECKSUM_SIZE) as usize)?.try_into()?,
            );
            let expected_checksum = checksum_hasher.finalize();
            if actual_checksum != expected_checksum {
                Err(WalReadError::ChecksumMismatch {
                    expected: expected_checksum,
                    actual: actual_checksum,
                })?;
            }
            pos += CHECKSUM_SIZE;
            let rec = KVWalRecord { key, value };
            Ok((pos, rec))
        }
    }

    impl Display for KVWalRecord<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!("record.key[0] {}", self.key[0]))
        }
    }

    async fn test_dir(test_name: &str) -> (Directory, PathBuf) {
        let mut path = PathBuf::from_str(env!("CARGO_MANIFEST_DIR")).unwrap();
        // let mut path = PathBuf::from_str("/tmp").unwrap();
        path.push("test_data");
        path.push(test_name);
        path.push("wal_segments");

        // consume value so there is no warning about must_use
        // glommio's directory doesnt have rm method, so let it be as it is
        fs::remove_dir_all(&path).ok();

        // and there is no create dir all in glommio too
        fs::create_dir_all(&path).expect("failed to create wal dir");
        let dir = Directory::open(&path)
            .await
            .expect("failed to create wal dir for test directory");
        dir.sync().await.expect("failed to sync wal dir");
        (dir, path)
    }

    #[derive(Default)]
    struct StubConsumer {
        data: Rc<RefCell<Vec<(Vec<u8>, Vec<u8>)>>>,
    }

    struct StubConsumeFut<'a> {
        data: Rc<RefCell<Vec<(Vec<u8>, Vec<u8>)>>>,
        record: KVWalRecord<'a>,
    }

    impl<'a> Future for StubConsumeFut<'a> {
        type Output = WalReadResult<()>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            println!("consuming key {}", self.record.key[0]);
            (&mut *self.data.borrow_mut())
                .push((self.record.key.to_owned(), self.record.value.to_owned()));
            Poll::Ready(Ok(()))
        }
    }

    impl WalConsumer for StubConsumer {
        type Record<'a> = KVWalRecord<'a>;
        type ConsumeFut<'a> = StubConsumeFut<'a>;

        fn consume<'a>(&self, record: Self::Record<'a>) -> Self::ConsumeFut<'a> {
            StubConsumeFut {
                data: Rc::clone(&self.data),
                record,
            }
        }
    }

    // TODO accept writer, key size, value size, number of records to write
    // fn write_wal() {
    //
    // }

    #[test]
    fn read_write_segment() {
        let ex = LocalExecutor::default();
        ex.run(async move {
            let (wal_dir, wal_dir_path) = test_dir("read_write_segment").await;

            // FIXME there is and error when buffers for read write have different sizes
            // e.g. 1 << 10 read and 512 << 10 write
            // this can be fixed by writing a padding size after padding marker
            // (because now it assumes that padding lasts until buffer end and this is different with different buf size)
            let buf_size = 1 << 10;
            let buf_num = 3;
            let segment_size = 512 << 20;
            let wal_segment_writer = WalWriter::new(
                wal_dir.try_clone().unwrap(),
                wal_dir_path,
                None,
                None,
                buf_size,
                buf_num,
                segment_size,
            )
            .await
            .expect("failed to create wal writer");
            let mut join_handles = vec![];

            let records = 100;

            for i in 0u64..records {
                let cloned = wal_segment_writer.clone();
                let jh = glommio::spawn_local(async move {
                    let key = vec![i as u8; 32];
                    let value = vec![i as u8; 32];

                    let record = KVWalRecord {
                        key: &key,
                        value: &value,
                    };
                    cloned.write(record).await.unwrap();
                })
                .detach();
                join_handles.push(jh);
            }
            let jh = glommio::spawn_local(async move {
                join_all(join_handles.into_iter()).await;
            })
            .detach();
            glommio::yield_if_needed().await; // take time to poll once write futures
            let flushed = wal_segment_writer.flush().await.unwrap();
            assert_eq!(flushed, records);
            jh.await;
            let wal_reader = WalReader::new(buf_size, wal_dir.try_clone().unwrap());
            let (read_finish_result, consumer) = wal_reader
                .pipe_to_consumer(StubConsumer::default())
                .await
                .expect("failed to pipe to consumer");

            let read_finish_result = read_finish_result.expect("read failed");
            assert_eq!(
                read_finish_result.last_segment.file_name().unwrap(),
                "0.wal"
            );
            assert_eq!(read_finish_result.last_segment_pos, 8532);

            assert!(consumer.data.borrow().len() > 0);
            assert_eq!(consumer.data.borrow().len() as u64, records);
            for (idx, (k, v)) in consumer.data.borrow().iter().enumerate() {
                assert_eq!(k, &vec![idx as u8; 32]);
                assert_eq!(v, &vec![idx as u8; 32]);
            }
        });
    }

    #[test]
    fn read_empty_segment() {
        let buf_size = 1 << 10;
        let ex = LocalExecutor::default();
        ex.run(async move {
            // no wal files in waldir
            let (wal_dir, _) = test_dir("read_empty_segment").await;
            let wal_reader = WalReader::new(buf_size, wal_dir.try_clone().unwrap());
            let (read_finish_result, consumer) = wal_reader
                .pipe_to_consumer(StubConsumer::default())
                .await
                .expect("failed to pipe to consumer");
            assert!(read_finish_result.is_none());
            assert!(consumer.data.borrow().len() == 0);

            // one empty file in waldir
            wal_dir.create_file("0.wal").await.unwrap();

            let wal_reader = WalReader::new(buf_size, wal_dir.try_clone().unwrap());
            let (read_finish_result, consumer) = wal_reader
                .pipe_to_consumer(StubConsumer::default())
                .await
                .expect("failed to pipe to consumer");
            assert!(read_finish_result.is_some());
            let read_finish_result = read_finish_result.unwrap();
            assert_eq!(
                read_finish_result.last_segment.file_name().unwrap(),
                "0.wal"
            );
            assert_eq!(read_finish_result.last_segment_pos, 0);

            assert!(consumer.data.borrow().len() == 0);
        });
    }

    #[test]
    fn read_write_many_segments() {
        let ex = LocalExecutor::default();
        ex.run(async move {
            let (wal_dir, wal_dir_path) = test_dir("read_write_many_segments").await;

            // FIXME there is and error when buffers for read write have different sizes
            // e.g. 1 << 10 read and 512 << 10 write
            // this can be fixed by writing a padding size after padding marker
            // (because now it assumes that padding lasts until buffer end and this is different with different buf size)
            let buf_size = 512;
            let buf_num = 3;
            let segment_size = 2048; // let 4 buffers fit
            let wal_segment_writer = WalWriter::new(
                wal_dir.try_clone().unwrap(),
                wal_dir_path,
                None,
                None,
                buf_size,
                buf_num,
                segment_size,
            )
            .await
            .expect("failed to create wal writer");
            let mut join_handles = vec![];

            let records = 100;

            for i in 0u64..records {
                let cloned = wal_segment_writer.clone();
                let jh = glommio::spawn_local(async move {
                    let key = vec![i as u8; 32];
                    let value = vec![i as u8; 32];

                    let record = KVWalRecord {
                        key: &key,
                        value: &value,
                    };
                    cloned
                        .write(record)
                        .await
                        .expect("failed to write wal record");
                })
                .detach();
                join_handles.push(jh);
            }
            let jh = glommio::spawn_local(async move {
                join_all(join_handles.into_iter()).await;
            })
            .detach();
            // wait for segment switch to happen and corresponding writes to complete
            glommio::timer::sleep(Duration::from_millis(100)).await;
            let _ = wal_segment_writer.flush().await.unwrap();
            // assert_eq!(flushed, records);
            jh.await;
            println!("-------------------------------------------------");
            let wal_reader = WalReader::new(buf_size, wal_dir.try_clone().unwrap());
            let (read_finish_result, consumer) = wal_reader
                .pipe_to_consumer(StubConsumer::default())
                .await
                .expect("failed to pipe to consumer");

            let read_finish_result = read_finish_result.expect("read failed");
            assert_eq!(
                read_finish_result.last_segment.file_name().unwrap(),
                "4.wal"
            );
            // assert_eq!(read_finish_result.last_segment_pos, 8532);

            // TODO check how many segments are in wal_dir

            assert!(consumer.data.borrow().len() > 0);
            // assert_eq!(consumer.data.borrow().len() as u64, records);
            for (idx, (k, v)) in consumer.data.borrow().iter().enumerate() {
                assert_eq!(k, &vec![idx as u8; 32], "key data mismatch at {idx}");
                assert_eq!(v, &vec![idx as u8; 32], "value data mismatch at {idx}");
            }
        });
    }

    // TODO test write, shutdown, continue repeatedly
}
