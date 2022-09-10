#![feature(generic_associated_types)] // for usage of generic lifetime parameters on associated Future types
#![feature(drain_filter)]

pub mod reader;
#[cfg(feature = "test_utils")]
pub mod test_utils;
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
    use super::reader::{WalConsumer, WalReadResult};
    use crate::{
        reader::WalReader,
        test_utils::{kv_record::KVWalRecord, test_dir_open},
        writer::WalWriter,
    };
    use futures::{future::join_all, Future};
    use glommio::LocalExecutor;
    use instrument_fs::instrument::durability_checker::DurabilityChecker;
    use std::{cell::RefCell, rc::Rc, task::Poll, time::Duration};

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
            // println!("consuming key {}", self.record.key[0]);
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
    // fn write_log() {
    //
    // }

    #[test]
    fn read_write_segment() {
        let ex = LocalExecutor::default();
        ex.run(async move {
            let instrument = DurabilityChecker::default();

            let (wal_dir, wal_dir_path) =
                test_dir_open("read_write_segment", instrument.clone()).await;

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

            let wal_reader =
                WalReader::new(buf_size, wal_dir.try_clone().unwrap(), instrument.clone());
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
            let instrument = DurabilityChecker::default();
            // no wal files in waldir
            let (wal_dir, _) = test_dir_open("read_empty_segment", instrument.clone()).await;

            let wal_reader =
                WalReader::new(buf_size, wal_dir.try_clone().unwrap(), instrument.clone());
            let (read_finish_result, consumer) = wal_reader
                .pipe_to_consumer(StubConsumer::default())
                .await
                .expect("failed to pipe to consumer");
            assert!(read_finish_result.is_none());
            assert!(consumer.data.borrow().len() == 0);

            // one empty file in waldir
            wal_dir.create_file("0.wal").await.unwrap();

            let wal_reader = WalReader::new(buf_size, wal_dir.try_clone().unwrap(), instrument);
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
            let instrument = DurabilityChecker::default();

            let (wal_dir, wal_dir_path) =
                test_dir_open("read_write_many_segments", instrument.clone()).await;

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

            jh.await;

            let wal_reader = WalReader::new(buf_size, wal_dir.try_clone().unwrap(), instrument);
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
