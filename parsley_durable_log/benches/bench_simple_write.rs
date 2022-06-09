use std::{
    fmt::Display,
    fs,
    io::Write,
    mem,
    path::PathBuf,
    str::FromStr,
    time::{Duration, Instant},
};

use crc32fast::Hasher;
use futures::future::join_all;
use glommio::{io::Directory, LocalExecutor};
use parsley_durable_log::{
    reader::{checksummed_read_buf, read_buf, WalReadError},
    writer::WalWriter,
    WalError, WalWritable,
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

// TODO test without segment switches, large segment
// test with segment switches
// TODO validate after write,
// TODO play with task queues with different priorities, experiment with different latency
// TODO create multiple executors?

fn prepare_test_dir() -> PathBuf {
    let mut path = PathBuf::from_str(env!("CARGO_MANIFEST_DIR")).unwrap();
    // let mut path = PathBuf::from_str("/tmp").unwrap();
    path.push("test_data");
    path.push("bench_simple_write");

    // consume value so there is no warning about must_use
    // glommio's directory doesnt have rm method, so let it be as it is
    fs::remove_dir_all(&path).ok();

    // and there is no create dir all in glommio too
    fs::create_dir_all(&path).expect("failed to create wal dir");

    path
}

fn get_record<'a>(key: &'a mut [u8], value: &'a [u8], record_no: u64) -> KVWalRecord<'a> {
    key[..8].copy_from_slice(&record_no.to_le_bytes());
    KVWalRecord { key, value }
}

async fn writer(
    wal_writer: WalWriter,
    writer_id: usize,
    num_records: u64,
    key_size: u64,
    value_size: u64,
) {
    let mut key = vec![0u8; key_size as usize];
    let mut value = vec![0u8; value_size as usize];
    value[..8].copy_from_slice(&writer_id.to_le_bytes());

    for record_no in 0..num_records {
        let record = get_record(&mut key, &value, record_no);
        wal_writer.write(record).await.unwrap(); // TODO error
    }
}

async fn flusher(wal_writer: WalWriter, flush_interval: Duration) {
    let mut flushed_total = 0;
    loop {
        glommio::timer::sleep(flush_interval).await;

        match wal_writer.flush().await {
            Ok(flushed) => {
                // println!("flushed records {} {}", flushed, flushed_total);
                flushed_total += flushed
            }
            Err(e) => match e {
                WalError::FlushIsAlreadyInProgress => {
                    println!("got flush already in progress, continueing");
                    continue;
                }
                other_error => panic!("failed to flush log: {}", other_error),
            },
        }
    }
}

fn display_bytes(size: u64) -> String {
    let mut size = size;
    let mut rem = 0;
    let mut variant_idx = 0;
    let variants = ["b", "Kb", "Mb", "Gb"];
    while size > 1024 && variant_idx <= variants.len() {
        size = size / 1024;
        rem = size % 1024;
        variant_idx += 1;
    }
    format!("{}.{} {}", size, rem, variants[variant_idx])
}

fn main() {
    let buf_size_bytes = 8192; // should be a multiple of 512
    let buf_queue_size = 3;
    let segment_size_bytes = 10 << 26;
    let num_writers = 2048;
    let num_records_per_writer = 512;
    let key_size = 100;
    let value_size = 100;
    let flush_interval = Duration::from_micros(100);

    let record_size = key_size + value_size + RECORD_HEADER_SIZE + CHECKSUM_SIZE;
    let num_records_total = num_writers as u64 * num_records_per_writer;
    println!("segment size {}", display_bytes(segment_size_bytes));
    println!(
        "about to write {} bytes from {}",
        display_bytes(num_records_total * record_size),
        num_records_total,
    );

    let target_dir_path = prepare_test_dir();

    let ex = LocalExecutor::default();
    ex.run(async move {
        let target_dir = Directory::open(&target_dir_path)
            .await
            .expect("failed to create wal dir for test directory");
        target_dir.sync().await.expect("failed to sync wal dir");

        let wal_writer = WalWriter::new(
            target_dir.try_clone().unwrap(),
            target_dir_path,
            None,
            None,
            buf_size_bytes,
            buf_queue_size,
            segment_size_bytes,
        )
        .await
        .expect("failed to create wal writer");

        let mut join_handles = vec![];
        let flusher_join_handle = glommio::spawn_local(flusher(wal_writer.clone(), flush_interval));

        for writer_id in 0..num_writers {
            let join_handle = glommio::spawn_local(writer(
                wal_writer.clone(),
                writer_id,
                num_records_per_writer,
                key_size,
                value_size,
            ));
            join_handles.push(join_handle);
        }
        let t0 = Instant::now();

        join_all(join_handles.into_iter()).await;
        let elapsed = t0.elapsed();
        println!(
            "num_recors_total / elapsed {:?}",
            elapsed / num_records_total as u32
        );
        println!(
            "num_recors / elapsed {:?}",
            elapsed / num_records_per_writer as u32
        );

        let h = wal_writer.write_distribution_histogram();
        for percentile in [90.0, 99.0, 99.9] {
            println!(
                "p{} {:?}",
                percentile,
                Duration::from_micros(h.percentile(percentile).unwrap())
            );
        }

        flusher_join_handle.cancel().await;
    });
}
