use std::time::{Duration, Instant};

use futures::future::join_all;
use glommio::{io::Directory, LocalExecutor};
use histogram::Histogram;
use parsley_durable_log::{
    test_utils::{
        kv_record::{KVWalRecord, CHECKSUM_SIZE, RECORD_HEADER_SIZE},
        test_dir,
    },
    writer::{WalWriteError, WalWriter},
};

// TODO test without segment switches, large segment
// test with segment switches
// TODO validate after write,
// TODO play with task queues with different priorities, experiment with different latency
// TODO create multiple executors?
// TODO accept jobfile and add visualization layer

fn get_record<'a>(key: &'a mut [u8], value: &'a [u8], record_no: u64) -> KVWalRecord<'a> {
    key[..8].copy_from_slice(&record_no.to_le_bytes());
    KVWalRecord { key, value }
}

async fn writer(
    wal_writer: WalWriter,
    writer_id: u64,
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
    // TODO make flushed a histogram
    let mut flushed_total = 0;
    loop {
        glommio::timer::sleep(flush_interval).await;

        match wal_writer.flush().await {
            Ok(flushed) => {
                // println!("flushed records {} {}", flushed, flushed_total);
                flushed_total += flushed
            }
            Err(e) => match e {
                WalWriteError::FlushIsAlreadyInProgress => {
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

fn display_histogram(h: Histogram) {
    println!("min: {:?}", Duration::from_micros(h.minimum().unwrap()));
    println!("max: {:?}", Duration::from_micros(h.maximum().unwrap()));
    println!("stddev: {:?}", Duration::from_micros(h.stddev().unwrap()));
    println!("mean: {:?}", Duration::from_micros(h.mean().unwrap()));
    for percentile in (0..95).step_by(5) {
        println!(
            "p{} {:?}",
            percentile,
            Duration::from_micros(h.percentile(percentile as f64).unwrap())
        );
    }
    println!(
        "p{} {:?}",
        99.9,
        Duration::from_micros(h.percentile(99.9 as f64).unwrap())
    );
}

// OBSERVATION:
//  the

fn main() {
    // let buf_size_bytes = 8192; // should be a multiple of 512
    let buf_size_bytes = 200 << 10;
    let buf_queue_size = 3;
    let segment_size_bytes = 1 << 30;
    let num_writers = 64;
    let key_size = 100;
    let value_size = 100;
    let flush_interval = Duration::from_micros(50);

    let target_size = 1 << 30;

    let record_size = key_size + value_size + RECORD_HEADER_SIZE + CHECKSUM_SIZE;
    let num_records_total = target_size / record_size;
    let num_records_per_writer = num_records_total / num_writers;

    println!("segment size {}", display_bytes(segment_size_bytes));
    println!(
        "about to write {} bytes from {} records",
        display_bytes(num_records_total * record_size),
        num_records_total,
    );

    let target_dir_path = test_dir("bench_simple_write");

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
            "num_recors_total / elapsed = {:?}",
            elapsed / num_records_total as u32
        );
        println!(
            "num_records_per_writer / elapsed = {:?}",
            elapsed / num_records_per_writer as u32
        );
        println!(
            "throughput num_records_total * record_size / elapsed = {}",
            display_bytes(num_records_total * record_size / elapsed.as_secs())
        );

        let h = wal_writer.write_distribution_histogram();
        println!("Write duration histo:");
        display_histogram(h);

        let h = wal_writer.flush_duration_histogram();
        println!("Flush duration histo:");
        display_histogram(h);

        flusher_join_handle.cancel().await;
    });
}
