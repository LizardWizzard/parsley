use std::{
    cell::RefCell,
    error::Error,
    rc::Rc,
    time::{Duration, Instant},
};

use futures::future::join_all;
use glommio::{io::Directory, LocalExecutor};
use histogram::Histogram;
use parsley_durable_log::{
    test_utils::{
        bench::display_histogram,
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

async fn flusher(
    wal_writer: WalWriter,
    flush_interval: Duration,
    should_stop: Rc<RefCell<bool>>,
) -> Histogram {
    let mut histo = Histogram::new();
    while !*should_stop.borrow() {
        glommio::timer::sleep(flush_interval).await;

        match wal_writer.flush().await {
            Ok(flushed) => {
                // println!("flushed records {}", flushed);
                // it is broken... always 1024
                histo.increment(flushed).unwrap()
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
    histo
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

struct Args {
    buf_size_bytes: u64,
    buf_queue_size: usize,
    segment_size_bytes: u64,
    num_writers: u64,
    key_size_bytes: u64,
    value_size_bytes: u64,
    flush_interval_micros: Duration,
    target_write_size_bytes: u64,
}

impl Args {
    fn parse() -> Result<Self, pico_args::Error> {
        let mut pargs = pico_args::Arguments::from_env();
        // TODO help
        // Help has a higher priority and should be handled separately.
        // if pargs.contains(["-h", "--help"]) {
        //     print!("{}", HELP);
        //     std::process::exit(0);
        // }
        let args = Args {
            buf_size_bytes: pargs.value_from_str("--buf-size-bytes")?,
            buf_queue_size: pargs.value_from_str("--buf-queue-size")?,
            segment_size_bytes: pargs.value_from_str("--segment-size-bytes")?,
            num_writers: pargs.value_from_str("--num-writers")?,
            key_size_bytes: pargs.value_from_str("--key-size-bytes")?,
            value_size_bytes: pargs.value_from_str("--value-size-bytes")?,
            flush_interval_micros: Duration::from_micros(
                pargs.value_from_str("--flush-interval-micros")?,
            ),
            target_write_size_bytes: pargs.value_from_str("--target-write-size-bytes")?,
        };

        // It's up to the caller what to do with the remaining arguments.
        let remaining = pargs.finish();
        if !remaining.is_empty() {
            eprintln!("Error: unknown arguments: {:?}.", remaining);
            std::process::exit(1)
        }
        Ok(args)
    }
}

impl Default for Args {
    fn default() -> Self {
        Self {
            buf_size_bytes: 200 << 10,
            buf_queue_size: 3,
            segment_size_bytes: 1 << 30, // 1G
            num_writers: 1024,
            key_size_bytes: 100,
            value_size_bytes: 100,
            flush_interval_micros: Duration::from_micros(50),
            target_write_size_bytes: 1 << 30, // 1G
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse()?;

    let record_size =
        args.key_size_bytes + args.value_size_bytes + RECORD_HEADER_SIZE + CHECKSUM_SIZE;
    let num_records_total = args.target_write_size_bytes / record_size;
    let num_records_per_writer = num_records_total / args.num_writers;

    eprintln!("segment size {}", display_bytes(args.segment_size_bytes));
    eprintln!(
        "about to write {} bytes from {} records",
        display_bytes(num_records_total * record_size),
        num_records_total,
    );
    eprintln!(
        "records fits one buffer {}",
        args.buf_size_bytes / record_size
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
            args.buf_size_bytes,
            args.buf_queue_size,
            args.segment_size_bytes,
        )
        .await
        .expect("failed to create wal writer");

        let mut join_handles = vec![];
        let flusher_should_stop = Rc::new(RefCell::new(false));
        let flusher_join_handle = glommio::spawn_local(flusher(
            wal_writer.clone(),
            args.flush_interval_micros,
            Rc::clone(&flusher_should_stop),
        ));

        for writer_id in 0..args.num_writers {
            let join_handle = glommio::spawn_local(writer(
                wal_writer.clone(),
                writer_id,
                num_records_per_writer,
                args.key_size_bytes,
                args.value_size_bytes,
            ));
            join_handles.push(join_handle);
        }
        let t0 = Instant::now();

        join_all(join_handles.into_iter()).await;
        let elapsed = t0.elapsed();
        *flusher_should_stop.borrow_mut() = true;
        let flush_records_histo = flusher_join_handle.await;
        eprintln!(
            "num_recors_total / elapsed = {:?}",
            elapsed / num_records_total as u32
        );
        eprintln!(
            "num_records_per_writer / elapsed = {:?}",
            elapsed / num_records_per_writer as u32
        );
        eprintln!(
            "throughput num_records_total * record_size / elapsed = {}",
            display_bytes(num_records_total * record_size / elapsed.as_secs())
        );

        let h = wal_writer.write_distribution_histogram();
        eprintln!("Write duration histo:");
        display_histogram("write_duration", h, |v| {
            format!("{:?}", Duration::from_micros(v))
        });

        let h = wal_writer.flush_duration_histogram();
        eprintln!("Flush duration histo:");
        display_histogram("flush_duration", h, |v| {
            format!("{:?}", Duration::from_micros(v))
        });

        eprintln!("Flush records histo:");
        display_histogram("flush_records", flush_records_histo, |v| v.to_string());
    });
    Ok(())
}
