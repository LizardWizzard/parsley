use std::{
    cell::RefCell,
    collections::{hash_map, HashMap},
    error::Error,
    rc::Rc,
    task::Poll,
    time::{Duration, Instant},
};

use futures::{future::join_all, Future};
use glommio::LocalExecutor;
use histogram::Histogram;
use instrument_fs::{adapter::glommio::InstrumentedDirectory, Noop};
use parsley_durable_log::{
    reader::{WalConsumer, WalReadResult, WalReader},
    writer::{WalWriteError, WalWriter},
};
use parsley_durable_log_bench::display_histogram;
use parsley_entry::Entry;
use test_utils::test_dir;

// TODO test without segment switches, large segment
// test with segment switches
// TODO validate after write,
// TODO play with task queues with different priorities, experiment with different latency
// TODO create multiple executors?
// TODO accept jobfile and add visualization layer

fn get_record<'a>(key: &'a mut [u8], value: &'a [u8], record_no: u64) -> Entry<'a> {
    key[..8].copy_from_slice(&record_no.to_le_bytes());
    Entry::new(key, value).expect("correct entry")
}

async fn writer(
    wal_writer: WalWriter<Noop>,
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
    wal_writer: WalWriter<Noop>,
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
                    eprintln!("got flush already in progress, continuing");
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
        let mut args = Args::default();
        if let Some(v) = pargs.opt_value_from_str("--buf-size-bytes")? {
            args.buf_size_bytes = v;
        }

        if let Some(v) = pargs.opt_value_from_str("--buf-queue-size")? {
            args.buf_queue_size = v;
        }

        if let Some(v) = pargs.opt_value_from_str("--segment-size-bytes")? {
            args.segment_size_bytes = v;
        }

        if let Some(v) = pargs.opt_value_from_str("--num-writers")? {
            args.num_writers = v;
        }

        if let Some(v) = pargs.opt_value_from_str("--key-size-bytes")? {
            args.key_size_bytes = v;
        }

        if let Some(v) = pargs.opt_value_from_str("--value-size-bytes")? {
            args.value_size_bytes = v;
        }

        if let Some(v) = pargs.opt_value_from_str("--flush-interval-micros")? {
            args.flush_interval_micros = Duration::from_micros(v);
        }

        if let Some(v) = pargs.opt_value_from_str("--target-write-size-bytes")? {
            args.target_write_size_bytes = v;
        }

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
            buf_size_bytes: 128 << 10,
            buf_queue_size: 3,
            segment_size_bytes: 10 << 30, // 10G
            num_writers: 1024,
            key_size_bytes: 100,
            value_size_bytes: 100,
            flush_interval_micros: Duration::from_micros(50),
            target_write_size_bytes: 10 << 30, // 10G
        }
    }
}

struct ValidateConsumerFut<'a> {
    data: Rc<RefCell<HashMap<u64, u64>>>,
    record: Entry<'a>,
}

impl<'a> Future for ValidateConsumerFut<'a> {
    type Output = WalReadResult<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let writer_id = u64::from_le_bytes(self.record.value()[..8].try_into().unwrap());
        let record_no = u64::from_le_bytes(self.record.key()[..8].try_into().unwrap());

        match self.data.borrow_mut().entry(writer_id) {
            hash_map::Entry::Occupied(mut o) => {
                let last_seen = o.get_mut();
                *last_seen += 1;
                // check that record ids for writers are strictly in order
                assert_eq!(*last_seen, record_no);
            }
            hash_map::Entry::Vacant(v) => {
                assert_eq!(record_no, 0); // we got first record
                v.insert(record_no);
            }
        }

        Poll::Ready(Ok(()))
    }
}

#[derive(Default)]
struct ValidateConsumer {
    pub data: Rc<RefCell<HashMap<u64, u64>>>, // writer_id -> max_seen_record_no
}

impl WalConsumer for ValidateConsumer {
    type Record<'a> = Entry<'a>;

    type ConsumeFut<'a> = ValidateConsumerFut<'a>;

    fn consume<'a>(&self, record: Self::Record<'a>) -> Self::ConsumeFut<'a> {
        ValidateConsumerFut {
            data: Rc::clone(&self.data),
            record,
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse()?;

    let record_size = args.key_size_bytes
        + args.value_size_bytes
        + parsley_entry::RECORD_HEADER_SIZE
        + parsley_entry::CHECKSUM_SIZE;
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

    eprintln!("records per writer {}", num_records_per_writer);

    let target_dir_path = test_dir("bench_simple_write");

    let ex = LocalExecutor::default();
    ex.run(async move {
        let target_dir = InstrumentedDirectory::open(&target_dir_path, Noop)
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
        display_histogram("write_duration", h, true);

        let h = wal_writer.flush_duration_histogram();
        eprintln!("Flush duration histo:");
        display_histogram("flush_duration", h, true);

        eprintln!("Flush records histo:");
        display_histogram("flush_records", flush_records_histo, false);

        eprintln!("Validating...");
        let reader = WalReader::new(args.buf_size_bytes, target_dir, Noop);
        let (read_finish_result, consumer) = reader
            .pipe_to_consumer(ValidateConsumer::default())
            .await
            .unwrap();

        let _ = read_finish_result.expect("read failed");

        for writer_id in 0..args.num_writers {
            let last_seen = consumer.data.borrow()[&writer_id];
            assert_eq!(last_seen, num_records_per_writer - 1) // record_id starts from 0
        }
        eprintln!("Ok")
    });
    Ok(())
}
