use std::{
    fs::File,
    io::{self, Write},
    time::{Duration, Instant},
};

use histogram::Histogram;
use parsley_durable_log::test_utils;

const BUF_SIZE: usize = 8192;
const FILE_SIZE: usize = 1 << 30; // 1G

// TODO avoid code duplication
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

fn main() -> io::Result<()> {
    let path = test_utils::test_dir("bench_std_write_page_fsync");
    let mut file = File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path.join("test"))?;

    // 1G / buf size
    let iterations = FILE_SIZE / BUF_SIZE;
    let mut buf = Vec::with_capacity(BUF_SIZE);
    for i in 0..BUF_SIZE {
        buf.push((i % 255) as u8)
    }

    let t0 = Instant::now();
    let mut h = Histogram::new();

    for iteration in 0..iterations {
        let write_t0 = Instant::now();
        buf[..8].copy_from_slice(&iteration.to_le_bytes());
        file.write_all(&buf)?;
        file.sync_all()?;
        h.increment(write_t0.elapsed().as_micros().try_into().unwrap())
            .unwrap();
    }
    println!("elapsed: {:?}", t0.elapsed());
    println!("Write histo:");
    display_histogram(h);

    Ok(())
}
