use std::{
    fs::File,
    io::{self, Write},
    time::Instant,
};

use histogram::Histogram;
use parsley_durable_log::test_utils::{self, bench::display_histogram};

const BUF_SIZE: usize = 8192;
// const FILE_SIZE: usize = 1 << 30; // 1GB
const FILE_SIZE: usize = 10 << 20; // 10MB

fn main() -> io::Result<()> {
    let path = test_utils::test_dir("bench_std_write_page_fsync");
    let mut file = File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path.join("test"))?;

    file.set_len(FILE_SIZE as u64)?;
    file.sync_all()?;

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
        file.sync_data()?;
        h.increment(write_t0.elapsed().as_micros().try_into().unwrap())
            .unwrap();
    }
    println!("elapsed={:?}", t0.elapsed());
    display_histogram("write", h, true);

    Ok(())
}
