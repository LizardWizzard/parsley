use std::{rc::Rc, time::Instant};

use futures::join;
use glommio::{io::Directory, GlommioError, LocalExecutor};
use histogram::Histogram;
use parsley_durable_log_bench::display_histogram;

const BUF_SIZE: usize = 8192;
// const FILE_SIZE: usize = 1 << 30; // 1GB
const FILE_SIZE: usize = 10 << 26; // 64MB

fn main() -> Result<(), glommio::GlommioError<()>> {
    let path = test_utils::test_dir("bench_glommio_write_page_fsync");
    println!("test_dir={}", path.display());

    let concurrent = std::env::var("CONCURRENT").is_ok();
    println!("concurrent={}", concurrent);
    let pre_allocate = std::env::var("PRE_ALLOCATE").is_ok();
    println!("pre_allocate={}", pre_allocate);

    let ex = LocalExecutor::default();
    ex.run(async move {
        let target_dir = Directory::open(&path)
            .await
            .expect("failed to create wal dir for test directory");
        target_dir.sync().await.expect("failed to sync wal dir");
        let file = Rc::new(target_dir.create_file("test").await?);
        if pre_allocate {
            file.pre_allocate(FILE_SIZE as u64, false).await?;
        }
        // file
        // file
        // 1G / buf size
        let iterations = FILE_SIZE / BUF_SIZE;
        let mut reference_buf = Vec::with_capacity(BUF_SIZE);
        for i in 0..BUF_SIZE {
            reference_buf.push((i % 255) as u8)
        }

        let t0 = Instant::now();
        let mut total = Histogram::new();
        let mut write = Histogram::new();
        let mut sync = Histogram::new();

        for iteration in 0..iterations {
            let write_t0 = Instant::now();
            let mut buf = file.alloc_dma_buffer(BUF_SIZE);
            let buf_mut = buf.as_bytes_mut();
            buf_mut.copy_from_slice(&reference_buf);

            buf_mut[..8].copy_from_slice(&iteration.to_le_bytes());
            if concurrent {
                let file_write = Rc::clone(&file);
                let jh_write = glommio::spawn_local(async move {
                    file_write
                        .write_at(buf, (iteration * BUF_SIZE) as u64)
                        .await
                        .unwrap()
                });
                let file_sync = Rc::clone(&file);
                let jh_sync =
                    glommio::spawn_local(async move { file_sync.fdatasync().await.unwrap() });
                join!(jh_write, jh_sync);
            } else {
                let raw_write_t0 = Instant::now();
                file.write_at(buf, (iteration * BUF_SIZE) as u64).await?;
                write
                    .increment(raw_write_t0.elapsed().as_micros().try_into().unwrap())
                    .unwrap();
                let raw_sync_t0 = Instant::now();
                file.fdatasync().await?;
                sync.increment(raw_sync_t0.elapsed().as_micros().try_into().unwrap())
                    .unwrap();
            }

            total
                .increment(write_t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
        println!("elapsed={:?}", t0.elapsed());
        display_histogram("total", total, true);
        display_histogram("write", write, true);
        display_histogram("sync", sync, true);
        Ok::<(), GlommioError<()>>(())
    })?;

    Ok(())
}
