use std::{
    rc::Rc,
    time::{Duration, Instant},
};

use futures::join;
use glommio::{io::Directory, GlommioError, LocalExecutor};
use histogram::Histogram;
use parsley_durable_log::test_utils::{self, bench::display_histogram};

const BUF_SIZE: usize = 8192;
const FILE_SIZE: usize = 1 << 30; // 1GB
                                  // const FILE_SIZE: usize = 10 << 20; // 10MB

fn main() -> Result<(), glommio::GlommioError<()>> {
    let path = test_utils::test_dir("bench_glommio_write_page_fsync");

    let concurrent = std::env::var("CONCURRENT").is_ok();
    println!("concurrent={}", concurrent);

    let ex = LocalExecutor::default();
    ex.run(async move {
        let target_dir = Directory::open(&path)
            .await
            .expect("failed to create wal dir for test directory");
        target_dir.sync().await.expect("failed to sync wal dir");
        let file = Rc::new(target_dir.create_file("test").await?);
        file.pre_allocate(FILE_SIZE as u64).await?;
        // file
        // 1G / buf size
        let iterations = FILE_SIZE / BUF_SIZE;
        let mut reference_buf = Vec::with_capacity(BUF_SIZE);
        for i in 0..BUF_SIZE {
            reference_buf.push((i % 255) as u8)
        }

        let t0 = Instant::now();
        let mut write_with_buf_alloc = Histogram::new();
        let mut write_without_buf_alloc = Histogram::new();

        for iteration in 0..iterations {
            let write_t0 = Instant::now();
            let mut buf = file.alloc_dma_buffer(BUF_SIZE);
            let buf_mut = buf.as_bytes_mut();
            buf_mut.copy_from_slice(&reference_buf);

            let write_t1 = Instant::now();
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
                file.write_at(buf, (iteration * BUF_SIZE) as u64).await?;
                file.fdatasync().await?;
            }

            write_with_buf_alloc
                .increment(write_t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
            write_without_buf_alloc
                .increment(write_t1.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
        println!("elapsed={:?}", t0.elapsed());
        display_histogram("write_without_buf_alloc", write_without_buf_alloc, |v| {
            format!("{:?}", Duration::from_micros(v))
        });
        display_histogram("write_with_buf_alloc", write_with_buf_alloc, |v| {
            format!("{:?}", Duration::from_micros(v))
        });
        Ok::<(), GlommioError<()>>(())
    })?;

    Ok(())
}
