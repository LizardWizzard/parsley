use std::{
    cell::{RefCell, RefMut},
    collections::VecDeque,
    fmt::Display,
    io::Write,
    mem,
    path::PathBuf,
    rc::Rc,
    task::{Poll, Waker},
    time::Instant,
};

use super::{RecordMarker, WalWritable};
use futures::future::{join_all, poll_fn};
use glommio::sync::RwLock;
use glommio::GlommioError;
use glommio::{io::DmaBuffer, task::JoinHandle};
use histogram::Histogram;
use instrument_fs::{
    adapter::glommio::{InstrumentedDirectory, InstrumentedDmaFile},
    Instrument,
};
use thiserror::Error;

fn wal_file_name(idx: u64) -> String {
    format!("{}.wal", idx)
}

// for now assume key value pair does not cross segment boundary, this may become useful for very large values, maybe support later
#[derive(Error, Debug)]
pub enum WalWriteError {
    #[error("record size exceeded size of one buffer, this is currently not supported")]
    RecordSizeLimitExceeded,

    #[error("task was closed")]
    TaskClosed,

    #[error("flush is already in progress")]
    FlushIsAlreadyInProgress,

    #[error("glommio error {0}")]
    GlommioError(#[from] GlommioError<()>),
}

pub type WalWriteResult<T> = Result<T, WalWriteError>;

enum WriteState {
    Pending(Waker),
    Ready,
}

struct WalSegmentWriterState<I: Instrument + Clone> {
    // This is the segment we are currently writing to
    segment_file: Rc<InstrumentedDmaFile<I>>,
    // These are wakers for tasks that called write and are waiting for the flush
    // u64 corrensponds to the position in the file which is needed to calculate
    // which writes are durable during flush. Flush operation is issued when
    // we are in a specific offset of the file, and when flush is completed
    // writes with lower offset are considered durable
    // TODO what about multiple segments and this u64? segment no should be included
    flush_waiter_wakers: Vec<(u64, WriteState)>,
    // these are handles to tasks which write filled buffers
    // FIXME we do nothing with pending tasks, we need to ensure that buffers scheduled
    // to write with position less than flush positon are successfully written
    // FIXME include segment no
    pending_writes: Vec<(u64, JoinHandle<WalWriteResult<()>>)>,
    // queue of allocated dma buffers (cannot be shared between different files, TODO find out why)
    // TODO make a task that fills the queue in the background
    // TODO do not fill with new buffers when segment is about to be finished (check if buffers can be reused between files)
    buf_queue: VecDeque<DmaBuffer>,
    // posision in the current buffer (buf_queue[0])
    buf_pos: u64,
    // position in the file, updated after buffer write is submitted
    file_pos: u64,
}

impl<I: Instrument + Clone> WalSegmentWriterState<I> {
    pub fn new_from_segment_file(
        segment_file: Rc<InstrumentedDmaFile<I>>,
        buf_size: u64,
        buf_num: usize,
    ) -> Self {
        // TODO check segsize vs buf_size * buf_num

        let mut buf_queue = VecDeque::with_capacity(buf_num);
        (0..buf_num)
            .into_iter()
            .map(|_| buf_queue.push_back(segment_file.alloc_dma_buffer(buf_size as usize)))
            .for_each(|_| {});

        WalSegmentWriterState {
            segment_file,
            flush_waiter_wakers: Vec::with_capacity(buf_num),
            buf_queue,
            buf_pos: 0,
            file_pos: 0,
            pending_writes: Vec::with_capacity(buf_num),
        }
    }

    pub fn write_record<'a, T: WalWritable<'a>>(&mut self, record: &T) {
        let buf = self.buf_queue[0].as_bytes_mut();
        buf[self.buf_pos as usize] = RecordMarker::Data as u8;
        self.buf_pos += 1;
        record.serialize_into(&mut buf[self.buf_pos as usize..]);
    }

    pub fn write_shutdown(&mut self) {
        let buf = self.buf_queue[0].as_bytes_mut();
        buf[self.buf_pos as usize] = RecordMarker::Shutdown as u8;
        self.buf_pos += 1;
    }
}

struct WalWriterState<I: Instrument + Clone> {
    wal_dir: InstrumentedDirectory<I>,
    wal_dir_path: PathBuf,
    buf_size: u64,
    buf_num: usize, // usize because used only to fill Vec with buffers which accepts usize
    segment_size: u64,
    current_segment_idx: u64,
    current_segment_writer_state: WalSegmentWriterState<I>,
    flush_is_in_progress: bool,
    has_unflushed_records: bool,
    switch_segments_lock: Rc<RwLock<()>>,
    write_duration_histogram: Histogram,
    flush_duration_histogram: Histogram,
}

#[derive(Clone)]
pub struct WalWriter<I: Instrument + Clone + 'static> {
    state: Rc<RefCell<WalWriterState<I>>>,
}

impl<I: Instrument + Clone + 'static> WalWriter<I> {
    pub async fn new(
        wal_dir: InstrumentedDirectory<I>,
        wal_dir_path: PathBuf,
        continue_segment_file: Option<InstrumentedDmaFile<I>>,
        continue_segment_file_idx: Option<u64>,
        buf_size: u64,
        buf_num: usize,
        segment_size: u64,
    ) -> WalWriteResult<Self> {
        // TODO find out if continue segment file is some
        //  or create new in wal dir
        let segment_file = match continue_segment_file {
            Some(dma_file) => dma_file,
            None => {
                // TODO proper segment naming, file name is first lsn
                // think about .partial extension
                let segment_file = wal_dir.create_file(wal_file_name(0)).await?;
                segment_file.fdatasync().await?;
                // it is required for durability to sync parent directories after creating new files
                // TODO is it the correct ordering, detect the wrong one?
                wal_dir.sync().await?;
                segment_file.ensure_durable(None);
                segment_file
            }
        };

        let segment_writer_state =
            WalSegmentWriterState::new_from_segment_file(Rc::new(segment_file), buf_size, buf_num);

        let state = WalWriterState {
            wal_dir,
            wal_dir_path,
            buf_size,
            buf_num,
            segment_size,
            current_segment_writer_state: segment_writer_state,
            current_segment_idx: continue_segment_file_idx.unwrap_or(0),
            flush_is_in_progress: false,
            has_unflushed_records: false,
            switch_segments_lock: Rc::new(RwLock::new(())),
            write_duration_histogram: Histogram::new(),
            flush_duration_histogram: Histogram::new(),
        };

        Ok(Self {
            state: Rc::new(RefCell::new(state)),
        })
    }

    pub fn write_distribution_histogram(&self) -> Histogram {
        let state = self.state.borrow();
        state.write_duration_histogram.clone()
    }

    pub fn flush_duration_histogram(&self) -> Histogram {
        let state = self.state.borrow();
        state.flush_duration_histogram.clone()
    }

    fn rotate_buffer(&self, state: &mut RefMut<'_, WalWriterState<I>>, record_size: u64) {
        let left = state.buf_size - state.current_segment_writer_state.buf_pos;
        if left < record_size {
            // rotate buffers
            let mut buf = state
                .current_segment_writer_state
                .buf_queue
                .pop_front()
                .unwrap();
            if left > 0 {
                buf.as_bytes_mut()[state.current_segment_writer_state.buf_pos as usize] =
                    RecordMarker::Padding as u8;
            }
            state.current_segment_writer_state.buf_pos = 0;

            // if remaining size is not less than buf_num * buf_size
            // TODO split into function and write tests for it
            // if !(state.segment_size - state.current_segment_writer_state.file_pos
            //     < state.buf_num as u64 * state.buf_size)
            // {
            let new_buf = state
                .current_segment_writer_state
                .segment_file
                .alloc_dma_buffer(state.buf_size as usize);

            state
                .current_segment_writer_state
                .buf_queue
                .push_back(new_buf); // TODO offload to low priority task queue

            // save write pos, and advance it for future writes, so concurrent writes receive correct positions so there is no race condition
            let file_pos = state.current_segment_writer_state.file_pos;
            state.current_segment_writer_state.file_pos += state.buf_size;
            let dma_file = Rc::clone(&state.current_segment_writer_state.segment_file);

            let join_handle = glommio::spawn_local(async move {
                dma_file.write_at(buf, file_pos).await?;
                Ok(())
            })
            .detach();

            // meh, avoid borrowing error
            let buf_size = state.buf_size;
            state
                .current_segment_writer_state
                .pending_writes
                .push((file_pos + buf_size, join_handle));
        }
    }

    fn should_switch_segments(state: &RefMut<'_, WalWriterState<I>>, record_size: u64) -> bool {
        state.current_segment_writer_state.file_pos
            + state.current_segment_writer_state.buf_pos
            + record_size
            > state.segment_size
    }

    async fn switch_segments(&self, record_size: u64) -> WalWriteResult<()> {
        let state = self.state.borrow_mut();
        if !Self::should_switch_segments(&state, record_size) {
            return Ok(());
        }

        let lock = Rc::clone(&state.switch_segments_lock);
        drop(state);
        // What a dance with Rc and drop...
        crate::debug_print!("acquiring switch segments lock");
        // just a way to wait for already running flush to complete
        let _guard = lock.write().await.unwrap();
        crate::debug_print!("switch segments lock acquired");
        let mut state = self.state.borrow_mut();
        if !Self::should_switch_segments(&state, record_size) {
            crate::debug_print!("no need to flush after concurrent flush finished");
            return Ok(());
        }

        crate::debug_print!(
            "got file_pos + buf_size + record_size > segment_size {} + {} + {} > {}",
            state.current_segment_writer_state.file_pos,
            state.current_segment_writer_state.buf_pos,
            record_size,
            state.segment_size
        );
        // Switch segments
        // Some things can be improved to decrease latency during a segment switch, but
        //  this should be pretty infrequent operation, so maybe it is better to avoid these
        //  complications and keep the code simpler
        // 0. do we need to block other operations? (TODO no, preallocate segments)
        // 1. issue self.flush for current segment
        //      currently we can do it synchronously but it is possible to spawn it
        //      but it requires extra reasoning and correctness checks
        // 2. close current file
        // 3. initialize new segment file (could it be pre initialized to reduce latency of segment switch?)
        // 4. continue writing

        // check if flush already in progress

        // we probably need a flag or a lock to prevent concurrent segment switch,
        // if we detected one we should wait on the lock and then continue with new one
        // we can even use same pattern with separate list of wakers, i e who waits for segment switch to complete
        // when it is completed we can wake them up
        let current_segment_idx = state.current_segment_idx + 1;

        let current_segment_file = Rc::clone(&state.current_segment_writer_state.segment_file);
        let new_segment_path = state.wal_dir_path.join(wal_file_name(current_segment_idx));

        // pad current buffer so reader correctly parses it
        // TODO do we need switch segment marker?
        // TODO can there be some smart strategy about that?
        //  Because padding means that other parts are left. See comment about the same thing
        //  inside flush_inner
        let pad_write_pos = if state.buf_size - state.current_segment_writer_state.buf_pos > 0 {
            Some(state.current_segment_writer_state.buf_pos as usize)
        } else {
            None
        };

        let buf = state
            .current_segment_writer_state
            .buf_queue
            .get_mut(0)
            .unwrap();

        if let Some(pos) = pad_write_pos {
            buf.as_bytes_mut()[pos] = RecordMarker::Padding as u8;
            crate::debug_print!(
                "state.current_segment_writer_state.file_pos {}",
                state.current_segment_writer_state.file_pos
            );
            crate::debug_print!(
                "state.current_segment_writer_state.buf_pos {}",
                state.current_segment_writer_state.buf_pos
            );

            crate::debug_print!(
                "inserting switch padding at {}",
                state.current_segment_writer_state.file_pos
                    + state.current_segment_writer_state.buf_pos
            );
            state.current_segment_writer_state.buf_pos += 1;
        }

        let wal_dir_to_fsync = state.wal_dir.try_clone().unwrap(); // why I cant just get the same fd without duplication?

        let instrument = state.wal_dir.instrument.clone();
        drop(state);
        self.flush().await?;
        // TODO can we batch these calls?
        current_segment_file.close_rc().await?;
        let new_segment_file = InstrumentedDmaFile::create(&new_segment_path, instrument).await?;
        new_segment_file.fdatasync().await?;
        wal_dir_to_fsync.sync().await?;
        // Running sync on directory is required for durability of newly created file directory entry 
        new_segment_file.ensure_durable(None);

        let mut state = self.state.borrow_mut();
        state.current_segment_idx = current_segment_idx;
        // TODO There should be no incomplete writes, should we block everything?
        // What to do with writes which are started during these awaits?
        // see comment above self.flush
        assert_eq!(state.current_segment_writer_state.pending_writes.len(), 0);
        assert_eq!(
            state.current_segment_writer_state.flush_waiter_wakers.len(),
            0
        );
        state.current_segment_writer_state = WalSegmentWriterState::new_from_segment_file(
            Rc::new(new_segment_file),
            state.buf_size,
            state.buf_num,
        );

        crate::debug_print!("switch to new segment completed {new_segment_path:?}");
        Ok(())
    }

    pub async fn write<'a, T: WalWritable<'a> + Display>(&self, record: T) -> WalWriteResult<()> {
        let state = self.state.borrow_mut();
        let record_size = record.size();
        if record_size > state.buf_size {
            // for now this is a limitation
            return Err(WalWriteError::RecordSizeLimitExceeded);
        }
        crate::debug_print!(
            "write start file_pos + buf_pos + record_size {} + {} + {} = {}",
            state.current_segment_writer_state.file_pos,
            state.current_segment_writer_state.buf_pos,
            record_size,
            state.current_segment_writer_state.file_pos
                + state.current_segment_writer_state.buf_pos
                + record_size
        );
        // TODO if we open next segment in advance, can we begin scheduling new writes before segment switch is finished?
        //   take into account waiting for pending writes
        // so annoying to not be able to pass it directly...
        drop(state);

        let write_start = Instant::now();

        self.switch_segments(record_size).await?;

        let mut state = self.state.borrow_mut();

        // if record doesn't fit current buffer, schedule write of the current buffer and move on with the next one
        self.rotate_buffer(&mut state, record_size);
        state.current_segment_writer_state.write_record(&record);
        state.current_segment_writer_state.buf_pos += record_size;

        state.has_unflushed_records = true;

        let file_pos = state.current_segment_writer_state.file_pos;
        let buf_pos = state.current_segment_writer_state.buf_pos;
        drop(state);

        // using poll_fn to catch current task waker and wait for flush operation to occur
        // TODO is it worth it to replace buf positions with lsn, so mark as committed records with lower or equal lsn?
        poll_fn::<(), _>(|cx| {
            let mut state = self.state.borrow_mut();
            match state
                .current_segment_writer_state
                .flush_waiter_wakers
                .binary_search_by_key(&(file_pos + buf_pos), |(pos, _)| *pos)
            {
                Ok(idx) => match state.current_segment_writer_state.flush_waiter_wakers[idx].1 {
                    WriteState::Pending(_) => Poll::Pending,
                    WriteState::Ready => {
                        state
                            .current_segment_writer_state
                            .flush_waiter_wakers
                            .remove(idx);
                        Poll::Ready(())
                    }
                },
                Err(_) => {
                    state
                        .current_segment_writer_state
                        .flush_waiter_wakers
                        .push((file_pos + buf_pos, WriteState::Pending(cx.waker().clone())));
                    Poll::Pending
                }
            }
        })
        .await;
        crate::debug_print!("write end {}", record);
        let mut state = self.state.borrow_mut();
        state
            .write_duration_histogram
            .increment(write_start.elapsed().as_micros().try_into().unwrap())
            .unwrap();
        Ok(())
    }

    pub async fn write_shutdown(&self) -> WalWriteResult<()> {
        let mut state = self.state.borrow_mut();
        self.rotate_buffer(&mut state, 1);
        state.current_segment_writer_state.write_shutdown();
        // clone it so the mutable reference to state is not held across await point
        let dma_file = Rc::clone(&state.current_segment_writer_state.segment_file);

        // TODO ensure there is no pending writes, so clone is not needed, probably take &mut self?
        //  set some shutdown flag (check it in write and return shutdown error if it is set)
        //  write out current buffer,
        //  flush
        drop(state);
        dma_file.fdatasync().await?;
        Ok(())
    }

    async fn flush_inner(&self) -> WalWriteResult<u64> {
        let mut state = self.state.borrow_mut();
        // get current buffer
        let mut dma_buf = state
            .current_segment_writer_state
            .buf_queue
            .pop_front()
            .unwrap(); // TODO use NonEmptyVec type to avoid it?

        let buf_pos = state.current_segment_writer_state.buf_pos as usize;

        // overwrite next byte with something that does not look like valid data marker: with zero
        // this is needed because when buffer is reallocated it contains old data, so it is possible that
        // valid data that was there on previous buffer flush is still there and looks as valid data
        // so to prevent reading it as valid add explicit zero.
        let rem = state.buf_size as usize - buf_pos;
        if rem > 0 {
            dma_buf.as_bytes_mut()[buf_pos] = 0;
        }

        // allocate a new one, and push it to the queue
        let new_buf = state
            .current_segment_writer_state
            .segment_file
            .alloc_dma_buffer(state.buf_size as usize);
        state
            .current_segment_writer_state
            .buf_queue
            .push_back(new_buf); // TODO offload to low priority task queue

        // TODO describe how buf_pos, file_pos, and flush_file_pos are advanced

        // since we flush incomplete buf, copy current buf content into next one,
        // because of that file_pos is not advanced in flush operation
        // TODO explore which is better, maybe just pad current buf and move to next one,
        // or even maybe use heuristic approach, e.g. if less than 1/3 buffer left pad it and move on,
        // if more space is available make copy and submit as is
        // TODO revisit
        // TODO copy from slice instead write_all
        state.current_segment_writer_state.buf_queue[0]
            .as_bytes_mut()
            .write_all(&dma_buf.as_bytes_mut()[..buf_pos])
            .unwrap();

        // clone it to be able to use it without holding borrowed state
        let dma_file = Rc::clone(&state.current_segment_writer_state.segment_file);
        // this is the position of the log that will become guaranteed to be durable
        let flush_file_pos = state.current_segment_writer_state.file_pos
            + state.current_segment_writer_state.buf_pos;
        crate::debug_print!("flush_file_pos: {}", flush_file_pos);

        // TODO how we can guarantee that all the writes in pending writes are ordered?
        let file_pos = state.current_segment_writer_state.file_pos;
        let dma_file_clone_for_write = Rc::clone(&dma_file);
        let join_handle = glommio::spawn_local(async move {
            dma_file_clone_for_write.write_at(dma_buf, file_pos).await?;
            Ok(())
        })
        .detach();

        // TODO can code benefit from extracting common wait_for_pending_writes function (need to precisely handle TaskClosed)

        state
            .current_segment_writer_state
            .pending_writes
            // NOTE: we record here flush_file_pos, which is file_pos + buf_pos, not file_pos + buf_size
            .push((flush_file_pos, join_handle));

        // gather pending writes that should be completed at the time of fdatasync call
        // we need pending writes which where scheduled with write pos <= flush_file_pos
        let pending_writes: Vec<_> = state
            .current_segment_writer_state
            .pending_writes
            // or equal is needed to wait for incomplete buf we've just scheduled to write
            .drain_filter(|(write_pos, _)| *write_pos <= flush_file_pos)
            .map(|(_, join_handle)| join_handle)
            .collect(); // we can probably avoid this collect

        crate::debug_print!("pending writes: {}", pending_writes.len());
        drop(state);

        // TODO have a plan on what to do if one of the writes fails before the fsync,
        //  we can wrap these errors with separate types,
        //  if fsync fails we should terminate the system (e.g as it is done in postgres)

        // wait for pending writes to complete
        for pending_write_result in join_all(pending_writes).await {
            pending_write_result.ok_or(WalWriteError::TaskClosed)??;
        }

        // ensure that all previous writes are durable
        dma_file.fdatasync().await?;
        dma_file.ensure_durable(Some(flush_file_pos));
        let mut state = self.state.borrow_mut();
        // wake every write that has file position less than flush position
        let mut ctr = 0;
        state
            .current_segment_writer_state
            .flush_waiter_wakers
            .iter_mut()
            .filter(|(write_file_pos, _)| *write_file_pos <= flush_file_pos)
            .for_each(|(_, write_state)| {
                let old_state = mem::replace(write_state, WriteState::Ready);
                match old_state {
                    WriteState::Pending(waker) => waker.wake(),
                    // TODO why is it reacheable? unreacheable? can we encounter elready finished writes?
                    WriteState::Ready => unreachable!(),
                };
                ctr += 1;
            });
        crate::debug_print!("flush end");
        Ok(ctr)
    }

    pub async fn flush(&self) -> WalWriteResult<u64> {
        let mut state = self.state.borrow_mut();

        if !state.has_unflushed_records {
            // nothing to flush
            return Ok(0);
        }

        if state.flush_is_in_progress {
            return Err(WalWriteError::FlushIsAlreadyInProgress);
        }
        state.flush_is_in_progress = true;
        drop(state);
        let flush_start = Instant::now();
        match self.flush_inner().await {
            Ok(flushed) => {
                // ugly borrow_mut again...
                let mut state = self.state.borrow_mut();

                state.flush_is_in_progress = false;

                // if no pending writes reset has_unflushed_ ...
                // TODO is it safe? test it ...
                if state
                    .current_segment_writer_state
                    .flush_waiter_wakers
                    .is_empty()
                {
                    state.has_unflushed_records = false;
                }

                state
                    .flush_duration_histogram
                    .increment(flush_start.elapsed().as_micros().try_into().unwrap())
                    .unwrap();

                Ok(flushed)
            }
            e @ Err(_) => {
                let mut state = self.state.borrow_mut();
                state.flush_is_in_progress = false;

                if state
                    .current_segment_writer_state
                    .flush_waiter_wakers
                    .is_empty()
                {
                    state.has_unflushed_records = false;
                }

                e
            }
        }
    }
}
