use std::{
    cell::{Ref, RefCell},
    convert::Infallible,
    fmt::{Debug, Display},
    path::PathBuf,
    rc::Rc,
};

pub mod adapter;
pub mod instrument;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileRange {
    start: u64,
    // exclusive
    end: u64,
}

impl FileRange {
    fn from_pos_and_buf_len(pos: u64, buf_len: u64) -> Self {
        Self {
            start: pos,
            end: pos + buf_len,
        }
    }
}

impl Display for FileRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[{}..{})", self.start, self.end))
    }
}

#[derive(Clone, Debug)]
pub struct WriteEvent {
    fd: i32,
    file_range: FileRange,
}

impl WriteEvent {
    fn new(fd: i32, pos: u64, buf_len: u64) -> Self {
        Self {
            fd,
            file_range: FileRange::from_pos_and_buf_len(pos, buf_len),
        }
    }
}

// TODO warn when reading unflushed data
// TODO catch fsync retry problem, that it first cleans buffers and then
// TODO single block overwrite should be safe?
// TODO aggregate multiple errors into one
// TODO check on drop if all files are persisted (do that explicitly on some finish() method call)

#[derive(Debug, Clone)]
pub enum EitherPathOrFd {
    Path(PathBuf),
    Fd(i32),
}

#[derive(Debug, Clone)]
pub enum Event {
    // To associate path with the fd
    Open(PathBuf, i32),
    // To remove the association
    Close(i32),
    // Registers duplicated fd
    Dup(i32, i32),
    // dirties file
    Write(WriteEvent),
    // directly sets max_written_pos for a file
    // disgards write events past specified size
    SetLen(i32, u64),
    // clears pending changes
    Fsync(i32),
    // clears pending changes without advancing synced length
    Fdatasync(i32),
    // removees file from tracking
    Delete(PathBuf),
    // initielizes file state
    Create(PathBuf),
    // initializes unsynced directory
    CreateDir(PathBuf),
    // dirties file, dirties parent dir
    // TODO check what posix rename accepts, path or fd for the first arg
    Rename {
        from: EitherPathOrFd,
        to: PathBuf,
    },
    EnsureFileDurable {
        target: EitherPathOrFd,
        up_to: Option<u64>,
    },
    EnsureDirDurable(PathBuf),
    // Needed to solve the validation of parent dir
    AddTrustedDir(PathBuf),
}

pub trait Instrument {
    type Error: std::error::Error;

    fn apply_event(&self, event: Event) -> Result<(), Self::Error>;
}

#[derive(Clone)]
pub struct Noop;

impl Instrument for Noop {
    type Error = Infallible;

    fn apply_event(&self, _event: Event) -> Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Clone)]
pub struct Collect {
    state: Rc<RefCell<Vec<Event>>>,
}

impl Collect {
    pub fn events(&self) -> Ref<'_, Vec<Event>> {
        self.state.borrow()
    }
}

impl Instrument for Collect {
    type Error = Infallible;

    fn apply_event(&self, event: Event) -> Result<(), Self::Error> {
        self.state.borrow_mut().push(event);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::FileRange;

    #[test]
    fn file_range() {
        let fr = FileRange::from_pos_and_buf_len(0, 10);
        assert_eq!(fr, FileRange { start: 0, end: 10 });

        let fr = FileRange::from_pos_and_buf_len(10, 10);
        assert_eq!(fr, FileRange { start: 10, end: 20 });

        let fr = FileRange::from_pos_and_buf_len(1, 0);
        assert_eq!(fr, FileRange { start: 1, end: 1 })
    }
}
