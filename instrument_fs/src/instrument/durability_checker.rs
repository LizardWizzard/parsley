use std::{
    cell::RefCell,
    cmp,
    collections::{hash_map, HashMap},
    fmt::Display,
    path::{Path, PathBuf},
    rc::Rc,
};

use crate::{EitherPathOrFd, Event, FileRange, Instrument};

/// Either up_to from ensure_durable_up_to or max_written_pos
/// Needed to give nicer error message
#[derive(Debug, PartialEq, Eq)]
enum Horizon {
    UpTo(u64), // TODO NonZeroU64
    MaxWrittenPos(u64),
}

impl Horizon {
    fn value(&self) -> u64 {
        match self {
            Horizon::UpTo(up_to) => *up_to,
            Horizon::MaxWrittenPos(max_written_pos) => *max_written_pos,
        }
    }
}

impl Display for Horizon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Horizon::UpTo(up_to) => f.write_fmt(format_args!("up to {}", up_to)),
            Horizon::MaxWrittenPos(max_written_pos) => {
                f.write_fmt(format_args!("max written pos {}", max_written_pos))
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct PendingChanges {
    pending_changes: Vec<FileRange>,
    max_durable_pos: u64,

    up_to: Horizon,
}

impl Display for PendingChanges {
    #[rustfmt::skip]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Max synced position: {}\n", self.max_durable_pos))?;
        f.write_fmt(format_args!("Horizon: {}\n", self.up_to))?;

        if !self.pending_changes.is_empty() {
            f.write_str("Pending changes: ")?;
            for change in self.pending_changes.iter() {
                match self.up_to {
                    Horizon::UpTo(up_to) => {
                        assert!(change.start <= up_to, "when checking with up_to there should be no changes that are entirely beyond up_to");
                        if change.end <= up_to {
                            f.write_fmt(format_args!("    {} earlier than specified up to {}", change, up_to))?
                        } else {
                            f.write_fmt(format_args!("    {} crosses specified up to {}", change, up_to))?                           
                        }
                    },
                    Horizon::MaxWrittenPos(max_written_pos) => {
                        if change.end <= max_written_pos {
                            f.write_fmt(format_args!("    {} earlier than max written pos {}\n", change, max_written_pos))?
                        } else if change.start > max_written_pos {
                            f.write_fmt(format_args!("    {} beyond max written position {}\n", change, max_written_pos))?
                        } else {
                            f.write_fmt(format_args!("    {} crosses specified up to {}\n", change, max_written_pos))?
                        }
                    }
                }
            }
        }

        if self.max_durable_pos < self.up_to.value() {
            // TODO better message
            f.write_fmt(format_args!("Max durable pos {} != up to {}\n", self.max_durable_pos, self.up_to.value()))?
        }

        Ok(())
    }
}

// TODO error registry, give error code,
// and provide explanation for each error so user can understand what went wrong
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum DurabilityViolationError {
    #[error("File has pending changes.\n{0}")]
    PendingChanges(PendingChanges),

    #[error("File has no pending changes, but it wasnt synced after call to `create`")]
    NotSyncedAfterCreation,

    #[error("File parent directory {0} is not syncronized to disk, syncronize it via fsync or fdatasync to fix the the problem")]
    ParentDirectoryNotSynced(Box<str>),
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum Error {
    #[error("Durability constraint violation: {0}")]
    DurabilityViolation(#[from] DurabilityViolationError),

    #[error("Unknown file at {0}")]
    UnknownPath(PathBuf),

    #[error("Unknown file descriptor {0}")]
    UnknownFd(i32),

    #[error("Attempt to create already existing file at {0}")]
    AlreadyExists(PathBuf),

    #[error("Expecteed file got dirctory")]
    ExpectedFileGotDirectory,

    #[error("Expecteed directory got file")]
    ExpectedDirectoryGotFile,

    #[error("No parent dir for {0}")]
    NoParentDir(PathBuf),
}

#[derive(Debug, Default)]
struct FileState {
    // Needed when filw was created, but there was no changes to it
    // So we can say whether the creation of the file was persisted or not
    synced_after_creation: bool,
    // Changes submitted to a file.
    pending_changes: Vec<FileRange>,
    // File size that is synced to the file system.
    // Since writes can extend the file, but if this change is not synced the size change can be lost.
    // Keeping it separately is required to implement difference between fsync and fdatasync
    // because fdatasync does not sychronize file metadata which containes size of the file
    max_durable_pos: u64,
    // Maximum position in a file that was written to.
    // Includes unsynced changes. Needed to verify durability when fdatasync
    // leaves behind unsynced writes that extend a file on the filesystem
    max_written_pos: u64,
}

impl FileState {
    // TODO unittest
    fn ensure_durable(&self) -> Result<(), Error> {
        // no pending changes
        if !(self.pending_changes.is_empty()
                // no writes that extend file were left unsynced
                && self.max_durable_pos == self.max_written_pos)
        {
            Err(Error::DurabilityViolation(
                DurabilityViolationError::PendingChanges(PendingChanges {
                    pending_changes: self.pending_changes.clone(),
                    max_durable_pos: self.max_durable_pos,
                    up_to: Horizon::MaxWrittenPos(self.max_written_pos),
                }),
            ))
        } else if !self.synced_after_creation {
            Err(Error::DurabilityViolation(
                DurabilityViolationError::NotSyncedAfterCreation,
            ))
        } else {
            Ok(())
        }
    }

    // TODO unittest
    fn ensure_durable_up_to(&self, up_to: u64) -> Result<(), Error> {
        // no pending changes crossing the `up_to` value
        let pending_changes = self
            .pending_changes
            .iter()
            .filter(|c| c.start <= up_to)
            .cloned()
            .collect::<Vec<_>>();

        if !pending_changes.is_empty()
                // no writes that extend file were left unsynced
                || self.max_durable_pos < up_to
        {
            Err(Error::DurabilityViolation(
                DurabilityViolationError::PendingChanges(PendingChanges {
                    pending_changes,
                    max_durable_pos: self.max_durable_pos,
                    up_to: Horizon::UpTo(up_to),
                }),
            ))
        } else {
            Ok(())
        }
    }

    fn apply_fsync(&mut self) {
        self.synced_after_creation = true;
        self.max_durable_pos = self.max_written_pos;
        self.pending_changes.clear()
    }
}

#[derive(Debug, Default)]
struct DirectoryState {
    // indicates whether the directory entry was synced to disk using fsync
    is_synced: bool,
    // is_trusted needed to break the file durability check
    // when it goes to verify that all directories up to a
    // files under check parent dir are durably written
    // Note that trusted dir can still become not synced which will result in an Error
    is_trusted: bool,
}

impl DirectoryState {
    fn ensure_durable(&self, path: PathBuf) -> Result<bool, Error> {
        if !self.is_synced {
            return Err(Error::DurabilityViolation(
                DurabilityViolationError::ParentDirectoryNotSynced(
                    path.display().to_string().into_boxed_str(),
                ),
            ));
        }

        // is_trusted=true breaks the loop to stop further checks in directory hierarchy
        Ok(self.is_trusted)
    }
}

#[derive(Debug)]
enum PathState {
    File(FileState),
    Directory(DirectoryState),
}

impl PathState {
    fn ensure_file_mut(&mut self) -> Result<&mut FileState, Error> {
        match self {
            PathState::File(file_state) => Ok(file_state),
            PathState::Directory(_) => Err(Error::ExpectedFileGotDirectory),
        }
    }

    fn apply_fsync(&mut self) {
        match self {
            PathState::File(file_state) => {
                file_state.apply_fsync();
            }
            PathState::Directory(directory_state) => directory_state.is_synced = true,
        }
    }

    fn default_file() -> Self {
        PathState::File(FileState::default())
    }

    fn default_dir() -> Self {
        PathState::Directory(DirectoryState::default())
    }

    fn ensure_file_durable(&self) -> Result<(), Error> {
        match self {
            PathState::File(file_state) => file_state.ensure_durable(),
            PathState::Directory(_) => Err(Error::ExpectedFileGotDirectory),
        }
    }

    fn ensure_file_durable_up_to(&self, up_to: u64) -> Result<(), Error> {
        match self {
            PathState::File(file_state) => file_state.ensure_durable_up_to(up_to),
            PathState::Directory(_) => Err(Error::ExpectedFileGotDirectory),
        }
    }

    fn ensure_dir_durable(&self, path: PathBuf) -> Result<bool, Error> {
        match self {
            PathState::File(_) => Err(Error::ExpectedDirectoryGotFile),
            PathState::Directory(directory_state) => directory_state.ensure_durable(path),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum FdatsyncBehavior {
    Linux,
    Bsd,
}

impl Default for FdatsyncBehavior {
    fn default() -> Self {
        FdatsyncBehavior::Linux
    }
}

#[derive(Default)]
pub struct DurabilityCheckerState {
    fdatasync_behavior: FdatsyncBehavior,
    fd_to_path_buf: HashMap<i32, PathBuf>,
    path_states: HashMap<PathBuf, PathState>,
}

impl DurabilityCheckerState {
    fn with_fdatasync_behavior(fdatasync_behavior: FdatsyncBehavior) -> Self {
        DurabilityCheckerState {
            fdatasync_behavior,
            ..Default::default()
        }
    }

    fn check_directory_hierarchy(&self, path: &Path) -> Result<(), Error> {
        // parent directory durable
        // check every parent until trusted dir is reached
        // i e for /foo/bar/baz check /foo/bar and /foo and /
        let components = path.components().collect::<Vec<_>>();

        for i in (0..components.len()).rev() {
            let dir_path_to_check = {
                let mut build = PathBuf::new();
                for comp in &components[..=i] {
                    build.push(comp)
                }
                build
            };

            let path_state = self
                .path_states
                .get(&dir_path_to_check)
                .ok_or_else(|| Error::UnknownPath(dir_path_to_check.clone()))?;

            let is_trusted = path_state.ensure_dir_durable(dir_path_to_check)?;
            if is_trusted {
                break;
            }
        }
        Ok(())
    }

    fn state_by_fd(&mut self, fd: i32) -> Result<&mut PathState, Error> {
        let path = self.fd_to_path_buf.get(&fd).ok_or(Error::UnknownFd(fd))?;

        // TODO what happens when directory is fdatasynced? currently it will error out
        self.path_states
            .get_mut(path)
            .ok_or_else(|| Error::UnknownPath(path.to_owned()))
    }

    fn apply_event(&mut self, event: Event) -> Result<(), Error> {
        match event {
            Event::Open(path, fd) => {
                self.fd_to_path_buf.insert(fd, path);
            }
            Event::Close(fd) => {
                // TODO use drop impl to track what was dropped without calling Close
                self.fd_to_path_buf.remove(&fd);
            }
            Event::Dup(source_fd, target_fd) => {
                let path = self
                    .fd_to_path_buf
                    .get(&source_fd)
                    .ok_or(Error::UnknownFd(source_fd))?;

                self.fd_to_path_buf.insert(target_fd, path.clone());
            }
            Event::Write(write_event) => {
                let mut file_state = self.state_by_fd(write_event.fd)?.ensure_file_mut()?;

                file_state.max_written_pos =
                    cmp::max(file_state.max_written_pos, write_event.file_range.end - 1);

                file_state.pending_changes.push(write_event.file_range);
            }
            Event::SetLen(fd, len) => {
                let file_state = self.state_by_fd(fd)?.ensure_file_mut()?;

                match len.cmp(&file_state.max_written_pos) {
                    cmp::Ordering::Less => {
                        // TODO unittest
                        for i in 0..file_state.pending_changes.len() {
                            // either disgard the range if it is completely past the file
                            if file_state.pending_changes[i].start > len {
                                file_state.pending_changes.remove(i);
                            // or truncate the range to end value
                            } else if file_state.pending_changes[i].end > len {
                                file_state.pending_changes[i].end = len
                            }
                        }
                    }
                    cmp::Ordering::Equal => {}
                    cmp::Ordering::Greater => {
                        // TODO write a test for off by one
                        file_state.pending_changes.push(FileRange {
                            start: file_state.max_written_pos + 1,
                            end: len - 1,
                        });
                        file_state.max_written_pos = len - 1;
                    }
                }
            }
            Event::Fsync(fd) => {
                let path_state = self.state_by_fd(fd)?;

                path_state.apply_fsync()
            }
            Event::Fdatasync(fd) => {
                // TODO write a test for directory Fdatasync
                let fdatasync_behavior = self.fdatasync_behavior;
                match self.state_by_fd(fd)? {
                    PathState::File(file_state) => {
                        match fdatasync_behavior {
                            FdatsyncBehavior::Linux => {
                                // in terms of file size tracking fdatasync is equal to fsync
                                // the only difference is metadata like time of the modification
                                // which will unlikely will lead to data loss,
                                // and it is aut of scope of the model
                                file_state.apply_fsync()
                            }
                            FdatsyncBehavior::Bsd => {
                                // Note: max_durable_pos is not updated. This is pessimistic approach to fdatasync semantics.
                                // On some platforms or on some of file systems it does not update metadata
                                // (the important bit of it is file size). Linux man states that file size
                                // is updated by fdatasync.
                                file_state.synced_after_creation = true;
                                file_state.pending_changes.clear();
                            }
                        }
                    }
                    PathState::Directory(dir) => dir.is_synced = true,
                }
            }
            Event::Create(path) => {
                match self.path_states.entry(path.clone()) {
                    hash_map::Entry::Occupied(e) => {
                        return Err(Error::AlreadyExists(e.key().to_owned()))
                    }
                    hash_map::Entry::Vacant(e) => {
                        e.insert(PathState::default_file());
                    }
                };
                let parent = path
                    .parent()
                    .expect("parent directory is missing for path passed to `create`")
                    .to_owned();

                let parent_state = self
                    .path_states
                    .get_mut(&parent)
                    .ok_or_else(|| Error::UnknownPath(parent.to_owned()))?;
                match parent_state {
                    PathState::File(_) => return Err(Error::ExpectedDirectoryGotFile),
                    PathState::Directory(dir) => dir.is_synced = false,
                }
            }
            Event::CreateDir(path) => {
                match self.path_states.entry(path) {
                    hash_map::Entry::Occupied(e) => {
                        return Err(Error::AlreadyExists(e.key().to_owned()))
                    }
                    hash_map::Entry::Vacant(e) => e.insert(PathState::default_dir()),
                };
            }
            Event::Delete(path) => {
                match self.path_states.entry(path) {
                    hash_map::Entry::Occupied(e) => {
                        e.remove();
                    }
                    hash_map::Entry::Vacant(e) => {
                        return Err(Error::UnknownPath(e.key().to_owned()))
                    }
                };
            }
            Event::Rename { from, to } => {
                let from_path = match from {
                    EitherPathOrFd::Path(path) => path,
                    EitherPathOrFd::Fd(fd) => self
                        .fd_to_path_buf
                        .get(&fd)
                        .ok_or(Error::UnknownFd(fd))?
                        .to_owned(), // needed to release borrow of fd_to_path_buf
                };

                let (_, from_path_state) = self
                    .path_states
                    .remove_entry(&from_path)
                    .ok_or_else(|| Error::UnknownPath(from_path.clone()))?;

                let parent_state = self
                    .path_states
                    .get_mut(
                        from_path
                            .parent()
                            .ok_or_else(|| Error::NoParentDir(from_path.clone()))?,
                    )
                    .ok_or_else(|| Error::UnknownPath(from_path.clone()))?;

                // clear is_synced on parent dir
                match parent_state {
                    PathState::File(_) => Err(Error::ExpectedDirectoryGotFile)?,
                    PathState::Directory(directory_state) => directory_state.is_synced = false,
                }

                // TODO should I mark file as dirty too?
                for (_, v) in self.fd_to_path_buf.iter_mut() {
                    if v == &from_path {
                        *v = to.clone()
                    }
                }

                self.path_states.insert(to, from_path_state);
            }
            Event::EnsureFileDurable { target, up_to } => {
                let path = match target {
                    EitherPathOrFd::Path(ref path) => path,
                    EitherPathOrFd::Fd(fd) => {
                        self.fd_to_path_buf.get(&fd).ok_or(Error::UnknownFd(fd))?
                    }
                };

                let path_state = self
                    .path_states
                    .get_mut(path)
                    .ok_or_else(|| Error::UnknownPath(path.to_owned()))?;

                match up_to {
                    // TODO bail if up to is larger than the size of the file
                    Some(up_to) => path_state.ensure_file_durable_up_to(up_to)?,
                    None => path_state.ensure_file_durable()?,
                }

                self.check_directory_hierarchy(
                    path.parent()
                        .ok_or_else(|| Error::NoParentDir(path.clone()))?,
                )?
            }
            Event::EnsureDirDurable(path) => {
                let path_state = self
                    .path_states
                    .get(&path)
                    .ok_or_else(|| Error::UnknownPath(path.clone()))?;

                path_state.ensure_dir_durable(path.clone())?;

                self.check_directory_hierarchy(
                    path.parent()
                        .ok_or_else(|| Error::NoParentDir(path.clone()))?,
                )?
            }
            // insert a file without pending modification that represents a directory
            // when a file is checked for durability, parent directories are checked as well
            // trusted dir allows to stop
            Event::AddTrustedDir(path) => match self.path_states.entry(path) {
                hash_map::Entry::Occupied(e) => {
                    return Err(Error::AlreadyExists(e.key().to_owned()))
                }
                hash_map::Entry::Vacant(e) => {
                    e.insert(PathState::Directory(DirectoryState {
                        is_synced: true,
                        is_trusted: true,
                    }));
                }
            },
        }
        Ok(())
    }
}

#[derive(Default, Clone)]
pub struct DurabilityChecker {
    state: Rc<RefCell<DurabilityCheckerState>>,
}

impl DurabilityChecker {
    pub fn with_fdatasync_behavior(fdatasync_behavior: FdatsyncBehavior) -> Self {
        DurabilityChecker {
            state: Rc::new(RefCell::new(
                DurabilityCheckerState::with_fdatasync_behavior(fdatasync_behavior),
            )),
        }
    }
}

impl Instrument for DurabilityChecker {
    type Error = Error;

    fn apply_event(&self, event: Event) -> Result<(), Self::Error> {
        self.state.borrow_mut().apply_event(event)
    }
}

pub fn apply_events(
    checker: &mut DurabilityChecker,
    events: impl IntoIterator<Item = Event>,
) -> Result<(), Error> {
    for event in events {
        checker.apply_event(event)?
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use crate::{
        instrument::durability_checker::PathState, EitherPathOrFd, Event, FileRange, Instrument,
        WriteEvent,
    };

    use super::{
        apply_events, DurabilityChecker, DurabilityViolationError, Error, FdatsyncBehavior,
        Horizon, PendingChanges,
    };

    #[test]
    fn basic() {
        let f = PathBuf::from("/root/data/log");
        let fd = 1;

        let f_parent = f.parent().unwrap().to_owned();
        let f_parent_fd = 11;

        let mut checker = DurabilityChecker::default();
        // check no fsync on a file
        let events = [
            Event::AddTrustedDir(PathBuf::from("/root")),
            Event::CreateDir(PathBuf::from("/root/data")),
            Event::Create(f.clone()),
            Event::Open(f.clone(), fd),
            Event::Write(WriteEvent {
                fd,
                file_range: FileRange { start: 0, end: 10 },
            }),
            Event::EnsureFileDurable {
                target: EitherPathOrFd::Fd(fd),
                up_to: None,
            },
        ];

        assert_eq!(
            apply_events(&mut checker, events).unwrap_err(),
            Error::DurabilityViolation(DurabilityViolationError::PendingChanges(PendingChanges {
                pending_changes: vec![FileRange { start: 0, end: 10 }],
                max_durable_pos: 0,
                up_to: Horizon::MaxWrittenPos(9),
            })),
        );

        // file synced, parent dir not synced
        checker.apply_event(Event::Fsync(fd)).unwrap();
        assert_eq!(
            checker
                .apply_event(Event::EnsureFileDurable {
                    target: EitherPathOrFd::Fd(fd),
                    up_to: None,
                })
                .unwrap_err(),
            Error::DurabilityViolation(DurabilityViolationError::ParentDirectoryNotSynced(
                "/root/data".to_owned().into_boxed_str(),
            )),
        );

        // sync parent dir, check should pass
        apply_events(
            &mut checker,
            [
                Event::Open(f_parent.clone(), f_parent_fd),
                Event::Fsync(f_parent_fd),
                Event::EnsureFileDurable {
                    target: EitherPathOrFd::Fd(fd),
                    up_to: None,
                },
            ],
        )
        .unwrap();

        // check rename
        // do not sync parent dir first
        let renamed = f.parent().unwrap().join("log_renamed");
        checker
            .apply_event(Event::Rename {
                from: EitherPathOrFd::Fd(fd),
                to: renamed.clone(),
            })
            .unwrap();

        assert_eq!(
            checker
                .apply_event(Event::EnsureFileDurable {
                    target: EitherPathOrFd::Fd(fd),
                    up_to: None,
                })
                .unwrap_err(),
            Error::DurabilityViolation(DurabilityViolationError::ParentDirectoryNotSynced(
                f_parent.display().to_string().into_boxed_str()
            )),
        );

        apply_events(
            &mut checker,
            [
                Event::Open(f_parent, f_parent_fd),
                Event::Fsync(f_parent_fd),
                Event::Close(f_parent_fd),
                Event::EnsureFileDurable {
                    target: EitherPathOrFd::Path(renamed),
                    up_to: None,
                },
            ],
        )
        .unwrap();
    }

    #[test]
    fn new_file_dirties_dir() {
        let f = PathBuf::from("/root/data");
        let fd = 1;

        let mut checker = DurabilityChecker::default();
        // check no fsync on a file
        // Event::Open(f_parent, f_parent_fd),
        // Event::Fsync(f_parent_fd),

        let events = [
            Event::AddTrustedDir(PathBuf::from("/root")),
            Event::Create(f.clone()),
            Event::Open(f.clone(), fd),
            Event::Write(WriteEvent {
                fd,
                file_range: FileRange { start: 0, end: 10 },
            }),
            Event::Fsync(fd),
            Event::EnsureFileDurable {
                target: EitherPathOrFd::Fd(fd),
                up_to: None,
            },
        ];

        assert_eq!(
            apply_events(&mut checker, events).unwrap_err(),
            Error::DurabilityViolation(DurabilityViolationError::ParentDirectoryNotSynced(
                "/root".to_owned().into_boxed_str()
            )),
        );
    }

    fn durable_create_dir(mut checker: &mut DurabilityChecker, path: &Path, dir_fd: i32) {
        let events = [
            Event::AddTrustedDir(PathBuf::from("/root")),
            Event::CreateDir(path.to_owned()),
            Event::Open(path.to_owned(), dir_fd),
            Event::Fsync(dir_fd),
            Event::EnsureDirDurable(path.to_owned()),
        ];
        apply_events(&mut checker, events).unwrap();
    }

    fn durable_create_empty_file(checker: &mut DurabilityChecker, file: &Path, fd: i32) {
        let parent = file.parent().unwrap().to_owned();
        let parent_fd = 9999;
        apply_events(
            checker,
            [
                Event::Create(file.to_owned()),
                Event::Open(file.to_owned(), fd),
                Event::Open(parent, 9999),
                Event::Fsync(fd),
                Event::Fsync(parent_fd),
                Event::Close(parent_fd),
                Event::EnsureFileDurable {
                    target: EitherPathOrFd::Path(file.to_owned()),
                    up_to: None,
                },
            ],
        )
        .unwrap()
    }

    // TODO test ensure file durable with up_to other than None

    #[test]
    fn fdatasync() {
        let mut checker = DurabilityChecker::with_fdatasync_behavior(FdatsyncBehavior::Bsd);
        // properly create directory
        let dir = PathBuf::from("/root/data");
        let dir_fd = 11;
        durable_create_dir(&mut checker, &dir, dir_fd);

        let f1 = PathBuf::from("/root/data/log1");
        let f1_fd = 1;
        durable_create_empty_file(&mut checker, &f1, f1_fd);

        // write 0, 10
        // fdatasync
        // ensure durable -> fail file not synced
        apply_events(
            &mut checker,
            [
                Event::Write(WriteEvent {
                    fd: f1_fd,
                    file_range: FileRange { start: 0, end: 10 },
                }),
                Event::Fdatasync(f1_fd),
            ],
        )
        .unwrap();

        assert_eq!(
            checker
                .apply_event(Event::EnsureFileDurable {
                    target: EitherPathOrFd::Path(f1.to_owned()),
                    up_to: None,
                })
                .unwrap_err(),
            Error::DurabilityViolation(DurabilityViolationError::PendingChanges(PendingChanges {
                pending_changes: vec![],
                max_durable_pos: 0,
                up_to: Horizon::MaxWrittenPos(9),
            })),
        );

        let f2 = PathBuf::from("/root/data/log2");
        let f2_fd = 2;
        durable_create_empty_file(&mut checker, &f2, f2_fd);

        // set len 11 (ran 10)
        // datasync
        // write 0, 10
        // ensure file durable -> ok
        apply_events(
            &mut checker,
            [
                Event::SetLen(f2_fd, 11),
                Event::Fsync(f2_fd),
                Event::Write(WriteEvent {
                    fd: f2_fd,
                    file_range: FileRange { start: 0, end: 10 },
                }),
                Event::Fdatasync(f2_fd),
                Event::EnsureFileDurable {
                    target: EitherPathOrFd::Path(f2.to_owned()),
                    up_to: None,
                },
            ],
        )
        .unwrap();

        let f3 = PathBuf::from("/root/data/log3");
        let f3_fd = 3;
        durable_create_empty_file(&mut checker, &f3, f3_fd);

        // set len 15
        // datasync
        // write 0, 11
        // write 5, 20
        // write 20, 30
        // set len 18
        // Checking resulting pending changes.
        let f4 = PathBuf::from("/root/data/log4");
        let f4_fd = 4;
        durable_create_empty_file(&mut checker, &f4, f4_fd);
        let events = [
            Event::SetLen(f4_fd, 15),
            Event::Fdatasync(f4_fd),
            Event::Write(WriteEvent::new(f4_fd, 0, 11)),
            Event::Write(WriteEvent::new(f4_fd, 5, 15)),
            Event::Write(WriteEvent::new(f4_fd, 20, 10)),
            Event::SetLen(f4_fd, 18),
        ];

        apply_events(&mut checker, events).expect("should be ok");
        {
            let state = checker.state.borrow_mut();
            let path_state = state.path_states.get(&f4).expect("state should exist");

            let file_state = match path_state {
                PathState::File(f) => f,
                PathState::Directory(_) => panic!("should be file, not a directory"),
            };

            assert_eq!(
                &file_state.pending_changes,
                &[
                    FileRange::from_pos_and_buf_len(0, 11),
                    FileRange::from_pos_and_buf_len(5, 13), // [5, 18)
                ]
            );
        }

        // set len 15
        // write 0, 20
        // datasync
        // durable up to 15
        // FIXME: fails
        // let f5 = PathBuf::from("/root/data/log5");
        // let f5_fd = 5;
        // durable_create_empty_file(&mut checker, &f5, f5_fd);
        // let events = [
        //     Event::SetLen(f5_fd, 15),
        //     Event::Write(WriteEvent::new(f5_fd, 0, 20)),
        //     Event::Fdatasync(f5_fd),
        //     Event::EnsureFileDurable {
        //         target: EitherPathOrFd::Fd(f5_fd),
        //         up_to: Some(15),
        //     },
        // ];

        // apply_events(&mut checker, events).expect("should be ok");

        // let state = checker.state.borrow_mut();
        // let path_state = state.path_states.get(&f5).expect("state should exist");

        // let file_state = match path_state {
        //     PathState::File(f) => f,
        //     PathState::Directory(_) => panic!("should be file, not a directory"),
        // };

        // dbg!(&file_state.pending_changes);
        // assert_eq!(
        //     &file_state.pending_changes,
        //     &[
        //         FileRange::from_pos_and_buf_len(0, 11),
        //         FileRange::from_pos_and_buf_len(5, 13), // [5, 18)
        //     ]
        // )

        // TODO check what changes are left

        // apply_events(
        //     &mut checker,
        //     [
        //         Event::SetLen(file3.clone(), 11),
        //         Event::Fsync(file3.clone()),
        //         Event::Write(WriteEvent {
        //             path: file3.clone(),
        //             file_range: FileRange { start: 0, end: 12 },
        //         }),
        //         Event::Fdatasync(file3.clone()),
        //         Event::EnsureFileDurable {
        //             path: file3.to_owned(),
        //             up_to: None,
        //         },
        //     ],
        // )
        // .unwrap();
    }

    // TODO write a test for fdatasync
    // TODO test synced_after_creation
}
