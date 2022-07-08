use std::{
    cmp,
    collections::{hash_map, HashMap},
    path::{Path, PathBuf},
};

#[derive(thiserror::Error, Debug)]
pub enum DurabilityViolationError {
    #[error("File has pending changes, fsync it to fix the issue")]
    FileNotSynced,

    #[error("File parent directory is not synced, fsync it to fix the issue")]
    ParentDirectoryNotSynced,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Durability constraint violation: {0}")]
    DurabilityViolation(#[from] DurabilityViolationError),

    #[error("Unknown file at {0}")]
    UnknownPath(PathBuf),

    #[error("Attempt to create already existing file at {0}")]
    AlreadyExists(PathBuf),

    #[error("Expecteed file got dirctory")]
    ExpectedFileGotDirectory,

    #[error("Expecteed directory got file")]
    ExpectedDirectoryGotFile,

    #[error("No parent dir for {0}")]
    NoParentDir(PathBuf),
}

#[derive(Clone, Debug)]
pub struct FileRange {
    start: usize,
    // inclusive
    end: usize,
}

#[derive(Clone, Debug)]
pub struct WriteEvent {
    path: PathBuf,
    file_range: FileRange,
}

// TODO warn when reading unflushed data
// TODO trace open event, to index file not only by the path, but by the fd too, or only by the fd?
//   on close we should remove the association because the fd might get assigned to another file
// TODO use a bool like fdatasync_updates_size inside durability cvhecker to configure fdatasync behavior

#[derive(Debug, Clone)]
pub enum Event {
    // dirties file
    Write(WriteEvent),
    // directly sets max_written_pos for a file
    // disgards write events past specified size
    SetLen(PathBuf, usize),
    // clears pending changes
    Fsync(PathBuf),
    // clears pending changes without advancing synced length
    // TODO add verify rules
    Fdatasync(PathBuf),
    // removees file from tracking
    Delete(PathBuf),
    // initielizes file state
    Create(PathBuf),
    // initializes unsynced directory
    CreateDir(PathBuf),
    // dirties file, dirties parent dir
    Rename { from: PathBuf, to: PathBuf },
    EnsureFileDurable { path: PathBuf, up_to: Option<usize> },
    EnsureDirDurable(PathBuf),
    // Needed to solve the validation of parent dir
    AddTrustedDir(PathBuf),
}

#[derive(Debug, Default)]
struct FileState {
    // Changes submitted to a file.
    pending_changes: Vec<FileRange>,
    // File size that is synced to the file system.
    // Since writes can extend the file, but if this change is not synced the size change can be lost.
    // Keeping it separately is required to implement difference between fsync and fdatasync
    // because fdatasync does not sychronize file metadata which containes size of the file
    max_durable_pos: usize,
    // Maximum position in a file that was written to.
    // Includes unsynced changes. Needed to verify durability when fdatasync
    // leaves behind unsynced writes that extend a file on the filesystem
    max_written_pos: usize,
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
                DurabilityViolationError::FileNotSynced,
            ))
        } else {
            Ok(())
        }
    }

    // TODO unittest
    fn ensure_durable_up_to(&self, up_to: usize) -> Result<(), Error> {
        // no pending changes crossing the `up_to` value
        if self.pending_changes.iter().any(|c| c.end <= up_to)
                // no writes that extend file were left unsynced
                || self.max_durable_pos < up_to
        {
            Err(Error::DurabilityViolation(
                DurabilityViolationError::FileNotSynced,
            ))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Default)]
struct DirectoryState {
    // indicates whether the directory entry was synced to disk using fsync
    is_synced: bool,
    // is_trusted needed to break the file durability check
    // when it goes to verify that all directories up to a
    // files under check parent dir are durably written
    is_trusted: bool,
}

impl DirectoryState {
    fn ensure_durable(&self) -> Result<bool, Error> {
        if self.is_synced || self.is_trusted {
            // is_trusted=true breaks the loop to stop further checks in directory hierarchy
            Ok(self.is_trusted)
        } else {
            Err(Error::DurabilityViolation(
                DurabilityViolationError::ParentDirectoryNotSynced,
            ))
        }
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
                file_state.max_durable_pos = file_state.max_written_pos;
                file_state.pending_changes.clear()
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

    fn ensure_file_durable_up_to(&self, up_to: usize) -> Result<(), Error> {
        match self {
            PathState::File(file_state) => file_state.ensure_durable_up_to(up_to),
            PathState::Directory(_) => Err(Error::ExpectedFileGotDirectory),
        }
    }

    fn ensure_dir_durable(&self) -> Result<bool, Error> {
        match self {
            PathState::File(_) => Err(Error::ExpectedDirectoryGotFile),
            PathState::Directory(directory_state) => directory_state.ensure_durable(),
        }
    }
}

#[derive(Default)]
pub struct DurabilityChecker {
    path_states: HashMap<PathBuf, PathState>,
}

impl DurabilityChecker {
    fn check_directory_hierarchy(&self, path: &Path) -> Result<(), Error> {
        // parent directory durable
        // check every parent until trusted dir is reached
        // i e for /foo/bar/baz check /foo/bar and /foo and /
        // TODO check will root dir be handled correctly, corner case with components
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
                .ok_or_else(|| Error::UnknownPath(dir_path_to_check))?;

            let is_trusted = path_state.ensure_dir_durable()?;
            if is_trusted {
                break;
            }
        }
        Ok(())
    }

    pub fn apply_event(&mut self, event: Event) -> Result<(), Error> {
        match event {
            Event::Write(write_event) => {
                let path_state = self
                    .path_states
                    .get_mut(&write_event.path)
                    .ok_or_else(|| Error::UnknownPath(write_event.path.clone()))?;

                let mut file_state = path_state.ensure_file_mut()?;

                file_state.max_written_pos =
                    cmp::max(file_state.max_written_pos, write_event.file_range.end);

                file_state.pending_changes.push(write_event.file_range);
            }
            Event::SetLen(path, len) => {
                let file_state = self
                    .path_states
                    .get_mut(&path)
                    .ok_or(Error::UnknownPath(path))?
                    .ensure_file_mut()?;

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
            Event::Fsync(path) => {
                let path_state = self
                    .path_states
                    .get_mut(&path)
                    .ok_or(Error::UnknownPath(path))?;

                path_state.apply_fsync()
            }
            Event::Fdatasync(path) => {
                // TODO what happens when directory is fdatasynced? currently it will error out
                let file_state = self
                    .path_states
                    .get_mut(&path)
                    .ok_or(Error::UnknownPath(path))?
                    .ensure_file_mut()?;
                // Note: max_durable_pos is not updated. This is pessimistic approach to fdatasync semantics.
                // On some platforms or on some of file systems it does not update metadata
                // (the important bit of it is file size). Linux man states that file size
                // is updated by fdatasync.
                file_state.pending_changes.clear();
            }
            Event::Create(path) => {
                match self.path_states.entry(path) {
                    hash_map::Entry::Occupied(e) => {
                        return Err(Error::AlreadyExists(e.key().to_owned()))
                    }
                    hash_map::Entry::Vacant(e) => e.insert(PathState::default_file()),
                };
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
                let (_, from_path_state) = self
                    .path_states
                    .remove_entry(&from)
                    .ok_or_else(|| Error::UnknownPath(from.clone()))?;

                let parent_state = self
                    .path_states
                    .get_mut(
                        from.parent()
                            .ok_or_else(|| Error::NoParentDir(from.clone()))?,
                    )
                    .ok_or_else(|| Error::UnknownPath(from.clone()))?;

                // reset sync on parent dir
                match parent_state {
                    PathState::File(_) => Err(Error::ExpectedDirectoryGotFile)?,
                    PathState::Directory(directory_state) => directory_state.is_synced = false,
                }
                // TODO should I mark file as dirty too?

                self.path_states.insert(to, from_path_state);
            }
            Event::EnsureFileDurable { path, up_to } => {
                let path_state = self
                    .path_states
                    .get(&path)
                    .ok_or_else(|| Error::UnknownPath(path.clone()))?;

                match up_to {
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

                path_state.ensure_dir_durable()?;

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
    use std::{
        fmt::Display,
        path::{Path, PathBuf},
    };

    use crate::{
        apply_events, DurabilityChecker, DurabilityViolationError, Error, Event, FileRange,
        WriteEvent,
    };

    fn assert_err<T, E: Display>(r: Result<T, E>, expected: E) {
        match r {
            Ok(_) => panic!("expected err got ok"),
            Err(e) => {
                if !matches!(&e, expected) {
                    panic!("got unexpected error {e} expected {expected}")
                }
            }
        }
    }

    #[test]
    fn basic() {
        let file_under_test = PathBuf::from("/root/data/log");
        let mut checker = DurabilityChecker::default();
        // check no fsync on a file
        let events = [
            Event::AddTrustedDir(PathBuf::from("/root")),
            Event::CreateDir(PathBuf::from("/root/data")),
            Event::Create(file_under_test.clone()),
            Event::Write(WriteEvent {
                path: file_under_test.clone(),
                file_range: FileRange { start: 0, end: 10 },
            }),
            Event::EnsureFileDurable {
                path: file_under_test.clone(),
                up_to: None,
            },
        ];
        assert_err(
            apply_events(&mut checker, events),
            Error::DurabilityViolation(DurabilityViolationError::FileNotSynced),
        );

        // file synced, parent dir not synced
        checker
            .apply_event(Event::Fsync(file_under_test.clone()))
            .unwrap();
        assert_err(
            checker.apply_event(Event::EnsureFileDurable {
                path: file_under_test.clone(),
                up_to: None,
            }),
            Error::DurabilityViolation(DurabilityViolationError::ParentDirectoryNotSynced),
        );

        // sync parent dir, check should pass
        apply_events(
            &mut checker,
            [
                Event::Fsync(file_under_test.parent().unwrap().to_owned()),
                Event::EnsureFileDurable {
                    path: file_under_test.clone(),
                    up_to: None,
                },
            ],
        )
        .unwrap();

        // chceck rename
        // do not sync parent dir first
        let renamed = file_under_test.parent().unwrap().join("log_renamed");
        checker
            .apply_event(Event::Rename {
                from: file_under_test.clone(),
                to: renamed.clone(),
            })
            .unwrap();

        assert_err(
            checker.apply_event(Event::EnsureFileDurable {
                path: file_under_test.clone(),
                up_to: None,
            }),
            Error::DurabilityViolation(DurabilityViolationError::ParentDirectoryNotSynced),
        );

        apply_events(
            &mut checker,
            [
                Event::Fsync(renamed.parent().unwrap().to_owned()),
                Event::EnsureFileDurable {
                    path: renamed.clone(),
                    up_to: None,
                },
            ],
        )
        .unwrap();
    }

    fn durable_create_empty(checker: &mut DurabilityChecker, file: &Path) {
        apply_events(
            checker,
            [
                Event::Create(file.to_owned()),
                Event::Fsync(file.to_owned()),
                Event::Fsync(file.parent().unwrap().to_owned()),
                Event::EnsureFileDurable {
                    path: file.to_owned(),
                    up_to: None,
                },
            ],
        )
        .unwrap()
    }

    #[test]
    fn fdatasync() {
        let mut checker = DurabilityChecker::default();
        // properly create directory
        let dir = PathBuf::from("/root/data");
        let events = [
            Event::AddTrustedDir(PathBuf::from("/root")),
            Event::CreateDir(dir.clone()),
            Event::Fsync(dir.clone()),
            Event::EnsureDirDurable(dir.clone()),
        ];
        apply_events(&mut checker, events).unwrap();

        let file1 = PathBuf::from("/root/data/log1");
        durable_create_empty(&mut checker, &file1);

        // write 0, 10
        // fdatasync
        // ensure durable -> fail file not synced
        apply_events(
            &mut checker,
            [
                Event::Write(WriteEvent {
                    path: file1.clone(),
                    file_range: FileRange { start: 0, end: 10 },
                }),
                Event::Fdatasync(file1.clone()),
            ],
        )
        .unwrap();

        assert_err(
            checker.apply_event(Event::EnsureFileDurable {
                path: file1.to_owned(),
                up_to: None,
            }),
            Error::DurabilityViolation(DurabilityViolationError::FileNotSynced),
        );

        let file2 = PathBuf::from("/root/data/log2");
        durable_create_empty(&mut checker, &file2);

        // set len 11 (range including 10)
        // datasync
        // write 0, 10
        // ensure file durable -> ok
        apply_events(
            &mut checker,
            [
                Event::SetLen(file2.clone(), 11),
                Event::Fsync(file2.clone()),
                Event::Write(WriteEvent {
                    path: file2.clone(),
                    file_range: FileRange { start: 0, end: 10 },
                }),
                Event::Fdatasync(file2.clone()),
                Event::EnsureFileDurable {
                    path: file2.to_owned(),
                    up_to: None,
                },
            ],
        )
        .unwrap();

        let file3 = PathBuf::from("/root/data/log3");
        durable_create_empty(&mut checker, &file3);

        // set len 15
        // datasync
        // write 0, 11
        // write 5, 20
        // write 20, 30
        // set len 18
        // TODO check what changes are left

        // set len 15
        // write 0, 20
        // datasync
        // durable up to 15
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
}
