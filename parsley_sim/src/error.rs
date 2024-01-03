use std::io;

use glommio::GlommioError;

// TODO wrap glommio error variants.
// The error type has to be the same for both envs in order to test error cases.
// Also sim env needs to be able to construct arbitrary error to simulate failures.
// With library provided error types this is usually is not possible (because of private constructor).
// TODO separate
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct Error(#[from] io::Error);

impl Error {
    pub fn new<E>(kind: io::ErrorKind, error: E) -> Error
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        Error(io::Error::new(kind, error))
    }
}

impl<T> From<GlommioError<T>> for Error {
    fn from(value: GlommioError<T>) -> Self {
        match value {
            GlommioError::IoError(io) => Error(io),
            GlommioError::EnhancedIoError { source, .. } => Error(source),
            e @ _ => unimplemented!(),
        }
    }
}
