use std::{fmt::Display, ops::Range};

use crc32fast::Hasher;

#[derive(Debug, thiserror::Error)]
pub struct UnexpectedEofError {
    offset: u64,
}

impl Display for UnexpectedEofError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Unexpected eof at offset {}", self.offset))
    }
}

// TODO is it better to customize it on top of io::Read?
pub fn read_buf(buf: &[u8], range: Range<usize>) -> Result<&[u8], UnexpectedEofError> {
    let expected_len = range.len();
    let slice = buf.get(range).ok_or(UnexpectedEofError {
        offset: buf.len() as u64,
    })?;

    if slice.len() != expected_len {
        Err(UnexpectedEofError {
            offset: slice.len() as u64,
        })?
    }
    Ok(slice)
}

pub fn checksummed_read_buf<'a>(
    buf: &'a [u8],
    hasher: &mut Hasher,
    range: Range<usize>,
) -> Result<&'a [u8], UnexpectedEofError> {
    let slice = read_buf(buf, range)?;
    hasher.update(slice);
    Ok(slice)
}
