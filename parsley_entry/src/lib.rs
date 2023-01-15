//! This module represents Entry type that wraps key-value pair concentrating
//! knowledge required for other subsystems to avoid duplication.
//! For example size related limits for key and value. Entry also contains common
//! functions. Such as serialization/deserialization, corresponding on-disk size, etc.
use std::{io::Write, mem};

use crc32fast::Hasher;
use parsley_durable_log::{reader::WalReadError, LogWritable};
use parsley_io_util::{checksummed_read_buf, read_buf};

// The same value used in cassandra CQL. Should be enough.
const KEY_SIZE_LIMIT: u64 = u16::MAX as u64;
// 64 MiB. Increase if needed, test with such a workload
const VALUE_SIZE_LIMIT: u64 = 64 << 20;

#[derive(Debug, thiserror::Error)]
pub enum InvalidEntryError {
    #[error("Key size limit exceeded. Value {got} exceeds limit: '{limit}'")]
    KeySizeLimitExceeded { got: u64, limit: u64 },

    #[error("Value size limit exceeded. Value '{got}' exceeds limit: '{limit}'")]
    ValueSizeLimitExceeded { got: u64, limit: u64 },

    #[error("Key cannot be empty")]
    EmptyKey,
}
#[derive(Debug)] // TODO eventually remove
pub struct Entry<'e> {
    key: &'e [u8],
    value: &'e [u8],
}

impl<'e> Entry<'e> {
    pub fn new(key: &'e [u8], value: &'e [u8]) -> Result<Self, InvalidEntryError> {
        if key.is_empty() {
            return Err(InvalidEntryError::EmptyKey);
        }

        if key.len() as u64 > KEY_SIZE_LIMIT {
            return Err(InvalidEntryError::KeySizeLimitExceeded {
                got: key.len() as u64,
                limit: KEY_SIZE_LIMIT,
            });
        }

        if value.len() as u64 > VALUE_SIZE_LIMIT {
            return Err(InvalidEntryError::ValueSizeLimitExceeded {
                got: value.len() as u64,
                limit: VALUE_SIZE_LIMIT,
            });
        }

        Ok(Self { key, value })
    }

    pub fn key(&self) -> &[u8] {
        self.key
    }

    pub fn value(&self) -> &[u8] {
        self.value
    }
}

pub const RECORD_HEADER_SIZE: u64 = (mem::size_of::<u64>() * 2) as u64;
pub const SIZE_OF_SIZE: u64 = mem::size_of::<u64>() as u64;
pub const CHECKSUM_SIZE: u64 = mem::size_of::<u32>() as u64;

impl<'rec> LogWritable<'rec> for Entry<'rec> {
    fn encoded_size(&self) -> u64 {
        RECORD_HEADER_SIZE + (self.key.len() + self.value.len()) as u64 + CHECKSUM_SIZE
    }

    fn encode_into(&self, mut buf: &mut [u8]) {
        let initial_pos = buf.len();

        let mut checksum_hasher = Hasher::new();
        // unwraps are ok? since caller already checked that there is enough space
        let key_len = (self.key.len() as u64).to_be_bytes();
        buf.write_all(&key_len).unwrap();

        let value_len = (self.value.len() as u64).to_be_bytes();
        buf.write_all(&value_len).unwrap();

        buf.write_all(self.key).unwrap();
        buf.write_all(self.value).unwrap();

        checksum_hasher.update(&key_len);
        checksum_hasher.update(&value_len);

        checksum_hasher.update(self.key);
        checksum_hasher.update(self.value);
        buf.write_all(&checksum_hasher.finalize().to_be_bytes())
            .unwrap();
        debug_assert_eq!(initial_pos - buf.len(), self.encoded_size() as usize);
    }

    fn decode_from<'buf>(buf: &'buf [u8]) -> Result<(u64, Self), WalReadError>
    where
        Self: Sized,
        'buf: 'rec,
    {
        let mut checksum_hasher = Hasher::new();
        let mut pos = 0;
        let key_size = u64::from_be_bytes(
            checksummed_read_buf(
                &buf,
                &mut checksum_hasher,
                pos as usize..(pos + SIZE_OF_SIZE) as usize,
            )?
            .try_into()?,
        );
        pos += SIZE_OF_SIZE;

        let value_size = u64::from_be_bytes(
            checksummed_read_buf(
                &buf,
                &mut checksum_hasher,
                pos as usize..(pos + SIZE_OF_SIZE) as usize,
            )?
            .try_into()?,
        );
        pos += SIZE_OF_SIZE;

        let key = checksummed_read_buf(
            &buf,
            &mut checksum_hasher,
            pos as usize..(pos + key_size) as usize,
        )?;
        pos += key_size;

        let value = checksummed_read_buf(
            &buf,
            &mut checksum_hasher,
            pos as usize..(pos + value_size) as usize,
        )?;
        pos += value_size;

        let actual_checksum = u32::from_be_bytes(
            read_buf(&buf, pos as usize..(pos + CHECKSUM_SIZE) as usize)?.try_into()?,
        );
        let expected_checksum = checksum_hasher.finalize();
        if actual_checksum != expected_checksum {
            Err(WalReadError::ChecksumMismatch {
                expected: expected_checksum,
                actual: actual_checksum,
            })?;
        }
        pos += CHECKSUM_SIZE;
        let rec = Entry { key, value };
        Ok((pos, rec))
    }
}

// TODO Test wal writable impl
// TODO Property test that encoded size matches size written to buf
// TODO Property test pairs encode into, decode from
// TODO Test for checksum mismatch
