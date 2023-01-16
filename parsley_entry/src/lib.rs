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

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
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

pub const KEY_SIZE_LEN: u64 = mem::size_of::<u16>() as u64;
pub const VALUE_SIZE_LEN: u64 = mem::size_of::<u32>() as u64;
pub const CHECKSUM_SIZE: u64 = mem::size_of::<u32>() as u64;

impl<'rec> LogWritable<'rec> for Entry<'rec> {
    fn encoded_size(&self) -> u64 {
        KEY_SIZE_LEN + VALUE_SIZE_LEN + (self.key.len() + self.value.len()) as u64 + CHECKSUM_SIZE
    }

    fn encode_into(&self, mut buf: &mut [u8]) {
        let initial_pos = buf.len();

        let mut checksum_hasher = Hasher::new();
        // unwraps are ok? since caller already checked that there is enough space
        let key_len = (u16::try_from(self.key.len()))
            .expect("is guaranteed by limit enforced in new")
            .to_be_bytes();
        buf.write_all(&key_len).unwrap();

        let value_len = (u32::try_from(self.value.len()))
            .expect("is guaranteed by limit enforced in new")
            .to_be_bytes();
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
        let key_size = u16::from_be_bytes(
            checksummed_read_buf(
                &buf,
                &mut checksum_hasher,
                pos as usize..(pos + KEY_SIZE_LEN) as usize,
                "key size",
            )?
            .try_into()?,
        ) as u64;
        pos += KEY_SIZE_LEN;

        let value_size = u32::from_be_bytes(
            checksummed_read_buf(
                &buf,
                &mut checksum_hasher,
                pos as usize..(pos + VALUE_SIZE_LEN) as usize,
                "value size",
            )?
            .try_into()?,
        ) as u64;
        pos += VALUE_SIZE_LEN;

        let key = checksummed_read_buf(
            &buf,
            &mut checksum_hasher,
            pos as usize..(pos + key_size) as usize,
            "key",
        )?;
        pos += key_size;

        let value = checksummed_read_buf(
            &buf,
            &mut checksum_hasher,
            pos as usize..(pos + value_size) as usize,
            "value",
        )?;
        pos += value_size;

        let actual_checksum = u32::from_be_bytes(
            read_buf(
                &buf,
                pos as usize..(pos + CHECKSUM_SIZE) as usize,
                "checksum",
            )?
            .try_into()?,
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

#[cfg(test)]
mod err_tests {
    use crate::{Entry, InvalidEntryError, KEY_SIZE_LIMIT, VALUE_SIZE_LIMIT};

    #[test]
    fn empty_key() {
        let err = Entry::new(&[], &[]).unwrap_err();
        assert_eq!(err, InvalidEntryError::EmptyKey)
    }

    #[test]
    fn large_key() {
        let key = vec![0u8; KEY_SIZE_LIMIT as usize + 1];
        let _ = Entry::new(&key[..KEY_SIZE_LIMIT as usize], &[])
            .expect("failed to create entry with key size equal to size limit");

        let err = Entry::new(&key, &[])
            .expect_err("failed to create entry with key size equal to size limit");
        assert_eq!(
            err,
            InvalidEntryError::KeySizeLimitExceeded {
                got: KEY_SIZE_LIMIT + 1,
                limit: KEY_SIZE_LIMIT
            }
        )
    }

    #[test]
    fn large_value() {
        let value = vec![0u8; VALUE_SIZE_LIMIT as usize + 1];
        let _ = Entry::new(b"foo", &value[..VALUE_SIZE_LIMIT as usize])
            .expect("failed to create entry with value size equal to size limit");

        let err = Entry::new(b"foo", &value)
            .expect_err("failed to create entry with value size equal to size limit");
        assert_eq!(
            err,
            InvalidEntryError::ValueSizeLimitExceeded {
                got: VALUE_SIZE_LIMIT + 1,
                limit: VALUE_SIZE_LIMIT
            }
        )
    }
}

#[cfg(test)]
mod writable_tests {
    use parsley_durable_log::{reader::WalReadError, LogWritable};

    use crate::Entry;

    #[test]
    fn size_calculation() {
        let key = [2; 10];
        let value = [4; 12];
        let entry = Entry::new(&key, &value).unwrap();
        // k size, v size, encoded k size (u16) + encoded v size (u32) + checksum (u32)
        let encoded_size = 10 + 12 + 2 + 4 + 4;
        assert_eq!(encoded_size, entry.encoded_size());
    }

    #[test]
    fn encode_decode() {
        for ksize in (1..=101).step_by(20) {
            for vsize in (0..=200).step_by(50) {
                let key = vec![2; ksize];
                let value = vec![4; vsize];
                let entry = Entry::new(&key, &value).unwrap();
                let size = entry.encoded_size();
                let mut buf = vec![0; size as usize];
                entry.encode_into(&mut buf);
                let _ = Entry::decode_from(&buf).expect("cannot decode");

                // Check checksum verification handles every byte change
                for i in 0..size {
                    let mut buf_clone = buf.clone();
                    buf_clone[i as usize] += 1;
                    let err = Entry::decode_from(&buf_clone)
                        .expect_err("should've failed checksum verification");
                    assert!(
                        matches!(
                            err,
                            WalReadError::ChecksumMismatch { .. } | WalReadError::UnexpectedEof(..)
                        ),
                        "{}",
                        err
                    );
                }

                // Detect if record took less space than returned by encoded_size()
                for i in 1..size {
                    let _ = Entry::decode_from(&buf[..(size - i) as usize])
                        .expect_err("decoded from shorter buf");
                }
            }
        }
    }
}
