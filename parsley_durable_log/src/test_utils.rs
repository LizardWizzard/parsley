pub mod kv_record {
    use std::{convert::TryInto, fmt::Display, io::Write, mem};

    use crc32fast::Hasher;

    use crate::{
        reader::{checksummed_read_buf, read_buf, WalReadError},
        WalWritable,
    };

    pub const RECORD_HEADER_SIZE: u64 = (mem::size_of::<u64>() * 2) as u64;
    pub const SIZE_OF_SIZE: u64 = mem::size_of::<u64>() as u64;
    pub const CHECKSUM_SIZE: u64 = mem::size_of::<u32>() as u64;

    pub struct KVWalRecord<'rec> {
        pub key: &'rec [u8],
        pub value: &'rec [u8],
    }

    impl<'rec> WalWritable<'rec> for KVWalRecord<'rec> {
        fn size(&self) -> u64 {
            RECORD_HEADER_SIZE + (self.key.len() + self.value.len()) as u64 + CHECKSUM_SIZE
        }

        fn serialize_into(&self, mut buf: &mut [u8]) {
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
            debug_assert_eq!(initial_pos - buf.len(), self.size() as usize);
        }

        fn deserialize_from<'buf>(buf: &'buf [u8]) -> Result<(u64, Self), WalReadError>
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
            let rec = KVWalRecord { key, value };
            Ok((pos, rec))
        }
    }

    impl Display for KVWalRecord<'_> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!("record.key[0] {}", self.key[0]))
        }
    }
}
