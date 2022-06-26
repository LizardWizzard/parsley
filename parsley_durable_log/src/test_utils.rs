use std::{
    fs,
    path::{Path, PathBuf},
    str::FromStr,
};

use glommio::io::Directory;

pub fn test_dir(subdir_name: impl AsRef<Path>) -> PathBuf {
    let mut path = PathBuf::from_str(env!("CARGO_MANIFEST_DIR")).unwrap();
    // let mut path = PathBuf::from_str("/tmp").unwrap();
    path.push("test_data");
    path.push(subdir_name.as_ref());

    // consume value so there is no warning about must_use
    // glommio's directory doesnt have rm method, so let it be as it is
    fs::remove_dir_all(&path).ok();

    // and there is no create dir all in glommio too
    fs::create_dir_all(&path).expect("failed to create wal dir");

    path
}

pub async fn test_dir_open(subdir_name: impl AsRef<Path>) -> (Directory, PathBuf) {
    let path = test_dir(subdir_name);
    let dir = Directory::open(&path)
        .await
        .expect("failed to create wal dir for test directory");
    dir.sync().await.expect("failed to sync wal dir");
    (dir, path)
}

pub mod bench {
    use histogram::Histogram;

    pub fn display_histogram(name: &'static str, h: Histogram, parser: impl Fn(u64) -> String) {
        println!("{name}.min={}", parser(h.minimum().unwrap()));
        println!("{name}.max={}", parser(h.maximum().unwrap()));
        println!("{name}.stddev={}", parser(h.stddev().unwrap()));
        println!("{name}.mean={}", parser(h.mean().unwrap()));
        for percentile in (0..95).step_by(5) {
            println!(
                "{name}.p{}={}",
                percentile,
                parser(h.percentile(percentile as f64).unwrap())
            );
        }
        println!("{name}.p{}={}", 99.9, parser(h.percentile(99.9 as f64).unwrap()));
    }
}

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
