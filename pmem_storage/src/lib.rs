#![feature(alloc_layout_extra)] // needed for handy Layout manipulations

use std::{collections::BTreeMap, io, path::PathBuf, ptr};

use alloc::PSlabAlloc;
use bitmaps::{Bits, BitsImpl};
use pptr::PPtr;

use crate::persist::persist_range;

use std::time::{Duration, Instant};

pub mod alloc;
pub mod persist;
pub mod pptr;

#[derive(Debug)]
pub struct Config {
    pub base_dir: PathBuf,
}

impl Config {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }
}

pub fn busy_sleep(dur: Duration) {
    let t0 = Instant::now();
    loop {
        if t0.elapsed() > dur {
            break;
        }
    }
}

#[derive(Debug)]
pub struct PMemStorage<const BLOCK_SIZE: usize, const KEY_SIZE: usize, const VALUE_SIZE: usize> {
    alloc: PSlabAlloc<BLOCK_SIZE, KEY_SIZE, VALUE_SIZE>,
    // search index store key in a vec and pptr points to value, not key value pair
    search_index: BTreeMap<Vec<u8>, PPtr<[u8; VALUE_SIZE]>>,
}

impl<const BLOCK_SIZE: usize, const KEY_SIZE: usize, const VALUE_SIZE: usize>
    PMemStorage<BLOCK_SIZE, KEY_SIZE, VALUE_SIZE>
where
    BitsImpl<{ BLOCK_SIZE }>: Bits,
{
    pub fn new(config: Config) -> Self {
        let data_dir = config.base_dir.clone();
        Self {
            alloc: PSlabAlloc::new(data_dir),
            search_index: BTreeMap::new(),
        }
    }

    pub fn from_data_dir(config: Config) -> Result<Self, io::Error> {
        let search_index = BTreeMap::new();
        // restore alloc, iterate over allocations, fill btree map
        // TODO duplicate keys, free one with lower allocation id
        // TODO remove dangling keys?
        // todo!()
        let alloc = PSlabAlloc::new(config.base_dir.clone());
        Ok(Self {
            alloc,
            search_index,
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // SAFETY: the value of VALUE_SIZE was previously safely written to storage, so it is safe to read it back
        match self.search_index.get(key) {
            Some(pptr) => Some(unsafe { ptr::read(pptr.vptr).to_vec() }),
            None => None,
        }
    }

    // in current architecture set is always an allocation
    // new one is allocated, persisted, and later previous deallocated
    // this is needed because in case of a failure in the middle of writing we will get corrupted value written without a way to restore consistent value
    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let new_allocation = unsafe { self.alloc.alloc().expect("Allocation failed") };
        // write key
        // FIXME allocation already marked as successful and currently there is no way to undo it
        unsafe {
            ptr::copy_nonoverlapping(key.as_ptr(), new_allocation.ptr.vptr as *mut u8, KEY_SIZE)
        };
        // write value
        let value_ptr = unsafe { new_allocation.ptr.vptr.add(KEY_SIZE) as *mut u8 };
        unsafe { ptr::copy_nonoverlapping(value.as_ptr(), value_ptr, VALUE_SIZE) }

        // persist allocation
        unsafe { persist_range(new_allocation.ptr.vptr, new_allocation.size) };
        // emulating nvm latency as described in HiKV paper:
        // assuming dram latency 60 ns, 3dxpoint's: 60 * 10, so busy sleep for 540 * size
        let for_sleep = Duration::from_nanos((new_allocation.size * 540) as u64);
        busy_sleep(for_sleep);

        // TODO maybe in the future make alloc guard which on drop flips needed bit in bitmap
        unsafe { self.alloc.finish_alloc(new_allocation.ptr) };
        // if exists in search index, dealloc previous value

        // add to search index
        self.search_index.insert(
            key,
            PPtr::new(
                new_allocation.ptr.ptr.allocation_id,
                new_allocation.ptr.ptr.offset,
                value_ptr as *mut [u8; VALUE_SIZE],
            ),
        );
    }

    pub fn delete(&mut self, _key: &[u8]) -> Option<()> {
        // NOTE: do not forget to pass to dealloc pptr to key value pair, however it wil be ok even without conversion
        // because persistent part of pointer is the same
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{fs, path::PathBuf, str::FromStr};

    use crate::{Config, PMemStorage};

    #[test]
    fn it_really_works() {
        // let mut data_dir = env::current_dir().unwrap();
        // data_dir.push("data");
        let data_dir = PathBuf::from_str("/tmp/ramdisk/data").unwrap();
        dbg!(&data_dir);
        let config = Config::new(data_dir);
        for dir_entry in fs::read_dir(&config.base_dir).unwrap() {
            let path = dir_entry.unwrap().path();
            if path.is_dir() {
                fs::remove_dir_all(path).unwrap();
            } else {
                fs::remove_file(path).unwrap();
            }
        }
        let mut storage = PMemStorage::<64, 16, 256>::new(config);
        for i in 0..220 {
            storage.set(vec![i; 16], vec![i; 256]);
            let r = storage.get(&vec![i; 16]);
            assert!(r.is_some());
        }
    }
}
