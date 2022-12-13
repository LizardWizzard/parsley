use std::{
    alloc::{Layout, LayoutError},
    fs::{self, OpenOptions},
    io, mem,
    path::PathBuf,
    ptr,
};

use bitmaps::{Bitmap, Bits, BitsImpl};
use memmap::MmapMut;

use crate::pptr::PPtr;

// TODO do not use KEY_SIZE and VALUE_SIZE separately, for generic allocator they both are just value, so there is no unnecessary separation

// std alloc error is unstable, for now use my own
#[derive(Debug)]
pub struct MyAllocError {}

/// Represents a block of allocated memory returned by an allocator.
// inspired by std::alloc::MemoryBlock but replaced with pptr
#[derive(Debug, Copy, Clone)]
pub struct PMemoryBlock {
    pub ptr: PPtr<u8>,
    pub size: isize,
}

pub unsafe fn map_mut(file_handle: fs::File) -> MmapMut {
    // idea, map to particular address range to determine allocator
    // for structs, but it is unsupported in library, may be this can be useful
    MmapMut::map_mut(&file_handle).unwrap()
}

#[derive(Debug)]
pub struct Slab<const BLOCK_SIZE: usize, const KEY_SIZE: usize, const VALUE_SIZE: usize> {
    allocation_id: isize,
    mmap: MmapMut,
}

impl<const BLOCK_SIZE: usize, const KEY_SIZE: usize, const VALUE_SIZE: usize>
    Slab<BLOCK_SIZE, KEY_SIZE, VALUE_SIZE>
where
    BitsImpl<BLOCK_SIZE>: Bits,
{
    pub fn new(allocation_id: isize, mmap: MmapMut) -> Self {
        let mut instance = Self {
            allocation_id,
            mmap,
        };
        instance.write_empty_bitmap();
        instance
    }

    // TODO fix unwrapping using custom error, possible to be const in some future
    pub fn key_value_layout() -> Result<(Layout, usize), LayoutError> {
        let key_layout = Layout::array::<u8>(KEY_SIZE)?;
        let value_layout = Layout::array::<u8>(VALUE_SIZE)?;
        key_layout.extend(value_layout)
    }

    pub fn layout() -> Result<(Layout, usize), LayoutError> {
        let bitmap_layout = Layout::new::<Bitmap<BLOCK_SIZE>>();
        let data_layout = Layout::repeat(&Self::key_value_layout()?.0, BLOCK_SIZE)?;
        bitmap_layout.extend(data_layout.0)
    }

    pub fn free_slot(&self) -> Option<usize> {
        // FIXME bitmap is not repr(C) so this works only if compiled by same compiler version
        let bitmap = self.mmap.as_ptr() as *const Bitmap<BLOCK_SIZE>;
        // SAFETY: bitmap is initialized by same version of compiler and package and written to storage before such access
        unsafe { bitmap.as_ref()? }.first_false_index()
    }

    pub fn set_bitmap(&mut self, offset: usize, value: bool) {
        let bitmap = self.mmap.as_ptr() as *mut Bitmap<BLOCK_SIZE>;
        unsafe { bitmap.as_mut().unwrap().set(offset, value) };
    }

    fn write_empty_bitmap(&mut self) {
        let empty_bitmap = Bitmap::<BLOCK_SIZE>::new();
        // FIXME cannot find a way to create MaybeUninit from pointer, the following is not ok because bitmap is not initialized yet
        let bitmap = self.mmap.as_ptr() as *mut Bitmap<BLOCK_SIZE>;
        unsafe {
            ptr::copy_nonoverlapping(
                &empty_bitmap as *const Bitmap<BLOCK_SIZE>,
                bitmap,
                mem::size_of::<Bitmap<BLOCK_SIZE>>(),
            );
        }
    }

    // give vptr for offset in the slab
    fn vptr_for_offset(&self, offset: usize) -> *const u8 {
        let layout = Self::key_value_layout()
            .unwrap()
            .0
            .repeat(offset)
            .unwrap()
            .0;
        unsafe { self.mmap.as_ptr().add(layout.size()) }
    }
}

#[derive(Debug)]
pub struct PSlabAlloc<const BLOCK_SIZE: usize, const KEY_SIZE: usize, const VALUE_SIZE: usize> {
    data_dir: PathBuf,
    // mapped files
    slabs: Vec<Slab<BLOCK_SIZE, KEY_SIZE, VALUE_SIZE>>,
    // current allocation id
    current_allocation_id: isize,
    // slab with free slot
    slab_with_free_slot: isize,
}

impl<const BLOCK_SIZE: usize, const KEY_SIZE: usize, const VALUE_SIZE: usize>
    PSlabAlloc<BLOCK_SIZE, KEY_SIZE, VALUE_SIZE>
where
    BitsImpl<{ BLOCK_SIZE }>: Bits,
{
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            slabs: vec![],
            current_allocation_id: 0,
            slab_with_free_slot: 0,
        }
    }

    pub fn from_data_dir(data_dir: PathBuf) -> Result<Self, io::Error> {
        // TODO issue read dir via io_uring
        let mut slabs = vec![];
        for dir_entry in std::fs::read_dir(&data_dir)? {
            let path = dir_entry.unwrap().path();
            let allocation_file_handle = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&path)?;

            let allocation_seqno: isize = path
                .file_stem()
                .unwrap()
                .to_str()
                .expect("file name is always unicode")
                .parse()
                .expect("we generate fname from usize, so it always parsable back");
            slabs.push(Slab::new(allocation_seqno, unsafe {
                map_mut(allocation_file_handle)
            }));
        }
        // Get current seqno. If metadata is empty -> 0 otherwise get max seqno of allocation
        // TODO discard empty slabs
        let current_allocation_id = slabs
            .iter()
            .max_by_key(|item| item.allocation_id)
            .map_or(0, |item| item.allocation_id);
        Ok(Self {
            data_dir,
            slabs,
            current_allocation_id,
            slab_with_free_slot: 0,
        })
    }

    fn make_new_slab(&mut self) -> Slab<BLOCK_SIZE, KEY_SIZE, VALUE_SIZE> {
        self.current_allocation_id += 1;
        let mut path = self.data_dir.clone();
        path.push(format!("{:?}.alloc", self.current_allocation_id));
        let allocation_file_handle = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .unwrap();
        let (slab_layout, _) = Slab::<BLOCK_SIZE, KEY_SIZE, VALUE_SIZE>::layout().unwrap();
        allocation_file_handle
            .set_len(slab_layout.size() as u64)
            .unwrap();

        let mmap = unsafe { map_mut(allocation_file_handle) };
        // unsafe {ptr::write_bytes(mmap.as_mut_ptr(), 0, slab_layout.size())};
        Slab::<BLOCK_SIZE, KEY_SIZE, VALUE_SIZE>::new(self.current_allocation_id, mmap)
    }

    // returns index of the slab in slabs and free slot idx in slab
    fn get_free_slab(&mut self) -> (usize, usize) {
        // if no slabs create new one and return it
        if self.slabs.len() == 0 {
            let slab = self.make_new_slab();
            self.slabs.push(slab);
            return (self.slabs.len() - 1, 0);
        }
        // iterate over existing slabs and try to find free slot
        // TODO maybe keep pointer for optimizations, i e first slab with free slot, advance it on alloc and move backward on dealloc

        for (idx, slab) in self.slabs[self.slab_with_free_slot as usize..]
            .iter()
            .enumerate()
        {
            if let Some(slot) = slab.free_slot() {
                self.slab_with_free_slot = idx as isize;
                return (idx, slot);
            }
        }
        // no free slots in existing slabs, make new slab
        let slab = self.make_new_slab();
        self.slabs.push(slab);
        (self.slabs.len() - 1, 0)
    }

    // There is no layout param because it is fixed to be Slab::key_value_layout()/
    // Allocation is performed in two steps, this makes integration with storage simpler,
    // because storage in that case doesn't have to hold it's own state/
    // Allocator state is sufficient to handle requests and perform a restore.
    // Both steps have to be performed atomically without yielding,
    // because first step allocates needed space, and second one makes it durable.
    // If this does not hold two allocations can possibly overwrite each other
    pub unsafe fn alloc(&mut self) -> Result<PMemoryBlock, MyAllocError> {
        let (slab_idx, offset) = self.get_free_slab();
        let vptr = self.slabs[slab_idx].vptr_for_offset(offset);
        Ok(PMemoryBlock {
            ptr: PPtr::new(slab_idx as u32, offset as u32, vptr as *mut u8),
            size: Slab::<BLOCK_SIZE, KEY_SIZE, VALUE_SIZE>::key_value_layout()
                .unwrap()
                .0
                .size() as isize, // layout can have negative size?
        })
    }

    pub unsafe fn finish_alloc(&mut self, pptr: PPtr<u8>) {
        self.slabs[pptr.ptr.allocation_id as usize].set_bitmap(pptr.ptr.offset as usize, true);
    }

    pub unsafe fn dealloc(&mut self, _pptr: PPtr<u8>, _layoutt: Layout) {
        // NOTE do not forget to decrement self.slab_with_free_slot
        todo!()
    }

    pub fn compact(&mut self) {
        todo!()
    }

    // // TODO impl as iterator
    // pub fn
}
