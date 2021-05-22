use std::{ptr, mem};
// Represents persistent part of a persistent pointer
// PPtr is divided to exclude saving calculated ptr in persistent part
#[derive(Debug, Copy, Clone)]
pub struct PPtrPersistent {
    pub allocation_id: u32,
    pub offset: u32,
    // for now dummylocator is used which has one allocation
                            //   TODO offset and allocation id can fit inside one u64, also alignment to cacheline can be made
                              // offset: usize,
}

impl PPtrPersistent {
    pub fn dangling() -> Self {
        return Self { allocation_id: 0, offset: 0 };
    }

    pub fn is_dangling(self) -> bool {
        self.allocation_id == 0
    }
}

#[derive(Debug, Copy, Clone)]
pub struct PPtr<T> {
    pub ptr: PPtrPersistent,
    pub vptr: *const T,
}

// TODO !Send?
// TODO NonNull or Unique?

impl<T> PPtr<T> {
    // loads PPtr from persistent representation calculating vptr
    // for dummy allocator it is simplified because there is no allocation id
    // in the future may be pass allocator to resolve allocation id into it's base
    // should it be unsafe?
    // pub fn load(pptr: PPtrPersistent, base: *mut u8) -> Self {
    //     return PPtr {
    //         ptr: pptr,
    //         vptr: base,
    //     };
    // }

    pub fn new(allocation_id: u32, offset: u32, vptr: *mut T) -> Self {
        // it is needed because later we assume for such a thing when thinking about atomicity
        debug_assert_eq!(mem::size_of::<Self>(), 16); // can it be compile time assert?
        PPtr {
            ptr: PPtrPersistent { allocation_id, offset },
            vptr: vptr as *const T,
        }
    }

    pub fn dangling() -> Self {
        PPtr {
            ptr: PPtrPersistent::dangling(),
            vptr: ptr::null(),
        }
    }

    pub fn is_dangling(self) -> bool {
        self.ptr.is_dangling()
    }
}
