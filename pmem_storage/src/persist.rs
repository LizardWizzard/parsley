use core::arch::x86_64 as arch;
use std::mem;

pub unsafe fn persist(p: *const u8) {
    arch::_mm_sfence();
    arch::_mm_clflush(p);
    arch::_mm_sfence();
}

// TODO maybe it is possible to place blocks aligned by cacheline and issue one flush per cache line

pub unsafe fn persist_range(p: *const u8, size: isize) {
    for offset in (0..size).step_by(mem::size_of::<*const u8>()) {
        persist(p.offset(offset));
    }
}
