#![feature(alloc_layout_extra)]
use std::mem;

use bitmaps::Bitmap;
use pmem_storage::alloc::Slab;

fn main() {
    // let (_, offset) = Slab::<160, 16, 256>::key_value_layout().unwrap();
    // dbg!(offset);
    let (layout, offset) = Slab::<160, 16, 256>::key_value_layout().unwrap();
    // dbg!(layout.size());
    let layout = Slab::<160, 16, 256>::key_value_layout().unwrap().0.repeat(2).unwrap().0;
    dbg!(layout.size());
    dbg!(mem::size_of::<*const u8>());
    // let mut b = Bitmap::<10>::new();
    // b.set(2, true);
    // dbg!(&b);
}