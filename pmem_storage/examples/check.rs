#![feature(alloc_layout_extra)]

use pmem_storage::alloc::Slab;

fn main() {
    let (layout, offset) = Slab::<160, 16, 256>::key_value_layout().unwrap();
    dbg!(layout.size(), offset);
    let layout = Slab::<160, 16, 256>::key_value_layout()
        .unwrap()
        .0
        .repeat(2)
        .unwrap()
        .0;
    dbg!(layout.size());
}
