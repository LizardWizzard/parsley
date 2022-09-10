use core::sharding::{
    builder::ShardBuilder,
    shard::{Placement, ShardDatum},
};
use log::LevelFilter;
use rangetree::rangevec::RangeVec;
use std::io::Result;

fn main() -> Result<()> {
    env_logger::builder().filter_level(LevelFilter::Info).init();
    log::info!("Starting db");

    let shard_builder: ShardBuilder<RangeVec<Vec<u8>, ShardDatum>> =
        ShardBuilder::new(Placement::first_n_cores(3), 10);
    shard_builder
        .spawn()
        .into_iter()
        .for_each(|handle| handle.join().expect("Executor failed"));
    Ok(())
}
