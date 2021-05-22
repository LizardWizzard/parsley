use core::{
    sharding::{
        shard::{Placement, ShardDatum},
        builder::ShardBuilder,
    }
};
use log::LevelFilter;
use rangetree::RangeVec;
use std::io::Result;

fn main() -> Result<()> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();
    log::info!("Starting db");

    let shard_builder: ShardBuilder<RangeVec<Vec<u8>, ShardDatum>> =
        ShardBuilder::new(Placement::first_n_cores(3), 10);
    shard_builder
        .spawn()
        .into_iter()
        .for_each(|handle| handle.join().expect("Executor failed"));
    Ok(())
}
