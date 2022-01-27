use std::{convert::TryInto, env, ops::Range, time::{Duration, Instant}};

use glommio::timer;
use histogram::Histogram;
use rand::{Rng, prelude::ThreadRng};
use rangetree::{RangeMap, RangeSpec};

use crate::{datum::Datum, sharding::shard::format_stats};

use super::shard::{Shard, ShardDatum};

const BOUNDS: ((u8, u8), (u8, u8), (u8, u8)) = ((0, 45), (46, 91), (92, 137));
const BOUND_SIZE: usize = 16;

pub fn get_benchmark_range_map<RangeMapType: RangeMap<Vec<u8>, ShardDatum>>() -> RangeMapType {
    let mut rangemap = RangeMapType::new();

    // let bounds = ((0, 21), (22, 43), (44, 65));
    // let bounds = ((0, 45), (46, 91), (92, 137));
    // let bound_size = 16;

    let range1 = RangeSpec::new(
        vec![BOUNDS.0 .0; BOUND_SIZE],
        vec![BOUNDS.0 .1; BOUND_SIZE],
        ShardDatum::new(0, 0),
    )
    .expect("always ok");
    rangemap.insert(range1);
    // datums.push(Datum::new(0, RangeSpec::trusted_new(range1.start, range1.end, ()), storages[0]));
    let range2 = RangeSpec::new(
        vec![BOUNDS.1 .0; BOUND_SIZE],
        vec![BOUNDS.1 .1; BOUND_SIZE],
        ShardDatum::new(1, 0),
    )
    .expect("always ok");
    rangemap.insert(range2);
    // datums.push(Datum::new(0, RangeSpec::trusted_new(range2.start, range2.end, ()), storages[1]));
    let range3 = RangeSpec::new(
        vec![BOUNDS.2 .0; BOUND_SIZE],
        vec![BOUNDS.2 .1; BOUND_SIZE],
        ShardDatum::new(2, 0),
    )
    .expect("always ok");
    rangemap.insert(range3);
    // datums.push(Datum::new(0, RangeSpec::trusted_new(range3.start, range3.end, ()), storages[2]));
    rangemap
}

pub fn get_local_remote_bounds(shard_id: usize) -> ((u8, u8), ((u8, u8), (u8, u8))) {
    if shard_id == 0 {
        return (BOUNDS.0, (BOUNDS.1, BOUNDS.2));
    } else if shard_id == 1 {
        return (BOUNDS.1, (BOUNDS.0, BOUNDS.2));
    } else if shard_id == 2 {
        return (BOUNDS.2, (BOUNDS.0, BOUNDS.1));
    } else {
        panic!("unexpected");
    }
}

pub fn gen(rng: &mut impl rand::Rng, low: u8, high: u8, n: usize) -> Vec<u8> {
    let mut res = vec![];
    for _ in 0..n {
        res.push(rng.gen_range(Range {
            start: low,
            end: high,
        }))
    }
    res
}

pub fn get_remote_percentage() -> usize {
    let remote_percentage: usize = env::var("REMOTE_PERCENTAGE").unwrap().parse().unwrap();
    log::info!("using remote percentage={:}", remote_percentage);
    remote_percentage
}

pub fn get_write_percentage(shard_id: usize, workload: &Workload) -> usize {
    let mut write_percentage: usize = env::var("WRITE_PERCENTAGE").unwrap().parse().unwrap();
    let workloads = [
        Workload::StorageWorkloadA,
        Workload::StorageWorkloadB,
        Workload::StorageWorkloadC,
    ];
    if workloads.contains(workload) && shard_id == 1 {
        write_percentage = 100 - write_percentage
    }
    log::info!(
        "shard={:} using write percentage={:}",
        shard_id,
        write_percentage
    );
    write_percentage
}

pub fn gen_three(rng: &mut impl rand::Rng, low: u8, high: u8) -> (u8, u8, u8) {
    (
        rng.gen_range(Range {
            start: low,
            end: high,
        }),
        rng.gen_range(Range {
            start: low,
            end: high,
        }),
        rng.gen_range(Range {
            start: low,
            end: high,
        }),
    )
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Workload {
    ReadLocal100,
    ReadLocalRemote,
    WriteLocal100,
    WriteLocalRemote,
    StorageWorkloadA,
    StorageWorkloadB,
    StorageWorkloadC,
    PMemStorageWorkload,
}

impl Workload {
    pub fn from_env() -> Self {
        let name = env::var("WORKLOAD").unwrap();
        match name.as_ref() {
            "ReadLocal100" => Self::ReadLocal100,
            "ReadLocalRemote" => Self::ReadLocalRemote,
            "WriteLocal100" => Self::WriteLocal100,
            "WriteLocalRemote" => Self::WriteLocalRemote,
            "StorageWorkloadA" => Self::StorageWorkloadA,
            "StorageWorkloadB" => Self::StorageWorkloadB,
            "StorageWorkloadC" => Self::StorageWorkloadC,
            "PMemStorageWorkload" => Self::PMemStorageWorkload,
            _ => panic!("got unexpected workload: {:}", name),
        }
    }
}

pub struct Ctx<RangeMapType: RangeMap<Vec<u8>, ShardDatum>> {
    pub shard_id: usize,
    pub num_queries: usize,
    pub key_length: usize,
    pub value_length: usize,
    pub rng: ThreadRng,
    pub datum: Datum,
    pub instance: Shard<RangeMapType>,
    pub histogram: Histogram,
    pub workload: Workload,
}

impl<RangeMapType: RangeMap<Vec<u8>, ShardDatum>> Ctx<RangeMapType> {
    pub fn new(
        shard_id: usize,
        num_queries: usize,
        key_length: usize,
        value_length: usize,
        rng: ThreadRng,
        datum: Datum,
        instance: Shard<RangeMapType>,
        histogram: Histogram,
        workload: Workload,
    ) -> Self {
        Self {
            shard_id,
            num_queries,
            key_length,
            rng,
            datum,
            instance,
            value_length,
            histogram,
            workload,
        }
    }
}

pub struct ShardBenchExt<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + 'static> {
    pub shard: Shard<RangeMapType>,
}
impl<RangeMapType> ShardBenchExt<RangeMapType> where
    RangeMapType: RangeMap<Vec<u8>, ShardDatum> + 'static
{
    pub fn new(shard: Shard<RangeMapType>) -> Self { Self { shard } }

    pub async fn benchmark_load_data(
        &mut self,
        shard_id: usize,
        datum: &Datum,
        key_length: usize,
        value_length: usize,
    ) -> usize {
        log::info!("data loading started for shard {:?}", shard_id);
        let mut ctr: usize = 0;
        for first in datum.range.start[0]..datum.range.end[0] {
            for second in datum.range.start[0]..datum.range.end[0] {
                for third in datum.range.start[0]..datum.range.end[0] {
                    let mut key = vec![255; key_length];
                    key[0] = first;
                    key[1] = second;
                    key[2] = third;
                    self.shard.set(key, vec![0; value_length]).await;
                    ctr += 1;
                }
            }
        }
        log::info!(
            "data loading finished for shard {:?} number of keys: {:?}",
            shard_id,
            ctr
        );
        ctr
    }

    pub async fn benchmark_read_local_100(&mut self, ctx: &mut Ctx<RangeMapType>) {
        for _ in 0..ctx.num_queries {
            // let key = gen(
            //     &mut rng,
            //     datum.range.start[0],
            //     datum.range.end[0],
            //     key_length,
            // );
            let (first, second, third) = gen_three(
                &mut ctx.rng,
                ctx.datum.range.start[0],
                ctx.datum.range.end[0],
            );
            let mut key = vec![255; ctx.key_length];
            key[0] = first;
            key[1] = second;
            key[2] = third;
            let t0 = Instant::now();
            self.shard.get(key).await;
            ctx.histogram
                .increment(t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
    }

    pub async fn benchmark_write_local_100(&mut self, ctx: &mut Ctx<RangeMapType>) {
        for _ in 0..ctx.num_queries {
            let (first, second, third) = gen_three(
                &mut ctx.rng,
                ctx.datum.range.start[0],
                ctx.datum.range.end[0],
            );
            let mut key = vec![255; ctx.key_length];
            key[0] = first;
            key[1] = second;
            key[2] = third;
            let t0 = Instant::now();
            self.shard.set(key, vec![0; ctx.value_length]).await;
            ctx.histogram
                .increment(t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
    }
    
    pub async fn benchmark_read_local_remote(&mut self, ctx: &mut Ctx<RangeMapType>) {
        let (local_bounds, remote_bounds) = get_local_remote_bounds(ctx.shard_id);
        let remote_percentage = get_remote_percentage();
    
        // let inprogress_limit = 100;
        // let semaphore = Semaphore::new(inprogress_limit);
    
        for _ in 0..ctx.num_queries {
            let value = ctx.rng.gen_range(Range { start: 0, end: 100 });
            let target_bounds = if value < remote_percentage {
                // going remote
                if value < remote_percentage / 2 {
                    remote_bounds.0
                } else {
                    remote_bounds.1
                }
            } else {
                local_bounds
            };
            let (first, second, third) = gen_three(&mut ctx.rng, target_bounds.0, target_bounds.1);
            let mut key = vec![255; ctx.key_length];
            key[0] = first;
            key[1] = second;
            key[2] = third;
            let t0 = Instant::now();
            // glommio::spawn_local(async move {
            //     semaphore.acquire(1).await;
            //     Shard::get(&ctx.instance, key).await;
            //     semaphore.signal(1);
            // }).detach();
            self.shard.get(key).await;
            ctx.histogram
                .increment(t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
    }
    
    pub async fn benchmark_write_local_remote(&mut self, ctx: &mut Ctx<RangeMapType>) {
        let (local_bounds, remote_bounds) = get_local_remote_bounds(ctx.shard_id);
        let remote_percentage = get_remote_percentage();
    
        for _ in 0..ctx.num_queries {
            let value = ctx.rng.gen_range(Range { start: 0, end: 100 });
            let target_bounds = if value < remote_percentage {
                // going remote
                if value < remote_percentage / 2 {
                    remote_bounds.0
                } else {
                    remote_bounds.1
                }
            } else {
                local_bounds
            };
            let (first, second, third) = gen_three(&mut ctx.rng, target_bounds.0, target_bounds.1);
            let mut key = vec![255; ctx.key_length];
            key[0] = first;
            key[1] = second;
            key[2] = third;
            let t0 = Instant::now();
            self.shard.set(key, vec![0; ctx.value_length]).await;
            ctx.histogram
                .increment(t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
    }
    
    pub async fn benchmark_read_write_local_remote(&mut self, ctx: &mut Ctx<RangeMapType>) {
        let (local_bounds, remote_bounds) = get_local_remote_bounds(ctx.shard_id);
        let remote_percentage = get_remote_percentage();
        let write_percentage = get_write_percentage(ctx.shard_id, &ctx.workload);
    
        for _ in 0..ctx.num_queries {
            let local_remote_value = ctx.rng.gen_range(Range { start: 0, end: 100 });
            let target_bounds = if local_remote_value < remote_percentage {
                // going remote
                if local_remote_value < remote_percentage / 2 {
                    remote_bounds.0
                } else {
                    remote_bounds.1
                }
            } else {
                local_bounds
            };
            let read_write_value = ctx.rng.gen_range(Range { start: 0, end: 100 });
    
            let (first, second, third) = gen_three(&mut ctx.rng, target_bounds.0, target_bounds.1);
            let mut key = vec![255; ctx.key_length];
            key[0] = first;
            key[1] = second;
            key[2] = third;
            let t0 = Instant::now();
            if read_write_value < write_percentage {
                self.shard.set(key, vec![0; ctx.value_length]).await;
            } else {
                self.shard.get(key).await;
            }
            ctx.histogram
                .increment(t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
    }
    
    pub async fn benchmark(&mut self) {
        if env::var("BENCHMARK").is_err() {
            return;
        }
        // wait for update of rangemap
        loop {
            if self.shard.state.borrow().datums.len() == 0 {
                glommio::yield_if_needed().await;
            } else {
                break;
            }
        }
    
        // fill data
        let mut datums = self.shard.state.borrow().datums.clone();
        let rng = rand::thread_rng();
        let key_length = 16;
        let value_length = 256;
        let shard_id = self.shard.state.borrow().id;
        assert_eq!(datums.len(), 1);
        let datum = datums.pop().expect("checked");
    
        let number_of_keys =
            self.benchmark_load_data(shard_id, &datum, key_length, value_length).await;
        log::info!("using number_of_keys={:}", number_of_keys);
        // wait for others to be ready
        timer::sleep(Duration::from_millis(300)).await;
        let histo = Histogram::new();
    
        let num_queries: usize = env::var("NUM_QUERIES").unwrap().parse().unwrap();
        log::info!("using num queries={:}", num_queries);
    
        let t0 = Instant::now();
        let workload = Workload::from_env();
        let mut ctx = Ctx::new(
            shard_id,
            num_queries,
            key_length,
            value_length,
            rng,
            datum,
            self.shard.clone(),
            histo,
            workload,
        );
        match workload {
            Workload::ReadLocal100 => self.benchmark_read_local_100(&mut ctx).await,
            Workload::ReadLocalRemote => self.benchmark_read_local_remote(&mut ctx).await,
            Workload::WriteLocal100 => self.benchmark_write_local_100(&mut ctx).await,
            Workload::WriteLocalRemote => self.benchmark_write_local_remote(&mut ctx).await,
            Workload::StorageWorkloadA => self.benchmark_read_write_local_remote(&mut ctx).await,
            Workload::StorageWorkloadB => self.benchmark_read_write_local_remote(&mut ctx).await,
            Workload::StorageWorkloadC => self.benchmark_read_write_local_remote(&mut ctx).await,
            Workload::PMemStorageWorkload => self.benchmark_read_write_local_remote(&mut ctx).await,
        }
        println!(
            "{:}",
            format_stats(&self.shard.state.borrow().stats, shard_id, t0.elapsed(), ctx),
        );
        timer::sleep(Duration::from_secs(2)).await;
    }
}






