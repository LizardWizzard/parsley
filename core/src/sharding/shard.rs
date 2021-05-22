use futures::future::join_all;
use glommio::{channels::shared_channel::ConnectedReceiver, sync::Semaphore, timer};
use histogram::Histogram;
use rand::{prelude::ThreadRng, Rng};
use rangetree::{RangeMap, RangeSpec, ReadonlyRangeMap};
use std::{
    cell::RefCell,
    collections::HashMap,
    convert::TryInto,
    env,
    ops::Range,
    rc::Rc,
    sync::Arc,
    task::Waker,
    time::{Duration, Instant},
    vec,
};

use glommio::{channels::shared_channel::ConnectedSender, prelude::*};
use std::ops::Deref;

use crate::datum::{
    BTreeModelStorage, Datum, DatumStorage, LSMTreeModelStorage, MemoryStorage, PMemDatumStorage,
};

use super::enums::{Data, HealthcheckError, Message, RestoreError, RestoreStatus, ShardRole};
use super::forward_futures::{ForwardedGet, ForwardedSet};

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

pub struct Ctx<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + PartialEq> {
    shard_id: usize,
    num_queries: usize,
    key_length: usize,
    value_length: usize,
    rng: ThreadRng,
    datum: Datum,
    instance: Rc<RefCell<Shard<RangeMapType>>>,
    histogram: Histogram,
    workload: Workload,
}

impl<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + PartialEq> Ctx<RangeMapType> {
    fn new(
        shard_id: usize,
        num_queries: usize,
        key_length: usize,
        value_length: usize,
        rng: ThreadRng,
        datum: Datum,
        instance: Rc<RefCell<Shard<RangeMapType>>>,
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct ShardDatum {
    pub shard_id: usize,
    pub datum_id: usize,
}

impl ShardDatum {
    pub fn new(shard_id: usize, datum_id: usize) -> Self {
        Self { shard_id, datum_id }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Placement {
    pub core: usize, // for now just core
}

impl Placement {
    pub fn new(core: usize) -> Self {
        Self { core }
    }

    pub fn first_n_cores(n: usize) -> Vec<Self> {
        (0..n).map(|core| Self::new(core)).collect()
    }
}

pub fn get_initial_range_map<RangeMapType: RangeMap<Vec<u8>, ShardDatum>>(
    shard_id: usize,
    datum_id: usize,
) -> (RangeMapType, RangeSpec<Vec<u8>, ShardDatum>) {
    // restore or default
    let mut rangemap = RangeMapType::new();
    let default_range = RangeSpec::new(
        vec![0, 0, 0, 0, 0, 0, 0],
        vec![255, 255, 255, 255, 255, 255, 255],
        ShardDatum::new(shard_id, datum_id), // FIXME insert default datum on master shard in case its a clean start
    )
    .expect("always ok");
    rangemap.insert(default_range.clone());
    (rangemap, default_range)
}

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

#[derive(Debug, Default)]
pub struct Stats {
    // served by direct calls on this shard
    pub served_own_gets: usize,
    pub served_own_sets: usize,
    pub served_own_deletes: usize,
    // served requests from other shard
    pub served_forwarded_gets: usize,
    pub served_forwarded_sets: usize,
    pub served_forwarded_deletes: usize,
    // requests forwarded to serve by another shard
    pub forwarded_gets: usize,
    pub forwarded_sets: usize,
    pub forwarded_deletes: usize,
}

// FIXME kind of custom inefficient Display :)
pub fn format_stats<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + PartialEq>(
    stat: &Stats,
    shard_id: usize,
    elapsed: Duration,
    ctx: Ctx<RangeMapType>,
) -> String {
    let own = format!(
        "served_own_gets={:} served_own_sets={:} served_own_deletes={:}",
        stat.served_own_gets,
        stat.served_own_sets - 91125,
        stat.served_own_deletes
    );
    let served_forwarded = format!(
        "served_forwarded_gets={:} served_forwarded_sets={:} served_forwarded_deletes={:}",
        stat.served_forwarded_gets, stat.served_forwarded_sets, stat.served_forwarded_deletes
    );
    let forwarded = format!(
        "forwarded_gets={:} forwarded_sets={:} forwarded_deletes={:}",
        stat.forwarded_gets, stat.forwarded_sets, stat.forwarded_deletes
    );
    let hist = format!("mean={:}", ctx.histogram.mean().unwrap());
    format!(
        "stats shard_id={:} elapsed={:} {:} {:} {:} {:}",
        shard_id,
        elapsed.as_secs_f64(),
        own,
        served_forwarded,
        forwarded,
        hist,
    )
}

pub struct Shard<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + PartialEq> {
    pub id: usize,
    pub role: ShardRole,
    pub rangemap: Arc<RangeMapType::FROZEN>,
    pub datums: Vec<Datum>,
    pub senders: Vec<ConnectedSender<Message<Data<RangeMapType>>>>,
    pub storages: Vec<DatumStorage>,
    // keep identifiers of forwarded requests
    // potentially later hashmap with value of async write implementor or sink to send actual result top client
    pub forwarded_get: HashMap<usize, (Option<Waker>, Option<Option<Vec<u8>>>)>,
    pub forwarded_set: HashMap<usize, (Option<Waker>, Option<()>)>,
    pub forwarded_del: HashMap<usize, Option<Waker>>,
    pub stats: Stats,
    op_counter: usize,
}

impl<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + 'static + PartialEq> Shard<RangeMapType> {
    pub fn new(id: usize, senders: Vec<ConnectedSender<Message<Data<RangeMapType>>>>) -> Self {
        let role = {
            if id == 0 {
                ShardRole::Master
            } else {
                ShardRole::Worker
            }
        };
        log::info!("shard id {:} role {:?}", id, role);
        let datums = vec![];
        let rangemap: Arc<RangeMapType::FROZEN> = Arc::new(RangeMapType::new().freeze());
        Self {
            id,
            role,
            rangemap,
            datums,
            senders,
            storages: vec![
                DatumStorage::MemoryStorage(Rc::new(RefCell::new(MemoryStorage::new()))),
                DatumStorage::BTreeModelStorage(Rc::new(RefCell::new(BTreeModelStorage::new()))),
                DatumStorage::LSMTreeModelStorage(Rc::new(
                    RefCell::new(LSMTreeModelStorage::new()),
                )),
                DatumStorage::PMemStorage(Rc::new(RefCell::new(PMemDatumStorage::new(id)))),
            ],
            stats: Stats::default(),
            forwarded_get: HashMap::new(),
            forwarded_set: HashMap::new(),
            forwarded_del: HashMap::new(),
            op_counter: 0,
        }
    }

    pub fn next_op(&mut self) -> usize {
        self.op_counter += 1;
        self.op_counter
    }

    async fn send_to(&self, idx: usize, msg: Data<RangeMapType>) {
        self.senders[idx]
            .send(Message::new(self.id, idx, msg))
            .await
            .expect(&format!("failed sendto. src {:?} dst {:?}", self.id, idx))
    }

    async fn healthcheck(
        &self,
        shard_receivers: &Vec<ConnectedReceiver<Message<Data<RangeMapType>>>>,
    ) -> Result<(), HealthcheckError> {
        for (idx, sndr) in self
            .senders
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != self.id)
        {
            sndr.send(Message::new(self.id, idx, Data::Log(String::from("hello"))))
                .await
                .map_err(|_| {
                    HealthcheckError::FailedToSend(format!(
                        "Healthcheck failed to send from {:} to {:}",
                        self.id, idx
                    ))
                })?
        }
        for (idx, sndr) in shard_receivers
            .iter()
            .enumerate()
            .filter(|(idx, _)| *idx != self.id)
        {
            let msg = sndr
                .recv()
                .await
                .ok_or(HealthcheckError::FailedToRecv(format!(
                    "Healthcheck failed to send from {:} to {:}",
                    self.id, idx
                )))?;
            if msg.source_id != idx {
                Err(HealthcheckError::IncorrectRecv(format!(
                    "Healthcheck failed correctly recv. Expected source {:} got {:}",
                    idx, msg.source_id
                )))?
            }
            if msg.source_id != idx {
                Err(HealthcheckError::IncorrectRecv(format!(
                    "Healthcheck failed correctly recv. Expected dst {:} got {:}",
                    self.id, msg.dst_id
                )))?
            }
        }
        Ok(())
    }

    async fn restore(&mut self) -> Result<RestoreStatus, RestoreError> {
        // restore data for shards
        // TODO
        // if master restores rangemap
        Ok(RestoreStatus::Empty)
    }

    pub async fn serve(
        mut self,
        shard_receivers: Vec<ConnectedReceiver<Message<Data<RangeMapType>>>>,
    ) -> Rc<RefCell<Shard<RangeMapType>>> {
        self.healthcheck(&shard_receivers).await.unwrap();

        match self.restore().await.unwrap() {
            RestoreStatus::Data => {}
            RestoreStatus::Empty => {
                if self.role == ShardRole::Master {
                    // default new datum params
                    // let datum_id = self.datums.len();
                    // // get default rangemap
                    // let (rangemap, rangespec) =
                    //     get_initial_range_map::<RangeMapType>(self.id, datum_id);

                    // // create datum with default range
                    // let datum = Datum::new(
                    //     datum_id,
                    //     RangeSpec::trusted_new(rangespec.start, rangespec.end, ()),
                    //     self.storages[0].clone(),
                    // );
                    // self.datums.push(datum);
                    let rangemap = get_benchmark_range_map::<RangeMapType>();

                    // FIXME redundant clone
                    for item in rangemap.clone().into_iter() {
                        if item.data.shard_id == self.id {
                            let storage = self.benchmark_get_datum_storage();
                            self.datums.push(Datum::new(
                                self.datums.len(),
                                RangeSpec::trusted_new(item.start, item.end, ()),
                                storage,
                            ))
                        }
                    }

                    // broadcast an update to others
                    self.rangemap = Arc::new(rangemap.freeze());
                    log::info!("broadcasting rangemap update");
                    for (idx, dest) in self
                        .senders
                        .iter()
                        .enumerate()
                        .filter(|(idx, _)| *idx != self.id)
                    {
                        dest.send(Message::new(
                            self.id,
                            idx,
                            Data::UpdateRangeMap(self.rangemap.clone()),
                        ))
                        .await
                        .expect("send should'nt fail")
                    }
                }
            }
        }
        // using rc refcell version of self. Tasks can mutably borrow self if mutable reference doesnt cross await boundary
        // since shard is thread local this is assumed to be safe
        let new_self = Rc::new(RefCell::new(self));
        // spawn consumer from other shards
        Local::local(Self::consume_from_other_shards(
            new_self.clone(),
            shard_receivers,
        ))
        .detach();
        // Local::executor_stats();
        // form benchmark rangemap
        // load data
        // generate data
        // measure
        // .await;
        // setup benchmark
        // let (sender, receiver) = local_channel::new_bounded::<u32>(100);
        // Local::local(Self::benchmark(new_self.clone()))
        // .detach()
        // .await;
        Local::later().await;
        Self::benchmark(new_self.clone()).await;
        new_self
    }

    fn benchmark_get_datum_storage(&self) -> DatumStorage {
        let workload_param = env::var("WORKLOAD");
        let storage = match workload_param {
            Err(_) => self.storages[0].clone(),
            Ok(workload) => {
                if workload == "StorageWorkloadA" {
                    // Btree
                    self.storages[1].clone()
                } else if workload == "StorageWorkloadB" {
                    // LSM
                    self.storages[2].clone()
                } else if workload == "StorageWorkloadC" {
                    if self.id == 1 {
                        // LSM
                        self.storages[2].clone()
                    } else {
                        // btree
                        self.storages[1].clone()
                    }
                } else if workload == "PMemStorageWorkload" {
                    self.storages[3].clone()
                } else {
                    log::info!("no workload special storage provided, using default one");
                    self.storages[0].clone()
                    // panic!("unexpected workload")
                }
            }
        };
        log::info!("shard {:} storage kind {:}", self.id, storage.get_kind());
        storage
    }

    async fn benchmark_load_data(
        instance: &Rc<RefCell<Self>>,
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
                    Shard::set(&instance, key, vec![0; value_length]).await;
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

    async fn benchmark_read_local_100(ctx: &mut Ctx<RangeMapType>) {
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
            Shard::get(&ctx.instance, key).await;
            ctx.histogram
                .increment(t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
    }

    async fn benchmark_write_local_100(ctx: &mut Ctx<RangeMapType>) {
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
            Shard::set(&ctx.instance, key, vec![0; ctx.value_length]).await;
            ctx.histogram
                .increment(t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
    }

    async fn benchmark_read_local_remote(ctx: &mut Ctx<RangeMapType>) {
        let (local_bounds, remote_bounds) = get_local_remote_bounds(ctx.shard_id);
        let remote_percentage = get_remote_percentage();

        let inprogress_limit = 100;
        let semaphore = Semaphore::new(inprogress_limit);

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
            // Local::local(async move {
            //     semaphore.acquire(1).await;
            //     Shard::get(&ctx.instance, key).await;
            //     semaphore.signal(1);
            // }).detach();
            Shard::get(&ctx.instance, key).await;
            ctx.histogram
                .increment(t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
    }

    async fn benchmark_write_local_remote(ctx: &mut Ctx<RangeMapType>) {
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
            Shard::set(&ctx.instance, key, vec![0; ctx.value_length]).await;
            ctx.histogram
                .increment(t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
    }

    async fn benchmark_read_write_local_remote(ctx: &mut Ctx<RangeMapType>) {
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
                Shard::set(&ctx.instance, key, vec![0; ctx.value_length]).await;
            } else {
                Shard::get(&ctx.instance, key).await;
            }
            ctx.histogram
                .increment(t0.elapsed().as_micros().try_into().unwrap())
                .unwrap();
        }
    }

    async fn benchmark(instance: Rc<RefCell<Self>>) {
        // wait for update of rangemap
        loop {
            if instance.borrow().datums.len() == 0 {
                Local::later().await;
            } else {
                break;
            }
        }

        // fill data
        let mut datums = instance.borrow().datums.clone();
        let rng = rand::thread_rng();
        let key_length = 16;
        let value_length = 256;
        let shard_id = instance.borrow().id;
        assert_eq!(datums.len(), 1);
        let datum = datums.pop().expect("checked");

        let number_of_keys =
            Shard::benchmark_load_data(&instance, shard_id, &datum, key_length, value_length).await;
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
            instance.clone(),
            histo,
            workload,
        );
        match workload {
            Workload::ReadLocal100 => Shard::benchmark_read_local_100(&mut ctx).await,
            Workload::ReadLocalRemote => Shard::benchmark_read_local_remote(&mut ctx).await,
            Workload::WriteLocal100 => Shard::benchmark_write_local_100(&mut ctx).await,
            Workload::WriteLocalRemote => Shard::benchmark_write_local_remote(&mut ctx).await,
            Workload::StorageWorkloadA => Shard::benchmark_read_write_local_remote(&mut ctx).await,
            Workload::StorageWorkloadB => Shard::benchmark_read_write_local_remote(&mut ctx).await,
            Workload::StorageWorkloadC => Shard::benchmark_read_write_local_remote(&mut ctx).await,
            Workload::PMemStorageWorkload => {
                Shard::benchmark_read_write_local_remote(&mut ctx).await
            }
        }

        println!(
            "{:}",
            format_stats(&instance.borrow().stats, shard_id, t0.elapsed(), ctx),
        );
        timer::sleep(Duration::from_secs(2)).await;
    }

    async fn consume_from_other_shards(
        instance: Rc<RefCell<Self>>,
        shard_receivers: Vec<ConnectedReceiver<Message<Data<RangeMapType>>>>,
    ) {
        let mut join_handles = vec![];
        for (idx, recv) in shard_receivers
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| *idx != instance.borrow().id)
        {
            let instance = instance.clone();
            let join_handle =
                Local::local(async move {
                    loop {
                        // log::debug!("consuming in {:} from {:}", instance.borrow().id, idx);
                        if let Some(msg) = recv.recv().await {
                            // log::debug!(
                            //     "shard {:} recvd from {:} message {:?}",
                            //     instance.borrow().id,
                            //     idx,
                            //     &msg
                            // );
                            if msg.dst_id != instance.borrow().id {
                                log::error!(
                                    "invalid dst id! Expected: {:} got {:}",
                                    instance.borrow().id,
                                    &msg.dst_id
                                );
                                continue;
                            }
                            match msg.data {
                                Data::Log(s) => log::info!("received log: {:}", s),
                                Data::UpdateRangeMap(rangemap) => {
                                    let mut borrowed = instance.borrow_mut();
                                    // FIXME redundant clone
                                    // FIXME hacky datum creation for benchmark, introduce special message type for that
                                    // and during update check that all datums from rangemap belonging to current shard actually exist
                                    let storage = borrowed.benchmark_get_datum_storage();
                                    for item in rangemap.deref().clone().into_iter() {
                                        if item.data.shard_id == borrowed.id {
                                            let len = borrowed.datums.len();
                                            borrowed.datums.push(Datum::new(
                                                len,
                                                RangeSpec::trusted_new(item.start, item.end, ()),
                                                storage.clone(),
                                            ))
                                        }
                                    }
                                    borrowed.rangemap = rangemap;
                                    log::info!("rangemap updated ok");
                                }
                                Data::GetRequest { id, key } => {
                                    let instance = instance.clone();
                                    let source = msg.source_id;

                                    Local::local(async move {
                                        let value = Shard::get(&instance, key).await;
                                        instance
                                            .borrow()
                                            .send_to(source, Data::GetResponse { id, value })
                                            .await;
                                        instance.borrow_mut().stats.served_forwarded_gets += 1;
                                    })
                                    .detach();
                                }
                                Data::GetResponse { id, value } => {
                                    instance.borrow_mut().forwarded_get.entry(id).and_modify(
                                        |(waker, result)| {
                                            // set result
                                            *result = Some(value);
                                            // wake the waker
                                            waker.take().expect("no waker in forwarded get").wake()
                                        },
                                    );
                                }
                                Data::SetRequest { id, key, value } => {
                                    let instance = instance.clone();
                                    let source = msg.source_id;
                                    Local::local(async move {
                                        Shard::set(&instance, key, value).await;
                                        instance
                                            .borrow()
                                            .send_to(source, Data::SetResponse { id })
                                            .await;
                                        instance.borrow_mut().stats.served_forwarded_sets += 1;
                                    })
                                    .detach();
                                }
                                Data::SetResponse { id } => {
                                    instance.borrow_mut().forwarded_set.entry(id).and_modify(
                                        |(waker, result)| {
                                            *result = Some(());
                                            // wake the waker
                                            waker.take().expect("no waker in forwarded set").wake()
                                        },
                                    );
                                }
                                Data::DelRequest { id, key } => {
                                    let instance = instance.clone();
                                    let source = msg.source_id;
                                    Local::local(async move {
                                        instance.borrow_mut().delete(&key).await;
                                        instance
                                            .borrow()
                                            .send_to(source, Data::DelResponse { id })
                                            .await;
                                        instance.borrow_mut().stats.served_forwarded_sets += 1;
                                    })
                                    .detach();
                                }
                                Data::DelResponse { id } => {
                                    instance.borrow_mut().forwarded_del.entry(id).and_modify(
                                        |waker| {
                                            // wake the waker
                                            waker.take().expect("no waker in forwarded get").wake()
                                        },
                                    );
                                }
                            }
                        } else {
                            log::info!("shard {:} recvd None from {:}", instance.borrow().id, idx);
                            break;
                        }
                    }
                })
                .detach();
            join_handles.push(join_handle);
        }
        join_all(join_handles).await;
    }

    async fn get(instance: &Rc<RefCell<Self>>, key: Vec<u8>) -> Option<Vec<u8>> {
        let mut borrowed = instance.borrow_mut();
        let rangespec = borrowed
            .rangemap
            .get(&key)
            .expect("always should be some range");
        let shard_id = rangespec.data.shard_id;
        let datum_id = rangespec.data.datum_id;
        if shard_id != borrowed.id {
            borrowed.stats.forwarded_gets += 1;
            let id = borrowed.next_op();
            let fut = ForwardedGet::new(id, instance.clone());
            drop(borrowed);
            instance
                .borrow()
                .send_to(shard_id, Data::GetRequest { id: id, key: key })
                .await;
            return fut.await;
        }
        borrowed.stats.served_own_gets += 1;
        drop(borrowed);

        if let Some(data) = instance.borrow().datums[datum_id].get(&key).await {
            return Some(data);
        }
        return None;
    }

    // async fn set(&mut self, key: &[u8], value: &[u8]) {
    async fn set(instance: &Rc<RefCell<Self>>, key: Vec<u8>, value: Vec<u8>) {
        let mut borrowed = instance.borrow_mut();
        let rangespec = borrowed.rangemap.get(&key).unwrap(); //.expect(&format!(
                                                              // "always should be some range. key: {:?} contents {:?}",
                                                              // &key, borrowed.rangemap
                                                              // ));
        let shard_id = rangespec.data.shard_id;
        let datum_id = rangespec.data.datum_id;

        if shard_id != borrowed.id {
            borrowed.stats.forwarded_sets += 1;
            let id = borrowed.next_op();
            let fut = ForwardedSet::new(id, instance.clone());
            drop(borrowed);
            instance
                .borrow()
                .send_to(shard_id, Data::SetRequest { id, key, value })
                .await;
            return fut.await;
        }
        borrowed.stats.served_own_sets += 1;
        drop(borrowed);
        instance.borrow_mut().datums[datum_id].set(key, value).await;
    }

    async fn delete(&mut self, key: &[u8]) -> Option<()> {
        let rangespec = self.rangemap.get(key).expect("always should be some range");
        if rangespec.data.shard_id != self.id {
            // FIXME now always return none, should await for response, may be use facade with operation on shard, spawning new operation and waiting for it resolve
            self.stats.forwarded_sets += 1;
            self.send_to(
                rangespec.data.shard_id,
                Data::DelRequest {
                    id: 1,
                    key: key.to_owned(),
                },
            )
            .await;
            return None;
        }
        self.datums[rangespec.data.datum_id].delete(key).await
    }
}

#[cfg(test)]
mod tests {
    use crate::sharding::{
        builder::ShardBuilder,
        enums::{Data, Message},
    };

    use super::*;
    use rangetree::RangeVec;
    use std::time::Duration;

    type RangeMapType = RangeVec<Vec<u8>, ShardDatum>;
    type Msg = Message<Data<RangeMapType>>;

    async fn make_pair<RangeMapType: 'static + RangeMap<Vec<u8>, ShardDatum>>(
    ) -> (ConnectedSender<Msg>, ConnectedReceiver<Msg>) {
        let (tx, rx) = shared_channel::new_bounded::<Msg>(10);
        join!({ async { tx.connect().await } }, {
            async { rx.connect().await }
        })
    }
    struct TestData {
        from_other_sender: ConnectedSender<Msg>, // sender from other shard to test one
        rc_to_other_receiver: Rc<RefCell<ConnectedReceiver<Msg>>>, // receiver from this shard to the other one
        shard: Rc<RefCell<Shard<RangeMapType>>>,
        other_recvd_messages: Rc<RefCell<Vec<Msg>>>,
    }

    impl TestData {
        async fn new() -> Self {
            // with other shard
            let (from_other_sender, from_other_receiver) = make_pair::<RangeMapType>().await;
            let (to_other_sender, to_other_receiver) = make_pair::<RangeMapType>().await;
            // with self
            let (from_self_sender, from_self_receiver) = make_pair::<RangeMapType>().await;
            let (to_self_sender, to_self_receiver) = make_pair::<RangeMapType>().await;

            let mut raw_shard: Shard<RangeMapType> =
                Shard::new(0, vec![to_self_sender, to_other_sender]);
            let (rangemap, rangespec) = get_initial_range_map::<RangeMapType>(0, 0);
            raw_shard.rangemap = Arc::new(rangemap.freeze());
            let rc_to_other_receiver = Rc::new(RefCell::new(to_other_receiver));
            from_other_sender
                .send(Message::new(1, 0, Data::Log(String::from("hello"))))
                .await
                .unwrap();
            let rc_to_other_receiver_cloned = rc_to_other_receiver.clone();
            let recvd_messages = Rc::new(RefCell::new(vec![]));
            let cloned = Rc::clone(&recvd_messages);
            Local::local(async move {
                while let Some(msg) = rc_to_other_receiver_cloned
                    .clone()
                    .borrow_mut()
                    .recv()
                    .await
                {
                    cloned.borrow_mut().push(msg);
                }
                // rc_to_other_receiver_cloned.clone().borrow_mut().recv().await; // healthcheck
                // rc_to_other_receiver_cloned.clone().borrow_mut().recv().await; // rangemap update
            })
            .detach();
            let shard = raw_shard
                .serve(vec![from_self_receiver, from_other_receiver])
                .await;
            Self {
                from_other_sender,
                rc_to_other_receiver: rc_to_other_receiver.clone(),
                shard,
                other_recvd_messages: Rc::clone(&recvd_messages),
            }
        }
    }

    #[test]
    fn test_shard_local() {
        LocalExecutorBuilder::new()
            .spawn(|| async move {
                let test_data = TestData::new().await;
                // check get set delete
                test_data
                    .shard
                    .borrow_mut()
                    .set(vec![1, 1, 1], vec![1, 1, 1])
                    .await;
                assert_eq!(
                    Shard::get(&test_data.shard, &vec![1, 1, 1]).await,
                    Some(vec![1, 1, 1])
                );
                assert_eq!(
                    test_data.shard.borrow_mut().delete(&vec![1, 1, 1]).await,
                    Some(())
                );
                test_data
                    .shard
                    .borrow_mut()
                    .set(vec![1, 1, 1], vec![1, 1, 1])
                    .await;
                // check concurrent get
                join!(
                    async { Shard::get(&test_data.shard, &vec![1, 1, 1]).await },
                    async { Shard::get(&test_data.shard, &vec![1, 1, 1]).await }
                );
                // check set with more concurrency
                join_all((0u8..100).into_iter().map(|_| {
                    let cloned = test_data.shard.clone();
                    async move {
                        assert_eq!(
                            cloned.borrow_mut().set(vec![1, 1, 1], vec![1, 1, 1]).await,
                            (),
                        );
                    }
                }))
                .await;

                // check resolving of forwarded requests, send operations like they are forwarded from other
                test_data
                    .from_other_sender
                    .send(Message::new(
                        1,
                        0,
                        Data::GetRequest {
                            id: 1,
                            key: vec![1, 1, 1],
                        },
                    ))
                    .await
                    .expect("ok");

                glommio::timer::sleep(Duration::from_micros(300)).await;
                assert_eq!(test_data.shard.borrow().stats.served_forwarded_gets, 1);
                assert_eq!(
                    *test_data.other_recvd_messages.borrow().last().unwrap(),
                    Message::new(
                        0,
                        1,
                        Data::GetResponse {
                            id: 1,
                            value: Some(vec![1, 1, 1])
                        }
                    )
                );
            })
            .unwrap()
            .join()
            .unwrap()
    }

    fn test_two_shards() {
        let shard_builder: ShardBuilder<RangeVec<Vec<u8>, ShardDatum>> =
            ShardBuilder::new(Placement::first_n_cores(2), 10);
        shard_builder.spawn();
    }
}
