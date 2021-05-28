use futures::future::join_all;
use glommio::channels::shared_channel::ConnectedReceiver;
use rangetree::{RangeMap, RangeSpec, ReadonlyRangeMap};
use std::{
    cell::RefCell, collections::HashMap, env, rc::Rc, sync::Arc, task::Waker, time::Duration, vec,
};

use glommio::{channels::shared_channel::ConnectedSender, prelude::*};
use std::ops::Deref;

use crate::{datum::{
        BTreeModelStorage, Datum, DatumStorage, LSMTreeModelStorage,
        PMemDatumStorage,
    }, sharding::bench::get_benchmark_range_map, storage::memory::MemoryStorage};

use super::{
    bench::Ctx,
    enums::{Data, HealthcheckError, Message, RestoreError, RestoreStatus, ShardRole},
};
use super::{
    bench::ShardBenchExt,
    forward_futures::{ForwardedGet, ForwardedSet},
};

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
        vec![0; 16],
        vec![255, 16],
        ShardDatum::new(shard_id, datum_id), // FIXME insert default datum on master shard in case its a clean start
    )
    .expect("always ok");
    rangemap.insert(default_range.clone());
    (rangemap, default_range)
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
        Local::later().await;
        let mut bench = ShardBenchExt::new(new_self.clone());
        bench.benchmark().await;
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
                }
            }
        };
        log::info!("shard {:} storage kind {:}", self.id, storage.get_kind());
        storage
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
                        if let Some(msg) = recv.recv().await {
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

    pub async fn get(instance: &Rc<RefCell<Self>>, key: Vec<u8>) -> Option<Vec<u8>> {
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
    pub async fn set(instance: &Rc<RefCell<Self>>, key: Vec<u8>, value: Vec<u8>) {
        let mut borrowed = instance.borrow_mut();
        let rangespec = borrowed.rangemap.get(&key).unwrap();
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
        instance.borrow_mut().datums[datum_id].set(&key, &value).await;
    }

    pub async fn delete(&mut self, key: &[u8]) -> Option<()> {
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

    use super::Placement;
    use super::*;
    use futures::join;
    use glommio::channels::shared_channel;
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

    fn key() -> Vec<u8> {
        vec![1u8; 16]
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
                Shard::set(&test_data.shard, key(), key()).await;
                assert_eq!(
                    Shard::get(&test_data.shard, key()).await,
                    Some(key())
                );
                assert_eq!(
                    test_data.shard.borrow_mut().delete(&key()).await,
                    Some(())
                );
                // check concurrent get
                join!(
                    async { Shard::get(&test_data.shard, key()).await },
                    async { Shard::get(&test_data.shard, key()).await }
                );
                // check set with more concurrency
                join_all((0u8..100).into_iter().map(|_| {
                    let cloned = test_data.shard.clone();
                    async move {
                        assert_eq!(
                            Shard::set(&cloned, key(), key()).await,
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
                            key: key(),
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
                            value: Some(key())
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
