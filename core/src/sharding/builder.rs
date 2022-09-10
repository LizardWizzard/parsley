use futures::{future::join_all, join};
use glommio::channels::shared_channel::SharedReceiver;
use glommio::{ExecutorJoinHandle, LocalExecutorBuilder};
use rangetree::RangeMap;
use std::rc::Rc;
use std::{marker::PhantomData, vec};

use glommio::channels::shared_channel::{self, SharedSender};

use super::{
    enums::{Data, Message},
    shard::{Placement, Shard, ShardDatum},
};
use glommio::Placement as GlommioPlacement;

pub struct SharedChannelHandle<RangeMapType: RangeMap<Vec<u8>, ShardDatum>> {
    sender: Option<SharedSender<Message<Data<RangeMapType>>>>,
    receiver: Option<SharedReceiver<Message<Data<RangeMapType>>>>,
}

impl<RangeMapType: RangeMap<Vec<u8>, ShardDatum>> SharedChannelHandle<RangeMapType> {
    pub fn new(size: usize) -> Self {
        let (sender, receiver) = shared_channel::new_bounded::<Message<Data<RangeMapType>>>(size);
        Self {
            sender: Some(sender),
            receiver: Some(receiver),
        }
    }
}

#[derive(Debug)]
pub struct ShardCommunicationBuilder<RangeMapType: RangeMap<Vec<u8>, ShardDatum>> {
    senders: Vec<SharedSender<Message<Data<RangeMapType>>>>,
    receivers: Vec<SharedReceiver<Message<Data<RangeMapType>>>>,
}

pub struct ShardBuilder<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + 'static> {
    pub placement: Vec<Placement>,
    pub channel_size: usize,
    _range_map: PhantomData<RangeMapType>,
}

impl<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + 'static> ShardBuilder<RangeMapType> {
    pub fn new(placement: Vec<Placement>, channel_size: usize) -> Self {
        Self {
            placement,
            channel_size,
            _range_map: PhantomData,
        }
    }

    pub fn num_shards(&self) -> usize {
        self.placement.len()
    }

    pub fn make_full_mesh(&self) -> Vec<Vec<SharedChannelHandle<RangeMapType>>> {
        (0..self.num_shards())
            .map(|_| {
                (0..self.num_shards())
                    .map(|_| SharedChannelHandle::new(self.channel_size))
                    .collect()
            })
            .collect()
    }

    pub fn group_per_shard(
        &self,
        mut full_mesh: Vec<Vec<SharedChannelHandle<RangeMapType>>>,
    ) -> Vec<ShardCommunicationBuilder<RangeMapType>> {
        let mut per_chard: Vec<ShardCommunicationBuilder<RangeMapType>> = vec![];
        for source_shard_id in 0..self.num_shards() {
            let mut senders = vec![];
            let mut receivers = vec![];
            for target_id in 0..self.num_shards() {
                senders.push(
                    full_mesh[source_shard_id][target_id]
                        .sender
                        .take()
                        .expect("Always Some"),
                );
                receivers.push(
                    full_mesh[target_id][source_shard_id]
                        .receiver
                        .take()
                        .expect("Always Some"),
                );
            }
            per_chard.push(ShardCommunicationBuilder { senders, receivers });
        }

        per_chard
    }

    pub fn spawn(self) -> Vec<ExecutorJoinHandle<()>> {
        let full_mesh = self.make_full_mesh();
        let per_shard = self.group_per_shard(full_mesh);

        self.placement
            .iter()
            .zip(per_shard)
            .enumerate()
            .map(|(id, (placement, channels))| {
                LocalExecutorBuilder::new(GlommioPlacement::Fixed(placement.core))
                    .name(&format!("shard-{:}", id))
                    .spawn(move || async move {
                        let (connected_receivers, connected_senders) = join!(
                            join_all(
                                channels
                                    .receivers
                                    .into_iter()
                                    .map(|chan| glommio::spawn_local(chan.connect()).detach())
                            ),
                            join_all(
                                channels
                                    .senders
                                    .into_iter()
                                    .map(|chan| glommio::spawn_local(chan.connect()).detach())
                            )
                        );
                        // let storage = Rc::new(RefCell::new(DatumStorage::MemoryStorage(MemoryStorage::new())));
                        let shard = Shard::<RangeMapType>::new(
                            id,
                            connected_senders
                                .into_iter()
                                .map(Option::unwrap)
                                .map(Rc::new)
                                .collect(),
                        );

                        shard
                            .serve(
                                connected_receivers
                                    .into_iter()
                                    .map(Option::unwrap)
                                    .collect(),
                            )
                            .await;
                    })
                    .unwrap()
            })
            .collect()
    }
}
