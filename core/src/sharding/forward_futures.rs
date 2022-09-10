use std::task::Poll;

use futures::Future;
use rangetree::RangeMap;

use super::shard::{Shard, ShardDatum};

pub struct ForwardedGet<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + 'static> {
    id: usize,
    shard: Shard<RangeMapType>,
}

impl<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + 'static> ForwardedGet<RangeMapType> {
    pub fn new(id: usize, shard: Shard<RangeMapType>) -> Self {
        Self { id, shard }
    }
}

impl<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + 'static> Future for ForwardedGet<RangeMapType> {
    type Output = Option<Vec<u8>>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // of there is no value -> insert waker with None result
        // else if there is value but no result, update waker
        // if result exists ready
        let mut shard = self.shard.state_mut();
        let (waker, result) = shard
            .forwarded_get
            .entry(self.id)
            .or_insert((Some(cx.waker().clone()), None));
        match result.take() {
            None => {
                *waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Some(response) => {
                shard.forwarded_get.remove(&self.id);
                Poll::Ready(response)
            }
        }
    }
}

pub struct ForwardedSet<RangeMapType: RangeMap<Vec<u8>, ShardDatum> + 'static> {
    id: usize,
    shard: Shard<RangeMapType>,
}

impl<RangeMapType: RangeMap<Vec<u8>, ShardDatum>> ForwardedSet<RangeMapType> {
    pub fn new(id: usize, shard: Shard<RangeMapType>) -> Self {
        Self { id, shard }
    }
}

impl<RangeMapType: RangeMap<Vec<u8>, ShardDatum>> Future for ForwardedSet<RangeMapType> {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // if there is no value -> insert waker with None result
        // else if there is value but no result, update waker
        // if result exists ready
        let mut shard = self.shard.state_mut();
        let (waker, result) = shard
            .forwarded_set
            .entry(self.id)
            .or_insert((Some(cx.waker().clone()), None));
        match result.take() {
            None => {
                *waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Some(response) => {
                shard.forwarded_set.remove(&self.id);
                Poll::Ready(response)
            }
        }
    }
}
