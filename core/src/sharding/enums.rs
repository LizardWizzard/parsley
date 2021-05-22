use std::sync::Arc;

use rangetree::RangeMap;

use super::shard::ShardDatum;


#[derive(Debug, PartialEq)]
pub struct Message<T: Send + PartialEq> {
    pub source_id: usize,
    pub dst_id: usize,
    pub data: T,
}

impl<T: Send + PartialEq> Message<T> {
    pub fn new(source_id: usize, dst_id: usize, data: T) -> Self {
        Self {
            source_id,
            dst_id,
            data,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum Data<RangeMapType: RangeMap<Vec<u8>, ShardDatum>> {
    Log(String),
    UpdateRangeMap(Arc<RangeMapType::FROZEN>),
    // types which are used to represent requests forwarded to another shard
    GetRequest {
        id: usize,
        key: Vec<u8>,
    },
    GetResponse {
        id: usize,
        value: Option<Vec<u8>>,
    },
    SetRequest {
        id: usize,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    SetResponse {
        id: usize,
    },
    DelRequest {
        id: usize,
        key: Vec<u8>,
    },
    DelResponse {
        id: usize,
    },
}

#[derive(Debug)]
pub enum HealthcheckError {
    FailedToSend(String),
    FailedToRecv(String),
    IncorrectRecv(String),
}

#[derive(Debug)]
pub enum RestoreError {}

pub enum RestoreStatus {
    Data,
    Empty,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ShardRole {
    Master,
    Worker,
}
