use std::{cell::RefCell, collections::HashMap, rc::Rc, task::Poll};

use futures::Future;

use super::Storage;

pub struct DurableMemoryStorageGetFuture<'key> {
    storage: Rc<RefCell<DurableMemoryStorage>>,
    key: &'key [u8],
}

impl<'key> DurableMemoryStorageGetFuture<'key> {
    pub fn new(storage: &Rc<RefCell<DurableMemoryStorage>>, key: &'key [u8]) -> Self {
        // TODO try to remove clone using extra lifetime for storage reference
        Self {
            storage: storage.clone(),
            key,
        }
    }
}

impl<'key> Future for DurableMemoryStorageGetFuture<'key> {
    type Output = Option<Vec<u8>>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match self.storage.borrow().data.get(self.key) {
            Some(value) => Poll::Ready(Some(value.to_vec())),
            None => Poll::Ready(None),
        }
    }
}

pub struct DurableMemoryStorageSetFuture<'key, 'value> {
    storage: Rc<RefCell<DurableMemoryStorage>>,
    key: &'key [u8],
    value: &'value [u8],
    // write_fut: impl Future<Output=()>,
}

impl<'key, 'value> DurableMemoryStorageSetFuture<'key, 'value> {
    pub fn new(
        storage: &Rc<RefCell<DurableMemoryStorage>>,
        key: &'key [u8],
        value: &'value [u8],
    ) -> Self {
        Self {
            storage: storage.clone(),
            key,
            value,
        }
    }
}

impl<'key, 'value> Future for DurableMemoryStorageSetFuture<'key, 'value> {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        // probably this is the only copy from receiving buf for example into actual storage so should be ok
        self.storage
            .borrow_mut()
            .data
            .insert(self.key.to_vec(), self.value.to_vec());
        Poll::Ready(())
    }
}

pub struct DurableMemoryStorageDelFuture<'key> {
    storage: Rc<RefCell<DurableMemoryStorage>>,
    key: &'key [u8],
}

impl<'key> DurableMemoryStorageDelFuture<'key> {
    pub fn new(storage: &Rc<RefCell<DurableMemoryStorage>>, key: &'key [u8]) -> Self {
        Self {
            storage: storage.clone(),
            key,
        }
    }
}

impl<'key> Future for DurableMemoryStorageDelFuture<'key> {
    type Output = Option<()>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<Self::Output> {
        let res = self
            .storage
            .borrow_mut()
            .data
            .remove(self.key)
            .map_or(None, |_| Some(()));
        Poll::Ready(res)
    }
}

#[derive(Debug, Default)]
pub struct DurableMemoryStorage {
    data: HashMap<Vec<u8>, Vec<u8>>,
    // wal_writer
}

impl Storage for DurableMemoryStorage {
    type GetFuture<'key> = DurableMemoryStorageGetFuture<'key>;
    type SetFuture<'key, 'value> = DurableMemoryStorageSetFuture<'key, 'value>;

    type DelFuture<'key> = DurableMemoryStorageDelFuture<'key>;

    fn get<'key>(instance: &Rc<RefCell<Self>>, key: &'key [u8]) -> Self::GetFuture<'key> {
        DurableMemoryStorageGetFuture::new(instance, key)
    }

    fn set<'key, 'value>(
        instance: &Rc<RefCell<Self>>,
        key: &'key [u8],
        value: &'value [u8],
    ) -> Self::SetFuture<'key, 'value> {
        DurableMemoryStorageSetFuture::new(instance, key, value)
    }

    fn delete<'key>(instance: &Rc<RefCell<Self>>, key: &'key [u8]) -> Self::DelFuture<'key> {
        DurableMemoryStorageDelFuture::new(instance, key)
    }
}
