use std::{cell::RefCell, collections::HashMap, rc::Rc, task::Poll};

use futures::Future;

use super::Storage;

pub struct MemoryStorageGetFuture<'key> {
    storage: Rc<RefCell<MemoryStorage>>,
    key: &'key [u8],
}

impl<'key> MemoryStorageGetFuture<'key> {
    pub fn new(storage: &Rc<RefCell<MemoryStorage>>, key: &'key [u8]) -> Self {
        // TODO try to remove clone using extra lifetime for storage reference
        Self {
            storage: storage.clone(),
            key,
        }
    }
}

impl<'key> Future for MemoryStorageGetFuture<'key> {
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

pub struct MemoryStorageSetFuture<'key, 'value> {
    storage: Rc<RefCell<MemoryStorage>>,
    key: &'key [u8],
    value: &'value [u8],
}

impl<'key, 'value> MemoryStorageSetFuture<'key, 'value> {
    pub fn new(storage: &Rc<RefCell<MemoryStorage>>, key: &'key [u8], value: &'value [u8]) -> Self {
        Self {
            storage: storage.clone(),
            key,
            value,
        }
    }
}

impl<'key, 'value> Future for MemoryStorageSetFuture<'key, 'value> {
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

pub struct MemoryStorageDelFuture<'key> {
    storage: Rc<RefCell<MemoryStorage>>,
    key: &'key [u8],
}

impl<'key> MemoryStorageDelFuture<'key> {
    pub fn new(storage: &Rc<RefCell<MemoryStorage>>, key: &'key [u8]) -> Self {
        Self {
            storage: storage.clone(),
            key,
        }
    }
}

impl<'key> Future for MemoryStorageDelFuture<'key> {
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

#[derive(Debug)]
pub struct MemoryStorage {
    pub data: HashMap<Vec<u8>, Vec<u8>>,
}

impl Storage for MemoryStorage {
    type GetFuture<'key> = MemoryStorageGetFuture<'key>;
    type SetFuture<'key, 'value> = MemoryStorageSetFuture<'key, 'value>;

    type DelFuture<'key> = MemoryStorageDelFuture<'key>;

    fn get<'key>(instance: &Rc<RefCell<Self>>, key: &'key [u8]) -> Self::GetFuture<'key> {
        MemoryStorageGetFuture::new(instance, key)
    }

    fn set<'key, 'value>(
        instance: &Rc<RefCell<Self>>,
        key: &'key [u8],
        value: &'value [u8],
    ) -> Self::SetFuture<'key, 'value> {
        MemoryStorageSetFuture::new(instance, key, value)
    }

    fn delete<'key>(instance: &Rc<RefCell<Self>>, key: &'key [u8]) -> Self::DelFuture<'key> {
        MemoryStorageDelFuture::new(instance, key)
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }
}
