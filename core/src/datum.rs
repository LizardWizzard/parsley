use std::{cell::RefCell, collections::HashMap, fs, ops::Deref, path::PathBuf, rc::Rc, str::FromStr, time::{Duration, Instant}};

use glommio::timer;
use pmem_storage::{Config, PMemStorage};

// no async traits for now unfortunately :( so doing match every time
pub trait Storage {
    fn get(&self, key: &[u8]) -> Option<&[u8]>;

    fn set(&mut self, key: &[u8], value: &[u8]) -> ();

    fn delete(&mut self, key: &[u8]) -> Option<()>;
}

#[derive(Debug)]
pub struct MemoryStorage {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub async fn get(&self, key: &Vec<u8>) -> Option<&[u8]> {
        match self.data.get(key) {
            Some(value) => Some(value),
            None => None,
        }
    }

    // pub async fn set(&mut self, key: &[u8], value: &[u8]) -> () {
    pub async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.data.insert(key, value);
    }

    pub async fn delete(&mut self, key: &[u8]) -> Option<()> {
        match self.data.remove(key) {
            Some(_) => Some(()),
            None => None,
        }
    }
}

pub fn busy_sleep(dur: Duration) {
    let t0 = Instant::now();
    loop {
        if t0.elapsed() > dur {
            break;
        }
    }
}

#[derive(Debug)]
pub struct BTreeModelStorage {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl BTreeModelStorage {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub async fn get(instance: &Rc<RefCell<Self>>, key: &Vec<u8>) -> Option<Vec<u8>> {
        busy_sleep(Duration::from_micros(40));
        // std::thread::sleep(Duration::from_micros(40));
        // timer::sleep(Duration::from_micros(40)).await;
        match instance.borrow().data.get(key) {
            Some(value) => Some(value.to_vec()),
            None => None,
        }
    }

    // pub async fn set(&mut self, key: &[u8], value: &[u8]) -> () {
    pub async fn set(instance: &Rc<RefCell<Self>>, key: Vec<u8>, value: Vec<u8>) {
        busy_sleep(Duration::from_micros(80));
        // std::thread::sleep(Duration::from_micros(80));
        // timer::sleep(Duration::from_micros(60)).await;
        instance.borrow_mut().data.insert(key, value);
    }

    pub async fn delete(instance: &Rc<RefCell<Self>>, key: &[u8]) -> Option<()> {
        match instance.borrow_mut().data.remove(key) {
            Some(_) => Some(()),
            None => None,
        }
    }
}

#[derive(Debug)]
pub struct LSMTreeModelStorage {
    data: HashMap<Vec<u8>, Vec<u8>>,
}

impl LSMTreeModelStorage {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub async fn get(instance: &Rc<RefCell<Self>>, key: &Vec<u8>) -> Option<Vec<u8>> {
        // FIXME because we borrow before polling, polling occurs "inside" borrowing,
        // so it breaks when there is an await in storage
        // one possible way to fix it implement get/set methods as ones returning futures, which wrap Rc<RefCell
        // and borrow only for poll duration. 
        busy_sleep(Duration::from_micros(80));
        // std::thread::sleep(Duration::from_micros(80));
        // timer::sleep(Duration::from_micros(130)).await;
        match instance.borrow().data.get(key) {
            Some(value) => Some(value.to_vec()),
            None => None,
        }
    }

    // pub async fn set(&mut self, key: &[u8], value: &[u8]) -> () {
    pub async fn set(instance: &Rc<RefCell<Self>>, key: Vec<u8>, value: Vec<u8>) {
        // busy_sleep(Duration::from_micros(80));
        busy_sleep(Duration::from_micros(40));
        // std::thread::sleep(Duration::from_micros(10));
        // timer::sleep(Duration::from_micros(25)).await;
        instance.borrow_mut().data.insert(key, value);
    }

    pub async fn delete(instance: &Rc<RefCell<Self>>, key: &[u8]) -> Option<()> {
        match instance.borrow_mut().data.remove(key) {
            Some(_) => Some(()),
            None => None,
        }
    }
}

#[derive(Debug)]
pub struct PMemDatumStorage {
    data: PMemStorage::<1024, 16, 256>,
}

impl PMemDatumStorage {
    pub fn new(shard_id: usize) -> Self {
        let mut base_dir = PathBuf::from_str("/tmp/ramdisk/data").unwrap();
        base_dir.push(format!("{:}", shard_id));
        let r = fs::create_dir(&base_dir);
        dbg!(r);
        let cfg = Config::new(base_dir);
        // FIXME hardcode for benchmark
        for dir_entry in fs::read_dir(&cfg.base_dir).unwrap() {
            let path = dir_entry.unwrap().path();
            fs::remove_file(path).unwrap();
        }
        Self {
            data: PMemStorage::new(cfg),
        }
    }

    pub async fn get(instance: &Rc<RefCell<Self>>, key: &Vec<u8>) -> Option<Vec<u8>> {
        match instance.borrow().data.get(key) {
            Some(value) => Some(value.to_vec()),
            None => None,
        }
    }

    // pub async fn set(&mut self, key: &[u8], value: &[u8]) -> () {
    pub async fn set(instance: &Rc<RefCell<Self>>, key: Vec<u8>, value: Vec<u8>) {
        instance.borrow_mut().data.set(key, value);
    }

    pub async fn delete(instance: &Rc<RefCell<Self>>, key: &[u8]) -> Option<()> {
        match instance.borrow_mut().data.delete(key) {
            Some(_) => Some(()),
            None => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum DatumStorage {
    MemoryStorage(Rc<RefCell<MemoryStorage>>),
    BTreeModelStorage(Rc<RefCell<BTreeModelStorage>>),
    LSMTreeModelStorage(Rc<RefCell<LSMTreeModelStorage>>),
    PMemStorage(Rc<RefCell<PMemDatumStorage>>),
}

impl DatumStorage {
    pub fn get_kind(&self) -> &str {
        match &self {
            DatumStorage::MemoryStorage(_) => "memory",
            DatumStorage::BTreeModelStorage(_) => "btree_model",
            DatumStorage::LSMTreeModelStorage(_) => "lsm_model",
            DatumStorage::PMemStorage(_) => "pmem",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Datum {
    pub id: usize, // for now it is assumed that datum id is local for shard and serves as an index for this datum in Shard.datums vec
    pub range: rangetree::RangeSpec<Vec<u8>>,
    pub storage: DatumStorage,
}

impl Datum {
    pub fn new(
        id: usize,
        range: rangetree::RangeSpec<Vec<u8>>,
        storage: DatumStorage,
    ) -> Self {
        Self { id, range, storage }
    }

    // pub async fn get(&self, key: &[u8]) -> Option<&[u8]> {
    // TODO optimize via read into AsyncWrite without reference return
    pub async fn get(&self, key: &Vec<u8>) -> Option<Vec<u8>> {
        match &self.storage {
            DatumStorage::MemoryStorage(storage) => {
                return Some(storage.borrow().get(&key).await?.to_owned())
            }
            DatumStorage::BTreeModelStorage(storage) => {
                return Some(BTreeModelStorage::get(&storage, &key).await?)
            }
            DatumStorage::LSMTreeModelStorage(storage) => {
                return Some(LSMTreeModelStorage::get(&storage, key).await?)
            }
            DatumStorage::PMemStorage(storage) => {
                return Some(PMemDatumStorage::get(storage, key).await?)
            }
        }
    }

    // pub async fn set(&mut self, key: &[u8], value: &[u8]) {
    pub async fn set(&mut self, key: Vec<u8>, value: Vec<u8>) {
        match &self.storage {
            DatumStorage::MemoryStorage(storage) => storage.borrow_mut().set(key, value).await,
            DatumStorage::BTreeModelStorage(storage) => BTreeModelStorage::set(&storage, key, value).await,
            DatumStorage::LSMTreeModelStorage(storage) => LSMTreeModelStorage::set(&storage, key, value).await,
            DatumStorage::PMemStorage(storage) => PMemDatumStorage::set(&storage, key, value).await,
        }
    }

    pub async fn delete(&mut self, key: &[u8]) -> Option<()> {
        match &self.storage {
            DatumStorage::MemoryStorage(storage) => storage.borrow_mut().delete(key).await,
            DatumStorage::BTreeModelStorage(storage) => BTreeModelStorage::delete(storage, key).await,
            DatumStorage::LSMTreeModelStorage(storage) => LSMTreeModelStorage::delete(storage, key).await,
            DatumStorage::PMemStorage(storage) => PMemDatumStorage::delete(storage, key).await
        }
    }
}
