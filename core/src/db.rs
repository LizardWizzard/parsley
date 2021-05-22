// use std::{
//     thread::JoinHandle,
// };

// use glommio::{
//     channels::{
//         shared_channel::ConnectedSender,
//     },
// };

// use crate::shard::{Data, Message};

// // use crate::shard::DbToShardMessage;

// #[derive(Clone)]
// pub struct Config {
//     cores: Vec<usize>,
// }

// impl Config {
//     pub fn new(cores: Vec<usize>) -> Self {
//         Self { cores }
//     }
// }

// type CPUCore = usize;

// pub struct Db {
//     config: Config,
//     shards: Vec<(CPUCore, ConnectedSender<Message<Data>>)>,
//     executor_handles: Vec<JoinHandle<()>>,
// }

// impl Db {
//     pub fn new(config: Config) -> Self {
//         Self {
//             config: config,
//             shards: vec![],
//             executor_handles: vec![],
//         }
//     }

    // TODO make async, setup master ex ecutor first, then setup child shard executors
    // when master sets up all needed stuff it itself transfers to init_shard routine,
    // since master executor is the same shard as others
    // now this panics because when issuing connect there is no running executor in current thread
    // pub fn run(&mut self) {
    //     // first core is master core with db instance
    //     // we need to initialize master executor and from it start child ones,
    //     // because when creating channels they have to be binded to executor
// }

// by key find shard (optionally datum) for that key
// find all afected shards and datums

// datum id to shard id global map?

// pub fn run(config: Config) -> () {
//     let mut cores = config.cores.clone();
//     let master_core = cores.pop().unwrap();
//     LocalExecutorBuilder::new()
//         .pin_to_cpu(master_core)
//         .spawn(move || async move {
//             log::info!("settung up master core");
//             let mut db = Db::new(config);
//             for core in cores.into_iter() {
//                 log::debug!("spawning local executor for core {:?}", core);
//                 // create channel to shard, add channel sender to db
//                 let (sender, receiver) = shared_channel::new_bounded(10);
//                 let join_handle = LocalExecutorBuilder::new()
//                     .pin_to_cpu(core)
//                     .spawn(move || async move { init_shard(core, receiver).await })
//                     .unwrap();
//                 // sender can connect when receiver is active, so await on connect after spawning the receiver
//                 db.shards.push((core, sender.connect().await));
//                 db.executor_handles.push(join_handle);
//                 log::debug!("spawned local executor for core {:?}", core);
//             }
//             // TODO launch shard at master core too.
//             let rng: [u32; 3] = [0; 3];
//             for _ in rng.iter() {
//                 for (core, shard) in db.shards.iter() {
//                     log::debug!("sending hello to shard core {:?}", core);
//                     shard
//                         .send(DbToShardMessage::Log(String::from("hello")))
//                         .await
//                         .unwrap();
//                 }
//                 Timer::new(Duration::from_secs(2)).await;
//             }
//             for (_, shard) in db.shards.iter() {
//                 shard.send(DbToShardMessage::Shutdown).await.unwrap();
//             }
//         })
//         .unwrap()
//         .join()
//         .unwrap();
// }
