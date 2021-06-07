# High level overview

parsley is a kv storage subsystem prototype developed for educational purposes so for now it is not intended for any kind of serious use cases.

Parsley is built upon [glommio](https://github.com/DataDog/glommio) a rust thread-per-core framework. 

Goal of parsley is to build scalable multithreaded kv storage which later can be efficiently used for non kv workloads, like scans.
The key concept of parsley is to abstract storage implementations to be able to combine them. Long standing goal is to make different storage implementations and an optimizer which can pick certain impl based on current workload for particular data block. 

For now implemented a basic shard concept. Each shard is running on a dedicated thread and owns certain number of data parts. Each data part can be tied to particular storage implementation. Currently also implemented forwarding of requests to shards that contain needed data.

There is a working implementation of storage based on Storage Class Memory (Optane DIMM in particular). It is basic and will be further developed to improve characteristics.
