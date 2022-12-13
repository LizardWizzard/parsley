alias c := check

check:
    cargo check --all --tests --benches

alias t := test

test:
    cargo test --all

alias b := build

build:
    cargo build --all --features=parsley_durable_log/bench

alias br := build-release

build-release:
    cargo build --all --release --features=parsley_durable_log/bench
