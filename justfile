alias c := check

check:
    cargo check --all --tests --benches

alias t := test

test:
    cargo test --all --no-fail-fast --exclude pmem_storage

alias b := build

build:
    cargo build --all

alias br := build-release

build-release:
    cargo build --all --release

clippy:
    cargo clippy

alias v := verify

verify: check test clippy
