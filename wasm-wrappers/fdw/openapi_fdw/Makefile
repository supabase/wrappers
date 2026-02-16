SHELL := /bin/bash
export PATH := $(HOME)/.cargo/bin:$(PATH)
export RUSTUP_HOME := $(HOME)/.rustup

.PHONY: fmt clippy test build check

fmt:
	cargo fmt

clippy:
	RUSTFLAGS="-D warnings" cargo clippy --all --tests --no-deps

test:
	cargo test

build:
	cargo component build --release --target wasm32-unknown-unknown

check: fmt clippy test build
