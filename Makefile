#!make
SHELL:=/bin/bash

# pp - pretty print function
yellow := $(shell tput setaf 3)
normal := $(shell tput sgr0)
define pp
	@printf '$(yellow)$(1)$(normal)\n'
endef


help: Makefile
	@echo " Choose a command to run:"
	@sed -n 's/^##//p' $< | column -t -s ':' | sed -e 's/^/ /'


# DEV #############################################################################################

## withenv: 😭 CALL TARGETS LIKE THIS `make withenv RECIPE=dev.init`
withenv:
# NB: IT APPEARS THAT LOADING ENVIRONMENT VARIABLES INTO make SUUUUCKS.
# NB: THIS RECIPE IS A HACK TO MAKE IT WORK.
# NB: THAT'S WHY THIS MAKEFILE NEEDS TO BE CALLED LIKE `make withenv RECIPE=dev.init`
	test -e .env || cp .env.example .env
	bash -c 'set -o allexport; source .env; set +o allexport; make "$$RECIPE"'

## dev.init: 🌏 Initialize local dev environment
# If rdkafka compilation fails with SSL error then install openssl@1.1 or later and export:
# export LDFLAGS=-L/opt/homebrew/opt/openssl@1.1/lib
# export CPPFLAGS=-I/opt/homebrew/opt/openssl@1.1/include
dev.init: install
	$(call pp,install git hooks...)
	cargo install cargo-watch
	cargo test

## dev.kafka_init: 🥁 Init kafka topic
dev.kafka_init:
	$(call pp,creating kafka topic...)
	cargo run --package talos_certifier --bin deploy_kafka

## pg.create: 🥁 Create database
pg.create:
	$(call pp,creating database...)
	cargo run --package postgres --bin create_database

## pg.migrate: 🥁 Run migrations on database
pg.migrate:
	$(call pp,running migrations on database...)
	cargo run --package postgres --bin migrations

# TEST / DEPLOY ###################################################################################

## install: 🧹 Installs dependencies
install:
	$(call pp,pull rust dependencies...)
	rustup install "${RUST_VERSION}"
	rustup component add rust-src clippy llvm-tools-preview
	rustup toolchain install nightly
	rustup override set "${RUST_VERSION}"
	cargo install cargo2junit grcov
	cargo fetch

## build: 🧪 Compiles rust
build:
	$(call pp,build rust...)
	cargo build


## dev.run: 🧪 Runs rust app in watch mode
dev.run:
	$(call pp,run app...)
	cargo  watch -q -c -x 'run --bin talos_certifier'
## run: 🧪 Runs rust app
run:
	$(call pp,run app...)
	cargo run --bin talos_certifier

## run: 🧪 Runs rust app in release mode
run.release:
	$(call pp,run app...)
	cargo run -r --bin talos_certifier

## lint: 🧹 Checks for lint failures on rust
lint:
	$(call pp,lint rust...)
	cargo check
	cargo fmt -- --check
	cargo clippy --all-targets -- -D warnings

## test.unit: 🧪 Runs unit tests
test.unit:
	$(call pp,rust unit tests...)
	cargo test

## test.unit.coverage.rust: 🧪 Runs rust unit tests with coverage 'cobertura' and 'junit' reports
test.unit.coverage.rust:
	$(call pp,rust unit tests...)
	mkdir -p build/.report
	mkdir -p build/.coverage
	RUSTFLAGS="-Zinstrument-coverage" LLVM_PROFILE_FILE="coverage-%p-%m.profraw" cargo +nightly test -- -Z unstable-options --format json --report-time > build/.report/rust-test-report.json
	cat build/.report/rust-test-report.json | cargo2junit > build/.report/rust-junit-report.xml
	grcov . --binary-path ./target/debug/ -s . -t cobertura --branch --ignore-not-existing  --ignore "*cargo*" --keep-only "**/**.rs" -o ./build/.coverage/rust-cobertura-coverage.xml

# PHONY ###########################################################################################

# To force rebuild of not-file-related targets, make the targets "phony".
# A phony target is one that is not really the name of a file;
# Rather it is just a name for a recipe to be executed when you make an explicit request.
.PHONY: build
