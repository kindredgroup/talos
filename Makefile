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
	cargo run --example kafka_create_topic

## pg.create: 🥁 Create database
pg.create:
	$(call pp,creating database...)
	cargo run --example pg_create_database

## pg.migrate: 🥁 Run sql on database
pg.migrate:
	$(call pp,running migrations on database...)
	cargo run --example pg_migrations

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

## init.samply: 🧪 Installs Samply profiler
init.samply:
	cargo install samply

## init.flamegraph: 🧪 Installs Flamegraph profiler
init.flamegraph:
	cargo install flamegraph

## agent.run-client: 🧪 Executes load test through Talos Agent
agent.run-client:
	$(call pp,runing Talos Agent example...)
	cargo run --example agent_client --release -- $(args)

## agent.run-profiler-samply: 🧪 Runs agent with Samply profiler
agent.run-profiler-samply:
	$(call pp,run-agent app...)
	cargo build --example agent_client
	samply record -o logs/samply-agent.json -s cargo run --example agent_client
	samply load logs/samply-agent.json

## agent.run-profiler-flamegraph: 🧪 Runs agent with Flamegraph profiler
# Add CARGO_PROFILE_RELEASE_DEBUG=true to .env
agent.run-profiler-flamegraph:
	$(call pp,run-agent app...)
	cargo build --example agent_client
	rm logs/flamegraph-agent.svg | true
	sudo cargo flamegraph -o logs/flamegraph-agent.svg --open --example agent_client

## agent.run-profiler-xcode-cpu: 🧪 Runs agent with XCode profiler for Time (CPU) Profiler (OSX only)
# Make sure XCode is installed
agent.run-profiler-xcode-cpu:
	$(call pp,run-agent app...)
	cargo build --release --example agent_client
	scripts/sign-binary-for-profiling.sh target/release/examples/agent_client
	rm -rf logs/agent-*.trace
	target/release/examples/agent_client &
	sleep 8
	xctrace record --template 'Time Profiler' --output logs/agent-time.trace --attach `pgrep agent_client`
	open logs/agent-cpu.trace

## agent.run-profiler-xcode-mem: 🧪 Runs agent with XCode profiler for Allocations (OSX only)
# Make sure XCode is installed
agent.run-profiler-xcode-mem:
	$(call pp,run-agent app...)
	cargo build --release --example agent_client
	scripts/sign-binary-for-profiling.sh target/release/examples/agent_client
	rm -rf logs/agent-*.trace
	target/release/examples/agent_client &
	sleep 8
	xctrace record --template 'Allocations' --output logs/agent-allocations.trace --attach `pgrep agent_client`
	open logs/agent-allocations.trace

## dev.run: 🧪 Runs Talos Certifier app in watch mode
dev.run:
	$(call pp,run app...)
	cargo  watch -q -c -x 'run --example certifier_kafka_pg'

## run: 🧪 Runs Talos Certifier app
run:
	$(call pp,running Talos Certifier...)
	cargo run --example certifier_kafka_pg

## run.release: 🧪 Runs Talos Certifier app in release mode
run.release:
	$(call pp,running Talos Certifier...)
	cargo run -r --example certifier_kafka_pg

## run.with_mock_db: 🧪 Runs Talos Certifier with mock DB in release mode
run.with_mock_db:
	$(call pp,run app...)
	cargo run -r --example certifier_kafka_dbmock

## cohort_banking.create_db: 🥁 Creates database for Cohort Banking and applies DB sql
cohort_banking.create_db:
	$(call pp,creating database for cohort...)
	cargo run --package cohort_banking --bin run_db_migrations create-db migrate-db

## cohort_banking.migrate_db: 🥁 Run sdatabase migrations for Cohort Banking
cohort_banking.migrate_db:
	$(call pp,running migrations on banking database for cohort...)
	cargo run --package cohort_banking --bin run_db_migrations

## cohort_banking.preload_db: 🧪 Injects initial data into Cohort Banking DB
cohort_banking.preload_db:
	$(call pp,populating banking database for cohort...)
	cargo run --package cohort_banking --bin preload_db -- $(args)

## cohort_banking.run_initiator_load_test_in_rust: 🧪 Executes load test through Cohort Initiator implemented in Rust
cohort_banking.run_initiator_load_test_in_rust:
	$(call pp,running "Cohort Initiator" implemented in Rust...)
	cargo run --example cohort_banking_with_sdk --release -- $(args)

## cohort_banking.run_replicator_rust: 🧪 Rust Cohort Banking Replicator implemented in Rust
cohort_banking.run_replicator_rust:
	$(call pp,running "Cohort Replicator" implemented in Rust...)
	cargo run --example cohort_replicator_kafka_pg --release -- $(args)

## cohort_banking.run_initiator_js: 🧪 Executes load test through Cohort Initiator implemented in JS
cohort_banking.run_initiator_load_test_in_js:
	$(call pp,running "Cohort Initiator" implemented in JS...)
	cd ./cohort_banking_initiator_js && npm start -- $(args)

## cohort_banking.run_replicator_js: 🧪 Runs Replicator JS app
cohort_banking.run_replicator_js:
	$(call pp,running "Cohort Replicator" implemented in JS...)
	cd ./cohort_banking_replicator_js && npm start

## dev.histogram_decision_timeline_from_kafka: 🧪 Reads all decisions from kafka and prints processing timeline as csv
dev.histogram_decision_timeline_from_kafka:
	$(call pp,histogram_decision_timeline_from_kafka...)
	cargo run --bin histogram_decision_timeline_from_kafka --release -- $(args)

## example.messenger_kafka: 🧪 Runs the example messenger that uses Kafka.
example.messenger_kafka:
	$(call pp,run app...)
	cargo run -r --example messenger_using_kafka

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

## test.some-unit: 🧪 Runs specified unit tests
#  Example: test.some-unit args="--test tx_batch_executor"
test.some-unit:
	$(call pp,rust unit tests...)
	cargo test -- $(args) | grep -v "running 0 tests" | grep -v "Doc-tests" | grep -v "filtered out"

## test.unit.coverage: 🧪 Runs rust unit tests with coverage 'cobertura' and 'junit' reports
test.unit.coverage:
	$(call pp,rust unit tests...)
	sh scripts/coverage-report.sh
# PHONY ###########################################################################################

# To force rebuild of not-file-related targets, make the targets "phony".
# A phony target is one that is not really the name of a file;
# Rather it is just a name for a recipe to be executed when you make an explicit request.
.PHONY: build
