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

## pg.create_cohort: 🥁 Create database for cohort and applies DB sql
pg.create_cohort:
	$(call pp,creating database for cohort...)
	cargo run --package cohort --bin run_db_migrations create-db migrate-db

## pg.migrate_cohort: 🥁 Run sql on database for cohort
pg.migrate_cohort:
	$(call pp,running migrations on database for cohort...)
	cargo run --package cohort --bin run_db_migrations

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

## dev.run-agent-client: 🧪 Runs agent
dev.run-agent-client:
	$(call pp,run-agent app...)
	cargo run --example agent_client --release

## init.samply: 🧪 Installs Samply profiler
init.samply:
	cargo install samply

## init.flamegraph: 🧪 Installs Flamegraph profiler
init.flamegraph:
	cargo install flamegraph

## dev.agent-run-profiler-samply: 🧪 Runs agent with Samply profiler
dev.agent-run-profiler-samply:
	$(call pp,run-agent app...)
	cargo build --example agent_client
	samply record -o logs/samply-agent.json -s cargo run --example agent_client
	samply load logs/samply-agent.json

## dev.agent-run-profiler-flamegraph: 🧪 Runs agent with Flamegraph profiler
# Add CARGO_PROFILE_RELEASE_DEBUG=true to .env
dev.agent-run-profiler-flamegraph:
	$(call pp,run-agent app...)
	cargo build --example agent_client
	rm logs/flamegraph-agent.svg | true
	sudo cargo flamegraph -o logs/flamegraph-agent.svg --open --example agent_client

## dev.agent-run-profiler-xcode-cpu: 🧪 Runs agent with XCode profiler for Time (CPU) Profiler (OSX only)
# Make sure XCode is installed
dev.agent-run-profiler-xcode-cpu:
	$(call pp,run-agent app...)
	cargo build --release --example agent_client
	scripts/sign-binary-for-profiling.sh target/release/examples/agent_client
	rm -rf logs/agent-*.trace
	target/release/examples/agent_client &
	sleep 8
	xctrace record --template 'Time Profiler' --output logs/agent-time.trace --attach `pgrep agent_client`
	open logs/agent-cpu.trace

## dev.agent-run-profiler-xcode-mem: 🧪 Runs agent with XCode profiler for Allocations (OSX only)
# Make sure XCode is installed
dev.agent-run-profiler-xcode-mem:
	$(call pp,run-agent app...)
	cargo build --release --example agent_client
	scripts/sign-binary-for-profiling.sh target/release/examples/agent_client
	rm -rf logs/agent-*.trace
	target/release/examples/agent_client &
	sleep 8
	xctrace record --template 'Allocations' --output logs/agent-allocations.trace --attach `pgrep agent_client`
	open logs/agent-allocations.trace

## dev.run: 🧪 Runs rust app in watch mode
dev.run:
	$(call pp,run app...)
	cargo  watch -q -c -x 'run --example certifier_kafka_pg'
## run: 🧪 Runs rust app
run:
	$(call pp,run app...)
	cargo run --example certifier_kafka_pg

## run.release: 🧪 Runs rust app in release mode
run.release:
	$(call pp,run app...)
	cargo run -r --example certifier_kafka_pg

## run.with_mock_db: 🧪 Runs certifier with mock DB
run.with_mock_db:
	$(call pp,run app...)
	cargo run -r --example certifier_kafka_dbmock

## dev.run_cohort: 🧪 Runs Cohort
dev.run_cohort:
	$(call pp,run cohort...)
	cargo run --bin cohort -- $(args)

## dev.run_replicator: 🧪 Runs replicator
dev.run_replicator:
	$(call pp,run replicator...)
	cargo run --bin replicator

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
