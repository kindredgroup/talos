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

## pg.migrate: 🥁 Run migrations on database
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

## dev.run-agent-client: 🧪 Runs agent
dev.run-agent-client:
	$(call pp,run-agent app...)
	cargo run --example agent_client --release

## dev.run: 🧪 Runs rust app in watch mode
dev.run:
	$(call pp,run app...)
	cargo  watch -q -c -x 'run --example certifier_kafka_pg'
## run: 🧪 Runs rust app
run:
	$(call pp,run app...)
	cargo run --example certifier_kafka_pg

## run: 🧪 Runs rust app in release mode
run.release:
	$(call pp,run app...)
	cargo run -r --example certifier_kafka_pg

run.with_mock_db:
	$(call pp,run app...)
	cargo run -r --example certifier_kafka_dbmock

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

## test.unit.coverage: 🧪 Runs rust unit tests with coverage 'cobertura' and 'junit' reports
test.unit.coverage:
	$(call pp,rust unit tests...)
	sh scripts/coverage-report.sh
# PHONY ###########################################################################################

# To force rebuild of not-file-related targets, make the targets "phony".
# A phony target is one that is not really the name of a file;
# Rather it is just a name for a recipe to be executed when you make an explicit request.
.PHONY: build
