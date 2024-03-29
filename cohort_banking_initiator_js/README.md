# Cohort Initiator JS App

## About
This is an example project demonstrating how to use Cohort SDK to communicate with Talos Certifier.

It is one of many components in Talos ecosystem. Section below describes the functionality of this application and how it depends on other components.

Project is implemented in TypeScript and uses the external library `cohort_sdk_client`, which in turn depends on `cohort_sdk_js`. The `cohort_sdk_js` is not desiged to be used directly, but only via its wrapper `cohort_sdk_client`. Both `cohort_sdk_*` libraries are shipped as NPM modules and hosted under `@kinderedgroup` in GitHub Packages NPM repository. While they both are NPM modules and can be installed via `npm install`, the installation of `cohort_sdk_js` has one extra step to compile native code on developer's machine. Please read more about it in the `cohort_sdk_js` package.

Cohort Initiator JS is TypeScript app. When launched it connects to Postgres database and to Kafka broker, sends some number of certification requests to Talos Certifier. Once all requests are processed the app prints basic performance metrics and shuts down.

## Usage

1. Start Postgres database server
1. Start Kafka broker
1. Prepare .env config file
1. Start Talos Certifier
1. Generate sample data file
1. Create DB schema for cohort
1. Insert sample data to cohort DB
1. Build Cohort Replicator JS
1. Run Cohort Replicator JS
1. Build Cohort Initiator JS
1. Run Cohort Initiator JS

### Setup your machine

#### Install development ecosystem for Rust

<em>This step is curently required but in the future we might be able to optimise build process and remove it. In the readme of "packages/cohort_sdk_js", please read more why it is compiled locally on user machine.</em>

Refer to https://www.rust-lang.org/tools/install

Check:
```
cargo --version && rustc --version && rustup --version

cargo 1.72.0 (103a7ff2e 2023-08-15)
rustc 1.72.0 (5680fa18f 2023-08-23)
rustup 1.26.0 (5af9b9484 2023-04-05)
info: This is the version for the rustup toolchain manager, not the rustc compiler.
info: The currently active `rustc` version is `rustc 1.72.0 (5680fa18f 2023-08-23)`
```

<em>For reader convenience this step is also described in the `../cohort_sdk_client` readme. When making changes to install procedure, please update another location as well.</em>

#### Start Postgres server

```
sudo su postgres

# Locate your Postgres installation (could be under "/Library/PostgreSQL/13/"), pg_ctl and data directory (could be "/Library/PostgreSQL/13/data").
# find ~ -name pg_ctl

./bin/pg_ctl -D ./data

```

#### Prepare .env config file
```
cp ./.env.example ./.env

```
Read the content of `.env` and adjsut connectivity details to Postgres and to Kafka

#### Start Talos Certifier

Please refer to Talos Certifier readme.

#### Generate data file

Generate data JSON file containing some number of bank accounts.

```
cd $TALOS/

accounts_file=../10k-accounts.json
./scripts/cohort/data-generator.py --action gen-initial --count 10000 --out $accounts_file

Generating 10000 accounts into:
../10k-accounts.json
...completed 1000 of 10000
...
...completed 10000 of 10000
```

#### Prepare DB

```
cd $TALOS/
# Create database schema
make withenv RECIPE=cohort_banking.create_db
make withenv RECIPE=cohort_banking.migrate_db

# Populate database
make withenv RECIPE=cohort_banking.preload_db args="--accounts $accounts_file"
```

### Building Cohort Replicator and Initiator apps

These sample apps depend on `@kindredgroup/cohort_sdk_client` package. It is hosted at [GitHub Packages](https://npm.pkg.github.com) repository.

#### Authenticate to repository GitHub Packages
```
touch $(pwd)/.npmrc-talos
echo "@kindredgroup:registry=https://npm.pkg.github.com" >> $(pwd)/.npmrc-talos

# Using this method the ".npmrc-talos" file will be updated with your token. Do not commit the file!
NPM_CONFIG_USERCONFIG=$(pwd)/.npmrc-talos npm login --registry https://npm.pkg.github.com
# password is your GitHub token
# Obtain token at GitHub website Profile -> Settings -> Developer settings -> Personal access tokens -> Tokens (classic)
```

#### For local development

This section describes how to install cohort client SDK for local development of cohort (or for running examples locally).

In the Talos project at GitHub, under ["Packages"](https://github.com/orgs/kindredgroup/packages?repo_name=talos) find `cohort_sdk_client` and the version number you need to install locally.

```
cd $TALOS/cohort_banking_initiator_js

# For example, the SDK version is "0.0.1-bfa8241"
sdk_version=0.0.1-bfa8241
npm i @kindredgroup/cohort_sdk_client-$sdk_version
```

For installing from branch simply use `npm ci`.

```
cd $TALOS/cohort_banking_initiator_js
# This might take some time while native JS bindings are being compiled
npm ci

cd $TALOS/cohort_banking_replicator_js
# This might take some time while native JS bindings are being compiled
npm ci
```

### Running Cohort Replicator
```
cd $TALOS/cohort_banking_replicator_js

# This app is the application server. It does not stop until process is terminated.
npm start
```

### Running Cohort Initiator
```
cd $TALOS/cohort_banking_initiator_js

# Genrate 1000 transation requests at the rate of 500 TPS, pick accounts from dataset containing 10k bank accounts
npm start count=1000 rate=500 accounts=10000
```