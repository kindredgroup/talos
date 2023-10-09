# Cohort Replicator JS App

## About
This is an example project demonstrating how to create Talos Replicator component using Cohort SDK

It is one of many components in Talos ecosystem. Section below describes the functionality of this application and how it depends on other components.

Project is implemented in TypeScript and used the external library `cohort_sdk_client`, which in turn depends on `cohort_sdk_js`. The `cohort_sdk_js` is not desiged to be used directly, but only via its wrapper `cohort_sdk_client`. Both `cohort_sdk_*` libraries are shipped as NPM modules and hosted under `@kinderedgroup` in GitHub Packages NPM repository. While they both are NPM modules and can be installed via `npm install`, the installation of `cohort_sdk_js` has one extra step to compile native code on developer's machine. Please read more about it in the `cohort_sdk_js` package.

Cohort Replicator JS is TypeScript app. When launched it connects to Postgres database and to Kafka broker, then listens to incoming messages (these are certification requests and decisions) then updates local database of Cohort Initiator. Cohort Initiator JS and Cohort Replicator JS share the same database.

## Usage

Please refer to readme of Cohort Initiator JS app.