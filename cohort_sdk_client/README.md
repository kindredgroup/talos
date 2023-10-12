# The JavaScript binding for Cohort SDK

## About
This module contains JavaScript SDK for creating Talos Cohort Initiator or Replicator applications in JavaScript. It is bundled as NPM package and available in [GitHub Packages NPM repository](https://github.com/orgs/kindredgroup/packages?repo_name=talos).

Logically it contains two parts: the internal implementation and public SDK. The internal implementation is made as a separate NPM module `@kindredgroup/cohort_sdk_js` and provided as dependency to `cohort_sdk_client`.

## Cohort SDK JS
 The "cohort_sdk_js" module is a bridge between Rust and JavaScript utilising Node-API - an API for building native Addons. In our case the addon is written in Rust. So, the actual Talos Cohort SDK can be found in `packages/cohort_sdk` package. The `cohort_sdk_js` should be considered an internal project as it is not designed to be consumed directly by JS apps. Applications should use `cohort_sdk_client`.

 ## Cohort SDK Client
The "cohort_sdk_client" module is a wrapper around `cohort_sdk_js` which makes use of SDK a bit easier as it smoothens some rough edges sticking out of pure Node-API for native bindings.

The example of SDK usage can be found in `cohort_banking_initiator_js` and in `cohort_banking_replicator_js` modules.

The SDK allows users to create certification transaction initiator and certification transaction replicator in JavaScript either as a single app or as two separate applications.

### The Initiator API
Please refer to [Initiator API](README-initiator-api.md) readme.

### The Replicator API
Please refer to [Replicator API](README-replicator-api.md) readme.
