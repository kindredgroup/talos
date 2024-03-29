JsDecision# The JavaScript binding for Cohort SDK

## About
This module contains JavaScript SDK for creating Talos Cohort Initiator or Replicator applications in JavaScript. It is bundled as NPM package and available in [GitHub Packages NPM repository](https://github.com/orgs/kindredgroup/packages?repo_name=talos).

Logically it contains two parts: the internal implementation and public SDK. The internal implementation is made as a separate NPM module `@kindredgroup/cohort_sdk_js` and provided as dependency to `cohort_sdk_client`.

## Cohort SDK JS
 The "cohort_sdk_js" module is a bridge between Rust and JavaScript utilising Node-API - an API for building native Addons. In our case the addon is written in Rust. So, the actual Talos Cohort SDK can be found in `packages/cohort_sdk` package. The `cohort_sdk_js` should be considered an internal project as it is not designed to be consumed directly by JS apps. Applications should use `cohort_sdk_client`.

 ## Cohort SDK Client
The "cohort_sdk_client" module is a wrapper around `cohort_sdk_js` which makes use of SDK a bit easier as it smoothens some rough edges sticking out of pure Node-API for native bindings.

The example of SDK usage can be found in `cohort_banking_initiator_js` and in `cohort_banking_replicator_js` modules.

The SDK allows users to create certification transaction initiator and certification transaction replicator in JavaScript either as a single app or as two separate applications.

### Installing
As a prerequisite for installing this package we require that Rust development environment is setup on the user machine. The main logic of SDK is implemented in `../packages/cohort_sdk`. In order for this code to be available to JavaScript in `@kindredgroup/cohort_sdk_js` it must be compiled into the native library for client architecture. There are two ways to achieve this:

1. On CI. When making NPM package `@kindredgroup/cohort_sdk_js` we configure build process so that it produces NPM with multiple optional dependencies each for dedicated architecture, such as for Linux, for ARM64 Mac, for Intel Mac and etc. This approach requires different build environments on CI. Most of our developers use ARM64 Mac, while CI does not have build image for it. We are looking for solution. In the meanwhile, please refer to option #2.
2. On the user machine. When installing `@kindredgroup/cohort_sdk_js` NPM as dependency also install native sources and compile native them locally. Then bundle compiled native lib in `@kindredgroup/cohort_sdk_js`. As such, for prerequisites of installing this package we require that Rust development environment is setup on the user machine.

#### Install development ecosystem for Rust

<em>This step is curently required but in the future we might be able to optimise build process and remove it.</em>

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

#### Install package
```
npm -i @kindredgroup/cohort_sdk_client@SOME_VERSION --foreground-scripts
```

The above step will download `cohort_sdk_client` and `cohort_sdk_js` dependencies. The `cohort_sdk_js` dependency will come with Rust code and native comilation will begin. Once finished, we will have `@kindredgroup/cohort_sdk_js` and `@kindredgroup/cohort_sdk_client` under `./node_modules/`

<em>For reader convenience this step is also described in the `../cohort_banking_initiator_js` readme. When making changes to install procedure, please update another location as well.</em>

# The Initiator API
The role of the Initiator is to validate transactions locally, submit updates for certification and apply submitted updates out-of-order.

```TypeScript
// Create and use initiator
const initiator = await Initiator.init(sdkConfig)

const fnMakeNewRequestCallback = async (...) => {
    // some logic how to create a new certification request
    // based on the current state of the app
};

const fnOooInstaller = async (...) => {
    // some logic how to install certified transaction
    // into local database.
}

const response = await initiator.certify(fnMakeNewRequestCallback, fnOooInstaller)

if (response.decision === JsDecision.Committed) {
    // do something
}

```

The `Initiator.certify` method accepts two callback functions:

1. The factory to create a new certification request, let's call it "New Request Callback".
2. The function to update database once transaction was certified by Talos. This callback will not be invoked if Talos did not certify the transaction.

These functions are known as Initiator callbacks.

## About "New Request Callback"

The details of how to create a new request is delegated to you. The shape of the callback function looks like this:
```TypeScript
async (): Promise<JsCertificationCandidateCallbackResponse>
```
- It receives no parameters.
- It returns `JsCertificationCandidateCallbackResponse`

```TypeScript
export interface JsCertificationCandidateCallbackResponse {
  cancellationReason?: string
  newRequest?: JsCertificationRequestPayload
}

export interface JsCertificationRequestPayload {
  candidate: JsCertificationCandidate
  snapshot: number
  timeoutMs: number
}

export interface JsCertificationCandidate {
  readset: Array<string>
  writeset: Array<string>
  readvers: Array<number>
  statemaps?: Array<Record<string, any>>
  onCommit?: JsCandidateOnCommitActions
}

export interface JsKafkaAction {
  cluster?: string
  /** Topic to publish the payload */
  topic: string
  /** Key encoding to be used. Defaults to `text/plain`. */
  keyEncoding?: string
  /** Key for the message to publish. */
  key?: string
  /** Optional if the message should be published to a specific partition. */
  partition?: number
  /** Optional headers while publishing. */
  headers?: Record<string, string>
  /** Key encoding to be used. Defaults to `application/json`. */
  valueEncoding?: string
  /** Payload to publish. */
  value: any
}
export interface JsCandidateOnCommitPublishActions {
  kafka: Array<JsKafkaAction>
}
export interface JsCandidateOnCommitActions {
  publish?: JsCandidateOnCommitPublishActions
}
```

### About "JsCertificationCandidate"

Before SDK can issue a certification request to Talos Certifier it needs some details from you. You will have to query your local database to fetch the following:
1. Identifiers and version numbers of all objects involved in your transaction. These are known as `readset`, `writeset` and `readvers`.
2. The copy of your transaction as one serializable object. It makes sense to describe your transaction as JSON object and serialise it to string. This is known as `statemap`.
3. Any additional message to be published for candidate requests with committed decision outcome, can be added to `onCommit` field. Currently the SDK supports only publishing to **Kafka**.

Above mentioned reads, writes and statemap fields together are known as certification candidate details. You may ask whether statemap is optional? Indeed, as you are passing the arrow function to `fnOooInstaller` callback you have the context of your request. From the perspective of Initiator app, the answer is "yes, it is optional". However, the statemap will also be received by your Replicator app. Replicator may be implemented as a separate process. Replicator will know what needs to be updated in the database by reading statemap.

Read about `statemap` in the end of this document. See section [About Statemap](#about-statemap).

### About "JsCertificationRequestPayload"

Talos is making a certification decision purely based on two major pieces of data. 1) the decisions it made to previous requests issued by you or by other cohorts 2) the state of objects in your database. In order to have a full picture, Talos Certifier needs to know not only _the state of your objects taking part in the current transaction_ but also the _global state of your cohort_. The global state is known as the snapshot version. You need to pass your objects (candidate) and your current global state (snapshot). Check this structure `JsCertificationRequestPayload`.

The response to your certification request will be asynchronously received by SDK. However, the async aspect of it is hidden from you for simplicity of usage. Your call to `await .certify(...)` will block until request is available or until it times out. Use the optional config `JsCertificationRequestPayload.timeoutMs` attribute to specify how long you are willing to wait for that specific response. If not provided, then SDK will use value from `JsInitiatorConfig.timeoutMs`.

### About "JsCertificationCandidateCallbackResponse"

What happens when you need to certify some transaction but:
- The data you have passed to Talos resulted in abort decision because it was outdated?
- There was a short network disruption communicating with either Kafka or your local DB (during execution of "New Request Callback")?

Most likely you will want to retry your request. The SDK implements retry with incremental random backoff logic. It is configurable via `JsInitiatorConfig.retry*` settings. Upon failure of "current" attempt the SDK will sleep a little and invoke "New Request Callback" again. Remember that you have provided this callback `.certify(here, ...)`. As your database is used in the concurrent fashion by multiple instances of your cohort and by other processes in your application, it is very likely that state of your objects or the global state changed while SDK was processing this current attempt. So, the retry logic inside SDK will make sure that request will be re-issued, however, you, as cohort developer, should make sure that you are providing the latest information about your state to Talos. So, once attempt to certify fails, SDK will enter into re-try - sleep loop. On every loop iteration your "New Request Callback" will be invoked again, and you will load a new fresh state from DB. By default, this "invoke Talos -> fail -> sleep -> reload -> invoke Talos" loop may go on until a) we've got successful response from Talos, b) until we timed out or c) until we exhausted max number of attempts. The SDK allows you to break off that loop and cancel a certification request by providing any reason in `JsCertificationCandidateCallbackResponse.cancellationReason`. Why would you want to cancel the transaction? It depends on the state of your objects. What if object was deleted from DB or it became a read-only etc. It depends on your application.

## About "Out of Order Install Callback"

If your business transaction requires a certification from Talos, it is expected that you will not do any changes to objects taking part in your transaction (you will not update database records) until the decision is received from Talos. Only after certification decision is received you will proceed with business transaction. Typically, this is going to be some database update, for example, you will update balances of relevant bank accounts, hence "transfer" money between them. This step is done inside "Out of Order Install Callback". SDK will invoke this callback only when Talos approved your transaction, in other words, when Talos checks that there are no conflicting requests to update your objects.

<em>What is the benefit of having out of order callback if its responsibility overlaps with "Statemap Installer Callback" found in Replicator?
You may wish not to implement this callback and rely on Replicator to do all your DB changes. Just keep in mind that Replicator will do it "later". How much later will depends on the overall load on the replicator and other dependent transactions which are still in-flight. If you did not implement out of order callback then it is possible to finish the call to `let response = await initiator.certify(...)`, have "go ahead" decision from Talos in the response variable, but your DB will not see this change. If, at that point, you returned response to user via HTTP and user went to query DB via another endpoint, it could be that user will not see the change yet (Replicator may still be processing the backlog of other transactions). On the other hand, with out of order callback in place, once the call to `let response = await initiator.certify(...)` finished, your DB is already updated and you may rely on that change in your following logic.
</em>

The shape of the callback function looks like this:
```TypeScript
async (oooRequest: JsOutOfOrderRequest): Promise<JsOutOfOrderInstallOutcome>;
```

- It receives `JsOutOfOrderRequest`
- It returns `JsOutOfOrderInstallOutcome`

What do you need to do inside "Out of Order Install Callback"?
Two things:
1. Implement your business transaction, for example, transfer the money.
2. Update state of your objects, by bumping up their version numbers. (What version number to set is given in the parameters, see `OutOfOrderRequest.newVersion`. You will set this version number to all affected objects).

Parameters to this callback:

```TypeScript
export interface OutOfOrderRequest {
  xid: string
  safepoint: number
  newVersion: number
}
```

- `OutOfOrderRequest.xid` - the unique ID of certification transaction attempt
- `newVersion` - when transaction is certified, the state of all objects from this transaction should be set to the new version. IF YOUR OBJECTS ARE ALREADY ON VERSION BIGGER OR EQUAL TO THIS, YOU SHOULD NOT UPDATE DB AT ALL. (In other words - if your objects were updated by some other transaction then you are risking overriding these changes, hence you should not update your objects.) In the example app, we use `SQL WHERE` filter: `UDPATE ... SET version = $newVersion WHERE version < $newVersion AND ...`
- `safepoint` - it is expected that your global state did not move past this version. IF YOUR COHORT SNAPSHOT IS SMALLER THAN THIS, YOU SHOULD NOT UPDATE DB AT ALL (In other words - if your global state has not reached the point when it is safe to update it, then no updates should be made.)

What response to return from "Out of Order Install Callback"?

```TypeScript
export const enum JsOutOfOrderInstallOutcome {
  Installed,
  InstalledAlready,
  SafepointCondition
}
```

- `JsOutOfOrderInstallOutcome.Installed` if you successfully updated your objects
- `JsOutOfOrderInstallOutcome.InstalledAlready` if your objects have been updated by some other transaction and you made no changes
- `JsOutOfOrderInstallOutcome.SafepointCondition` if your global state hasn't reached the safe point.

## The Initiator API Errors

Some things may go wrong when working with Initiator API. Possible errors can be detected by checking `TalosSdkError`

```TypeScript
        try {
            const tx = ...
            const resp = initiator.certify(...)
        } catch (e) {
            if (e instanceof TalosSdkError) {
                const sdkError = e as TalosSdkError
                logger.error("Unable to process tx: %s. ", JSON.stringify(tx),
                "\nTalosSdkError.message: " + sdkError.message
                "\nTalosSdkError.kind: " + sdkError.kind
                "\nTalosSdkError.name: " + sdkError.name
                "\nTalosSdkError.cause: " + sdkError.cause
                "\nTalosSdkError.stack: " + sdkError.stack)
            } else {
                logger.error("Unable to process tx: %s. Error: %s", JSON.stringify(tx), e)
            }
        }
```

Where `TalosSdkError.kind` is defined as:

```TypeScript
export const enum SdkErrorKind {
  Certification,
  CertificationTimeout,
  Cancelled,
  Messaging,
  Persistence,
  Internal,
  OutOfOrderCallbackFailed,
  OutOfOrderSnapshotTimeout
}
```

| Kind  | Description |
| ------------- | ------------- |
| `Certification` | Error during certification  |
| `CertificationTimeout` | The timeout as configured  |
| `Cancelled` | You have cancelled the certification retry attempt via `JsCertificationCandidateCallbackResponse.cancellationReason`|
| `Messaging` | Error communicating with Kafka broker |
| `Persistence` | Error communicating with database |
| `Internal` | Some unexpected SDK error |
| `OutOfOrderCallbackFailed` | Error when invoking out of order installer callback |
| `OutOfOrderSnapshotTimeout` | You have indicated that out of order install was not successful because of `JsOutOfOrderInstallOutcome.SafepointCondition` and we exhausted all retry attempts. |

## The Initiator API Configuration

See structure defined in `JsInitiatorConfig`

| Parameter  | Description | Example, default |
| ---------- | ----------- | ---------------- |
| `retryAttemptsMax` | This parameter comes into play during re-try logic implemented in SDK. It controls many times to re-try the certification request. | 10 |
| `retryBackoff.minMs` | The re-try logic implemented in SDK uses exponential backoff strategy. The delay will grow anywhere between min and max values. | 20 |
| `retryBackoff.maxMs` | | 1500 |
| `retryOoAttemptsMax` | This parameter comes into play during re-try logic implemented in SDK. How many times to re-try installing statemap. The difference between this parameter and `retryAttemptsMax` is that this retry is implemented for each certification attempt.| 10 |
| `retryOoBackoff.minMs` | The re-try logic implemented in SDK uses exponential backoff strategy. The delay will grow anywhere between min and max values. | 20 |
| `retryOoBackoff.maxMs` | | 1000 |
| `backoffOnConflict.minMs` | The re-try logic implemented in SDK uses exponential backoff strategy. The delay will grow anywhere between min and max values. | 1 |
| `backoffOnConflict.maxMs` | | 1500 |
| `snapshotWaitTimeoutMs` | This parameter comes into play during retry logic implemented in SDK. When Talos certifier aborts the transaction (if conflict is detected) then you should wait until your global state reaches the safe point. This is number of millis to wait until `OutOfOrderSnapshotTimeout` is raised. | 10000 |
| `agent` | Cohort Agent Name, this must be unique per kafka consumer instance. Passed in the kafka mesage header `certAgent`. Used to correlate response message with certification request message issued by same process. | |
| `cohort` | Cohort name. This param, and `agent` param are passed to Talos in the candidate message payload and returned in the decision message. | |
| `bufferSize` | The size of internal buffer in certification messages. | 10000 |
| `timeoutMs` | The number of millis SDK should wait for response from Talos before giving up. | |
| `kafka.brokers` | The array of kafka brokers. | `["127.0.0.1:9092"]` |
| `kafka.topic` | The certification topic name. The same as used by Talos Certifier. | |
| `kafka.clientId` | The client id of connecting cohort. | |
| `kafka.groupId` | TODO: Explain why it should not be set for cohort. | |
| `kafka.username` | Kafka auth | |
| `kafka.password` | Kafka auth | |
| `kafka.producerConfigOverrides` | This is `Map<string, string>` All keys and values are passed directly to underlying kafka library when configuring produser. | `{ "enable.auto.commit": "false" }`  |
| `kafka.consumerConfigOverrides` | This is `Map<string, string>` All keys and values are passed directly to underlying kafka library when configuring consumer. | |
| `kafka.producerSendTimeoutMs` | The `queue_timeout` parameter controls how long to retry for if the librdkafka producer queue is full. | 10 |
| `kafka.logLevel` | The verbocity level of kafka library. One of "alert", "critical", "debug", "emerg", "error", "info", "notice", "warning". | "info" |


# The Replicator API
The role of Replicator in Talos ecosystem is to listen to certification decisions and apply transactions in the same order as they were certified.

```TypeScript
const replicator = await Replicator.init(...)

const fnSnapshotProviderCallback = async () => {...}
const fnStatemapInstallerCallback = async () => {...}
// this will run indefinitely
await replicator.run(fnSnapshotProviderCallback, fnTransactionInstaller)
```
The replicator is a server running in the background. It connects to Kafka and recieves all messages from certification topic. Regardless of the order messages were received, the replicator will re-order them and feed into your `fnStatemapInstallerCallback` in the correct order. Your role here is to implement both callbacks.

## About "Snapshot Provider Callback"
The callback has the shape of simple getter function which returns the "global state" - the latest version number which you have in your DB. The signature of the function looks like this:

```TypeScript
async (): Promise<number>
```
- It receives no parameters.
- It returns the snapshot version as number.

This callback is invoked only once during startup of Replicator server. It fetches current snapshot. Replicator uses current snapshot version as a point in time marker which tells Replictor not to react to any messages older than that version.

## About "Statemap Installer Callback"
This statemap installer callback looks like this:

```TypeScript
async (data: JsStatemapAndSnapshot): Promise<void>
```
- It receives `JsStatemapAndSnapshot`
- There is no return value, if function completes successfully SDK will proceed to the next transaction.

```TypeScript
export interface JsStatemapAndSnapshot {
  statemap: Array<JsStatemapItem>
  version: number
}

export interface JsStatemapItem {
  action: string
  version: number
  safepoint?: number
  payload: any
}
```
The purpose of statemap installer callback is to "catch-up" on changes made by other cohorts (including your cohort).

In the deployment where there is no Talos present, microservices typically implement eventual consistency model by utilising messaging broker. For example, Kafka. If changes are made to bank account in Cohort 1 that change will eventually propagate to Cohort 2 through messaging middleware.

In the deployment with Talos, the replicator, specifically this callback, is responsible for updating the database so that your cohort has the up-to-date view of bank account (or any other shared object). The async messaging is abstracted away from you by SDK. You just provide a handler how you want your database to be updated.

However, there are few rules which Talos protocol expects you to implement.

- Once you are done updating business objects in your database, it is very important to update the global snapshot. The replicator, specifically this callback, is the only place within cohort which is responsible for writing into snapshot table.
- The version number in the snapshot table should only be incremented. If it happens that callback is invoked with older version, then no change should be made to database.
- The change to business objects and to snapshot table should be atomic (in a single DB transaction with isolation level matching repeatable read or stricter).
- When callback is invoked, it is possible that your business objects are already updated. In this case, the job of callback is to update the snapshot table only.
  - This may happen if replicator and initiator belong to the same cohort, for example, out of order installer in initiator may have executed and updated our business objects before the replicator. However, installer should never write to snapshot table.
  - When replicator belong to different cohort, it is just catching up on the changes made by other cohorts, hence it may not encounter the state when business objects are updated already. Unless there was some contingency, like unexpected restart.
- When updating business objects, also update their versions so that versions match with snapshot version. Also version should only be incremented.

What value to write into snapshot table? Use this attribute: `JsStatemapAndSnapshot.version` <br>
What version to set into business objects? Use this attribute: `JsStatemapAndSnapshot.version`

## About Statemap

The `statemap` is a reserved attribute of certification request. This attribute is not being interpreted by Talos or by SDK. It is reserved to pass around the context of transaction between request and response. You give this data to SDK with request and SDK will presents it back to your Initiator and to your Replicator callbacks when handling "go ahead" decision from Talos. <em>FYI: The statemap data is part of Candidate message only. Replicator gets it by listening to request messages in the certification topic. SDK hides these details internally.</em>

Talos and SDK do not read statemap. The statemap is given back to you in the callback after certification decision is made. You will use details encoded in statemap to understand what transaction has been certified just now and what to do next. In the example app, which simulates a banking transaction to transfer money between two accounts, we define the statemap as action with some parameters:

```json
[
    {
        "TRANSFER": {
            "from": "account 1",
            "to": "account 2",
            "amount": "100.00"
        }
    }
]
```
This is free form object. When Talos certifies the transaction, we know that the request to transfer 100.00 from account1 to account2 can go ahead, so installer function has all the necessary details to implement this transfer.

We find it easier to understand the statemap by comparing it to JSON request sent as HTTP POST. For example, when REST API client is invoking HTTP POST it has no knowledge how this request will be handled on the server side. The POST payload usually contains all the details needed to express the intention of user. Both the client and the server agree on the structure of JSON. Similar concept is applicable between Initiator and Replicator. The Initiator makes no assumption how many cohorts are present in the deployment, what are their DB schemas etc. Initiator only knows that statemap will be delivered to all cohorts and these cohorts may choose to update their internal databases. That means all cohorts participating in the transaction should know how to parse statemap. The "schema" of the statemap is meant to be agreed and understood by cohorts only and not by Talos.