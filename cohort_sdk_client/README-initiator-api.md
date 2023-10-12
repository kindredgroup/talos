# The Initiator API
The role of Initiator in Talos ecosystem is to send certification request to Talos Certifier and receive response.

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

if (response.decision === Decision.Committed) {
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
const fnMakeNewRequestCallback = async () => { ... };
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
}
```

### About "JsCertificationCandidate"

Before SDK can issue a certification request to Talos Certifier it needs some details from you. You will have to query your local database to fetch the following:
1. Identifiers and version numbers of all objects involved in your transaction. These are known as `readset`, `writeset` and `readvers`.
2. The copy of your transaction as one serializable object. It makes sense to describe your transaction as JSON object and serialise it to string. This is known as `statemap`.

Above mentioned reads, writes and statemap fields together are known as certification candidate details. Talos and SDK does not use statemap. The statemap is given back to you along with the response to your certification request. When you receive response, you will use details encoded in statemap to understand what transaction has been certified just now and what to do next. In the example app, which simulates a banking transaction to transfer money between two accounts, we define the statemap as action with some parameters:

```json
[
    {
        "TRANSFER: {
            "from": "account 1",
            "to": "account 2",
            "amount": "100.00"
        }
    }
]
```
This is free form object. When Talos certifies the transaction, we know that the request to transfer 100.00 from account1 to account2 can go ahead, as installer function has all the necessary details to implement this transfer.
You may ask whether statemap is optional? Indeed, as you are passing the arrow function to `fnOooInstaller` callback you have the context of your request. So, from the perspective of Initiator app, the answer is "yes, it is optional". However, the statemap will also be received by your Replicator app. Replicator may be implemented as a separate process but it still needs access to the statemap.

### About "JsCertificationRequestPayload"

Talos is making a certification decision purely based on two major pieces of data. 1) the decisions it made to previous requests issued by you or by other cohorts 2) the state of objects in your database. In order to have a full picture, Talos Certifier needs to know not only _the state of your objects taking part in the current transaction_ but also the _global state of your cohort_. The global state is known as the snapshot version. So, you need to pass your objects (candidate) and your current global state (snapshot). Check this structure `JsCertificationRequestPayload`.

The response to your certification request will be asynchronously received by SDK. However, the async aspect of it is hidden from you for simplicity of usage. Your call to `await .certify(...)` will block until request is available or until it times out. The optional value of `JsCertificationRequestPayload.timeoutMs` attribute allows you to specify how long you are willing to wait for that specific response. If not provided, then value will be taken from `JsInitiatorConfig.timeoutMs`.

### About "JsCertificationCandidateCallbackResponse"

What happens when you need to certify some transaction but:
- the data you have passed to Talos resulted in abort decision because it was outdated?
- there was a short network disruption communicating with either Kafka or your local DB (during execution of "New Request Callback")?

Most likely you will want to retry your request. The SDK implements retry with incremental random backoff logic. It is configurable via `JsInitiatorConfig.retry*` settings. Upon failure of "current" attempt the "New Request Callback" which you have provided to `.certify(here, ...)` will be invoked again (after some short delay). As your database is used in the concurrent fashion by multiple instances of your cohort and by other processes in your application, it is very likely that state of your objects or the global state changed while SDK was processing this current attempt. So, the retry mechanism inside SDK will make sure that request will be re-issued, however, you, as cohort developer, should make sure that you are providing the latest information about your state to Talos. So, once attempt to certify fails, your "New Request Callback" will be invoked again, and you will load a new fresh state from DB. By default, this "invoke Talos -> fail -> sleep -> reload -> invoke Talos" loop may go on until we got successful response from Talos, until we timed out or until we exhausted max number of attempts. The SDK allows you to tap in and cancel a certification request by providing any reason in `JsCertificationCandidateCallbackResponse.cancellationReason`. Why would you want to cancel the transaction? It depends on the state of your objects, what if object was deleted from DB or became a read-only etc. It depends on your application.

## About "Out of Order Install Callback"

If your business transaction requires a certification from Talos, it is expected that you will not do any changes to objects taking part in your transaction (you will not update database records) until the decision is received from Talos. Only after certification decision is received you will proceed with business transaction. Typically, this is going to be some database update, for example, you will update balances of relevant bank accounts, hence "transfer" money between them. This step is done inside "Out of Order Install Callback". SDK will invoke this callback only when Talos approved your transaction, in other words, when Talos checks that there are no conflicting requests to update your objects.

The shape of the callback function looks like this:
```TypeScript
const fnOooInstaller = async (oooRequest: JsOutOfOrderRequest): Promise<JsCertificationCandidateCallbackResponse> => { ... };
```

- It receives `JsOutOfOrderRequest`
- It returns `JsCertificationCandidateCallbackResponse`

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
- `newVersion` - when certified your objects state should be set to this new version. IF YOUR OBJECTS ARE ALREADY ON VERSION BIGGER OR EQUAL TO THIS, YOU SHOULD NOT UPDATE DB AT ALL. (In other words - if your objects were updated by some other transaction then you are risking overriding these changes, hence you should not update your objects.)
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
                "\nTalosSdkError.stack: " + sdkError.stack
            } else {
                logger.error("Unable to process tx: %s. Error: %s", JSON.stringify(tx), e)
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
| `OutOfOrderSnapshotTimeout` | You have indicated that out of order install was not successful because of `JsOutOfOrderInstallOutcome.SafepointCondition` and we echausted all retry attempts. |

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
