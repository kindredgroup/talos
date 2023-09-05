import { JsConfig } from "cohort_sdk_js"

const SDK_CONFIG: JsConfig = {
    backoffOnConflict: {
        minMs: 2,
        maxMs: 1500,
      },
      retryBackoff: {
        minMs: 2,
        maxMs: 1500,
      },
      retryAttemptsMax: 2,
      retryOoBackoff: {
        minMs: 2,
        maxMs: 1500,
      },
      retryOoAttemptsMax: 2,
      snapshotWaitTimeoutMs: 2,
      agent: "cohort-js",
      cohort: "cohort-js",
      bufferSize: 100_000,
      timeoutMs: 60_000,
      kafka: {
        brokers: ["127.0.0.1:9092"],
        topic: "dev.ksp.certification",
        clientId: "cohort-js",
        groupId: "cohort-js",
        producerSendTimeoutMs: 10,
        logLevel: "info",
        producerConfigOverrides: {},
        consumerConfigOverrides: {},
        username: "",
        password: "",
      },
}

export { SDK_CONFIG }