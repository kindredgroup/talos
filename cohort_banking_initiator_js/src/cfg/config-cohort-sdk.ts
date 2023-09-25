import { JsInitiatorConfig } from "cohort_sdk_client"

const SDK_CONFIG: JsInitiatorConfig = {
    backoffOnConflict: {
        minMs: 2,
        maxMs: 1500,
      },
      retryBackoff: {
        minMs: 20,
        maxMs: 1500,
      },
      retryAttemptsMax: 10,
      retryOoBackoff: {
        minMs: 20,
        maxMs: 1000,
      },
      retryOoAttemptsMax: 10,
      snapshotWaitTimeoutMs: 10_000,
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
        consumerConfigOverrides: {
            "enable.auto.commit": "false"
        },
        username: "",
        password: "",
      },
}

export { SDK_CONFIG }