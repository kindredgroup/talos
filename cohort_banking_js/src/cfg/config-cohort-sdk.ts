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
      brokers: "127.0.0.1:9092",
      topic: "dev.ksp.certification",
      saslMechanisms: null,
      kafkaUsername: null,
      kafkaPassword: null,
      agentGroupId: "cohort-js",
      agentFetchWaitMaxMs: 6000,
      agentMessageTimeoutMs: 15000,
      agentEnqueueTimeoutMs: 10,
      agentLogLevel: 2,
      dbPoolSize: 2,
      dbUser: "postgres",
      dbPassword: "admin",
      dbHost: "127.0.0.1",
      dbPort: "5432",
      dbDatabase: "talos-sample-cohort-dev"
}

export { SDK_CONFIG }