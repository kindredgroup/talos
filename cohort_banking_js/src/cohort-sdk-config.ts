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
      bufferSize: 2,
      timeoutMs: 2,
      brokers: "127.0.0.1:9092",
      topic: "dev.ksp.certification",
      saslMechanisms: null,
      kafkaUsername: null,
      kafkaPassword: null,
      agentGroupId: "cohort-js",
      agentFetchWaitMaxMs: 2,
      agentMessageTimeoutMs: 2,
      agentEnqueueTimeoutMs: 2,
      agentLogLevel: 2,
      dbPoolSize: 2,
      dbUser: "postgres",
      dbPassword: "admin",
      dbHost: "127.0.0.1",
      dbPort: "5432",
      dbDatabase: "talos-sample-cohort-dev"
}

export { SDK_CONFIG }