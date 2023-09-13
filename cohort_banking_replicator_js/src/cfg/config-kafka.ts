import { JsKafkaConfig } from "cohort_sdk_js"

const kafkaConfig: JsKafkaConfig = {
    brokers: ["127.0.0.1:9092"],
    topic: "dev.ksp.certification",
    clientId: "cohort-replicator-js3",
    groupId: "cohort-replicator-js3",
    producerSendTimeoutMs: 10,
    logLevel: "info",
    producerConfigOverrides: {},
    consumerConfigOverrides: { "enable.auto.commit": "false" },
    username: "",
    password: "",
}

export { kafkaConfig as KAFKA_CONFIG }