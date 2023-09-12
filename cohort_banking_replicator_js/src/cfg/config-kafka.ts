import { JsKafkaConfig } from "cohort_sdk_js"

const kafkaConfig: JsKafkaConfig = {
    brokers: ["127.0.0.1:9092"],
    topic: "dev.ksp.certification",
    clientId: "cohortr-replicator-js",
    groupId: "cohortr-replicator-js",
    producerSendTimeoutMs: 10,
    logLevel: "info",
    producerConfigOverrides: {},
    consumerConfigOverrides: { "enable.auto.commit": "false" },
    username: "",
    password: "",
}

export { kafkaConfig as KAFKA_CONFIG }