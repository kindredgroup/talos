import { JsKafkaConfig } from "@kindredgroup/cohort_sdk_client"

const getParam = (param: string, defaultValue: string): string => {
    const v = process.env[param]
    if (v) return v
    return defaultValue
}

const kafkaConfig: JsKafkaConfig = {
    brokers: ["127.0.0.1:9092"],
    topic: getParam("KAFKA_TOPIC", "dev.ksp.certification"),
    clientId: "cohort-replicator-js",
    groupId: "cohort-replicator-js",
    producerSendTimeoutMs: 10,
    logLevel: "info",
    producerConfigOverrides: {},
    consumerConfigOverrides: { "enable.auto.commit": "false" },
    username: getParam("KAFKA_USERNAME", ""),
    password: getParam("KAFKA_PASSWORD", ""),
}

export { kafkaConfig as KAFKA_CONFIG }