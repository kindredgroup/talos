import { Replicator } from "cohort_sdk_client"
import { Database } from "./database"

import { KAFKA_CONFIG } from "./cfg/config-kafka"
import { REPLICATOR_CONFIG } from "./cfg/config-replicator"

import { MetricsCollector } from "./metrics"

new Promise(async (_resolve) => {
    const database = await Database.init()
    const replicator = await Replicator.init(KAFKA_CONFIG, REPLICATOR_CONFIG)

    const metricsCollector = new MetricsCollector()
    metricsCollector.run()

    await replicator.run(
        async () => await database.getSnapshot(),
        async (_, params) => await database.install(params, { delayMs: 2, maxAttempts: 50 })
    )
})