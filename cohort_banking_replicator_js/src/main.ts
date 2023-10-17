import { JsStatemapAndSnapshot, Replicator, TalosSdkError } from "@kindredgroup/cohort_sdk_client"
import { Database } from "./database"

import { KAFKA_CONFIG } from "./cfg/config-kafka"
import { REPLICATOR_CONFIG } from "./cfg/config-replicator"

import { MetricsCollector } from "./metrics"
import { logger } from "./logger"

new Promise(async (_resolve) => {
    const database = await Database.init()
    const replicator = await Replicator.init(KAFKA_CONFIG, REPLICATOR_CONFIG)

    const metricsCollector = new MetricsCollector()
    metricsCollector.run()

    try {
        await replicator.run(
            async () => await database.getSnapshot(),
            async (data: JsStatemapAndSnapshot) => await database.install(data, { delayMs: 2, maxAttempts: 50 })
        )
        logger.info("Replciator is running ...")
    } catch (e) {
        logger.error("Unable start replicator.")
        if (e instanceof TalosSdkError) {
            const sdkError = e as TalosSdkError
            logger.error("TalosSdkError.message: %s", sdkError.message)
            logger.error("TalosSdkError.kind: %s", sdkError.kind)
            logger.error("TalosSdkError.name: %s", sdkError.name)
            logger.error("TalosSdkError.cause: %s", sdkError.cause)
            logger.error("TalosSdkError.stack: %s", sdkError.stack)
        } else {
            logger.error("Error: %s", e)
        }

        throw e
    }
})