import { BroadcastChannel } from "node:worker_threads"
import { Pool } from "pg"

import { logger } from "./logger"
import { createGeneratorService } from "./load-generator"
import { BankingApp } from "./banking-app"
import { DB_CONFIG } from "./cfg/config-db-pool"
import { Pond } from "./pond"

logger.info("App: Cohort JS Application: %d", 111)
logger.info("App: ---------------------")

const CHANNEL_NAME = "banking-transactions"
const COUNT = 200_000
const RATE = 4000

const printMetrics = (spans: Array<any>) => {
    for (let span of spans) {
        logger.info("METRIC: %d, %d, %d, %d, %d, %d", span.enqueue, span.process, span.processDetails?.stateDuration, span.processDetails?.stateEnd, span.processDetails?.ooinstallDuration, span.processDetails?.ooinstallEnd)
    }
}

new Promise(async (resolve) => {
    const database = new Pool(DB_CONFIG)
    database.on("error",   (e, _) => { logger.error("DBPool.error: Error: %s", e) })
    database.on("release", (e, _) => { if (e) { logger.error("DBPool.release: Error: %s", e) } })

    const c = await database.connect()
    await c.query("SELECT 1 as test")
    c.release()

    const queue = new BroadcastChannel(CHANNEL_NAME)

    const fnFinish = (appRef: BankingApp) => {
        resolve(1)
        database.end()

        logger.info("Collected metrics: %d", appRef.spans.length)
        printMetrics(appRef.spans)
    }

    const app = new BankingApp(
        COUNT,
        new Pond(400),
        database,
        queue,
        fnFinish,
    )
    await app.init()
    const _worker = createGeneratorService({ channelName: CHANNEL_NAME, count: COUNT, rate: RATE })
})
