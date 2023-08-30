import { BroadcastChannel } from "node:worker_threads"
import { Pool } from "pg"

import { logger } from "./logger"
import { createGeneratorService } from "./load-generator"
import { BankingApp } from "./banking-app"
import { DB_CONFIG } from "./cfg/config-db-pool"

logger.info("App: Cohort JS Application: %d", 111)
logger.info("App: ---------------------")

const CHANNEL_NAME = "banking-transactions"
const COUNT = 200_000
const RATE = 5_000

const printMetrics = (spans: Array<any>) => {
    for (let span of spans) {
        logger.info("METRIC: %d, %d, %d, %d", span.enqueue, span.process, span.processDetails?.state, span.processDetails?.ooinstall)
    }
}

new Promise(async (resolve) => {
    const database = new Pool(DB_CONFIG)
    database.on("error", (e, _) => { logger.error("DBPool.error: Error: %s", e) })
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

    const app = new BankingApp(COUNT, database, queue, fnFinish)
    await app.init()
    let transferChunks = await app.generate_payload(COUNT, RATE)


    let arr = await Promise.all(transferChunks.map(r => app.process_payload(r)))

    logger.info("App: ---------------------")
    logger.info(
        "\nProcessing finished.\nThroughput: %d (tps)\n     Count: %d\n     Items installed by replicator: %d\n",
        app.getThroughput(Date.now()).toFixed(2),
        app.handledCount,
        app.installedByReplicator
    )
    await app.close();
    // const _worker = createGeneratorService({ channelName: CHANNEL_NAME, count: COUNT, rate: RATE })
})
