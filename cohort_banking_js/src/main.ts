import { BroadcastChannel } from "node:worker_threads"
import { Pool } from "pg"

import { logger } from "./logger"
import { createGeneratorService } from "./load-generator"
import { BankingApp } from "./banking-app"
import { DB_CONFIG } from "./cfg/config-db-pool"

logger.info("App: Cohort JS Application: %d", 111)
logger.info("App: ---------------------")

const CHANNEL_NAME = "banking-transactions"
const COUNT = 25_000
const RATE = 4_000

new Promise(async (resolve) => {
    const database = new Pool(DB_CONFIG)
    database.on("error",   (e, _) => { logger.error("DBPool.error: Error: %s", e) })
    database.on("release", (e, _) => { if (e) { logger.error("DBPool.release: Error: %s", e) } })

    const c = await database.connect()
    await c.query("SELECT 1 as test")
    c.release()

    const queue = new BroadcastChannel(CHANNEL_NAME)

    const fnFinish = () => {
        resolve(1)
        database.end()
    }
    const app = new BankingApp(COUNT, database, queue, fnFinish)
    await app.init()
    const _worker = createGeneratorService({ channelName: CHANNEL_NAME, count: COUNT, rate: RATE })
})
