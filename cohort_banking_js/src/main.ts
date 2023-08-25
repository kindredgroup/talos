import { createGeneratorService } from "./load-generator"
import { logger } from "./logger"
import { BroadcastChannel } from "node:worker_threads"
import { BankingApp } from "./banking-app"
import { Pool, PoolConfig } from "pg"

logger.info("App: Cohort JS Application: %d", 111)
logger.info("App: ---------------------")

const CHANNEL_NAME = "banking-transactions"
const COUNT = 1
const RATE = 1

const dbConfig: PoolConfig = {
    application_name: "cohort_banking_js",
    keepAlive: true,
    host: "127.0.0.1",
    port: 5432,
    database: "talos-sample-cohort-dev",
    user: "postgres",
    password: "admin",
    max: 40,
    min: 40,
}

new Promise(async (resolve) => {
    const database = new Pool(dbConfig)
    const c = await database.connect()
    await c.query("SELECT 1 as test")
    c.release()

    const queue = new BroadcastChannel(CHANNEL_NAME)
    const app = new BankingApp(COUNT, database, queue)
    await app.init()
    const _worker = createGeneratorService({ channelName: CHANNEL_NAME, count: COUNT, rate: RATE })
    resolve(1)
})
