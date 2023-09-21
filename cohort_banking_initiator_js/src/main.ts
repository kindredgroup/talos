import { BroadcastChannel } from "node:worker_threads"
import { Pool } from "pg"

import { logger } from "./logger"
import { createGeneratorService } from "./load-generator"
import { BankingApp } from "./banking-app"
import { DB_CONFIG } from "./cfg/config-db-pool"
import { Pond } from "./pond"
import { TalosSdkError, SdkErrorKind } from "cohort_sdk_client"

logger.info("App: Cohort JS Application: %d", 111)
logger.info("App: ---------------------")

const CHANNEL_NAME = "banking-transactions"

const printMetrics = (spans: Array<any>) => {
    for (let span of spans) {
        logger.info("METRIC: %d, %d, %d, %d, %d, %d", span.enqueue, span.process, span.processDetails?.stateDuration, span.processDetails?.stateEnd, span.processDetails?.ooinstallDuration, span.processDetails?.ooinstallEnd)
    }
}

class LaunchParams {
    transactionsCount: number = 10_000
    targetRatePerSecond: number = 1_000

    static parse(args: string[]): LaunchParams {
        const params = new LaunchParams()
        if (args.length <= 2) {
            logger.warn("No launch parameters found, using defaults.")
            return params
        }

        for (let i = 2; i < args.length; i++) {
            const arg = args[i].toLowerCase()

            if (arg.startsWith("count")) {
                params.transactionsCount = parseInt(arg.replaceAll("count=", ""))
            } else if (arg.startsWith("rate")) {
                params.targetRatePerSecond = parseInt(arg.replaceAll("rate=", ""))
            }
        }

        return params
    }
}

new Promise(async (resolve) => {
    const params = LaunchParams.parse(process.argv)

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
        //printMetrics(appRef.spans)
    }

    const app = new BankingApp(
        params.transactionsCount,
        new Pond(400),
        database,
        queue,
        fnFinish,
    )

    try {
        await app.init()
    } catch (e) {
        // this is an example of error parsing
        console.log(JSON.stringify(e, null, 2))
        if (e instanceof TalosSdkError) {
            const sdkError = e as TalosSdkError
            if (sdkError.kind == SdkErrorKind.Messaging) {
                logger.error("Unable to connect to kafka....")
            }
        }
        throw e
    }
    const _worker = createGeneratorService({ channelName: CHANNEL_NAME, count: params.transactionsCount, rate: params.targetRatePerSecond })
})
