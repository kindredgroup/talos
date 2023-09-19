import { Pool, PoolClient } from "pg"
import { BroadcastChannel } from "worker_threads"

import { logger } from "./logger"
import { JsStatemapAndSnapshot } from "cohort_sdk_client"
import { DB_CONFIG } from "./cfg/config-db-pool"
import { METRICS_CHANNEL_NAME, MetricsSet } from "./metrics"


class TransferRequest {
    constructor(public from: string, public to: string, public amount: number) {}
}

class RetryConfig {
    constructor(public delayMs: number, public maxAttempts: number) {}
}

export class Database {
    static async init(): Promise<Database> {
        const pool = new Pool(DB_CONFIG)
        pool.on("error",   (e, _) => { logger.error("DBPool.error: Error: %s", e) })
        pool.on("release", (e, _) => { if (e) { logger.error("DBPool.release: Error: %s", e) } })

        const c = await pool.connect()
        await c.query("SELECT 1 as test")
        c.release()

        return new Database(pool, new BroadcastChannel(METRICS_CHANNEL_NAME))
    }

    private constructor(readonly pool: Pool, readonly metricsChannel: BroadcastChannel) {
    }

    async getSnapshot(): Promise<number> {
        let cnn = null
        try {
            cnn = await this.pool.connect()

            const result = await cnn.query({
                name: "getSnapshot",
                text: `SELECT "version" FROM cohort_snapshot WHERE id = $1`,
                values: ["SINGLETON"]
            })

            if (result.rowCount === 0) {
                throw "There is no snapshot in the database."
            }

            const version = result.rows[0].version

            logger.warn("Database.getSnapshot(): %d", version)

            return Number(version)

        } finally {
            cnn?.release()
            const metric = new MetricsSet()
            metric.callbackGetSnapshotCount++
            metric.updatedAt = Date.now()
            this.metricsChannel.postMessage(metric)
        }
    }

    private async installStatemap(params: JsStatemapAndSnapshot, cnn: PoolClient): Promise<number> {
        const sql = `
        UPDATE bank_accounts ba SET
        "amount" =
        (CASE
            WHEN ba."number" = ($1)::TEXT THEN ba."amount" - ($3)::DECIMAL
            WHEN ba."number" = ($2)::TEXT THEN ba."amount" + ($3)::DECIMAL
            END),
            "version" = ($4)::BIGINT
            WHERE ba."number" IN (($1)::TEXT, ($2)::TEXT)
            AND ba."version" < ($4)::BIGINT`

        const itemToInstall = params.statemap[0]
        if (itemToInstall.action != "TRANSFER") throw `Invalid statemap. Unknown action: "${itemToInstall.action}"`

        const request: TransferRequest = itemToInstall.payload

        const result = await cnn.query({
            name: "installStatemap",
            text: sql,
            values: [request.from, request.to, request.amount, params.version]
        })

        return result.rowCount
    }

    async install(params: JsStatemapAndSnapshot, retry: RetryConfig) {
        const cnn = await this.pool.connect()
        const snapshotVersion = params.version
        let attemptNr = 0
        let startedAt = Date.now()
        const metric = new MetricsSet()
        metric.callbackInstallCount++

        try {
            let recentError = null as any
            while (attemptNr <= retry.maxAttempts) {
                attemptNr++

                if (attemptNr > retry.maxAttempts) {
                    const elapsedSec = (Date.now() - startedAt) / 1000.0
                    throw { message: `Statemap install timed out after: ${elapsedSec} sec`, error: recentError }
                }

                await cnn.query('BEGIN')
                try {
                    if (params.statemap.length > 0) {
                        const updatedRowsCount = await this.installStatemap(params, cnn)
                        if (updatedRowsCount == 0) {
                            metric.noopInstallsCount++

                            logger.debug(
                                "Database.install(): No rows were updated when installing: %s. Snapshot will be set to: %d",
                                JSON.stringify(params.statemap),
                                snapshotVersion
                            )
                        } else {
                            logger.debug(
                                "Database.install(): %d rows were updated when installing: {:?}. Snapshot will be set to: %d",
                                updatedRowsCount,
                                JSON.stringify(params.statemap),
                                snapshotVersion
                            )
                        }
                    } else {
                        metric.itemsWithoutStatemaps++
                    }

                    const result = await cnn.query({
                        name: "updateSnapshot",
                        text: `UPDATE cohort_snapshot SET "version" = ($1)::BIGINT WHERE id = $2 AND "version" < ($1)::BIGINT`,
                        values: [snapshotVersion, "SINGLETON"]
                    })

                    if (result.rowCount === 0) {
                        metric.noopSnapshotUpdatesCount++
                        logger.debug("Database.install(): No rows were updated when updating snapshot. Snapshot is already set to %d or higher", snapshotVersion)
                    } else {
                        logger.debug("Database.install(): %d rows were updated when updating snapshot to %d", result.rowCount, snapshotVersion);
                    }

                    await cnn.query('COMMIT')
                    metric.installed++
                    break

                } catch (e) {
                    let error = { installError: e, rollbackError: null }
                    try {
                        await cnn.query('ROLLBACK')
                    } catch (e) {
                        error.rollbackError = e
                    } finally {
                        recentError = error
                    }

                    await new Promise(resolve => setTimeout(resolve, retry.delayMs))
                }
            }
        } finally {
            cnn?.release()
            metric.attemptsUsed = attemptNr
            this.metricsChannel.postMessage(metric)
        }
    }
}