import { BroadcastChannel } from "node:worker_threads"
import { Pool, PoolClient } from "pg"

import { logger } from "./logger"

import { CapturedItemState, CapturedState, TransferRequest } from "./model"
import { Initiator, JsCertificationRequest, OoRequest } from "cohort_sdk_js"
import { SDK_CONFIG as sdkConfig } from "./cfg/config-cohort-sdk"

export class BankingApp {
    private startedAtMs: number = 0
    private handledCount: number = 0
    private initiator: Initiator

    constructor(
        private expectedTxCount: number,
        private database: Pool,
        private queue: BroadcastChannel,
        private onFinishListener: () => any) {}

    async init() {
        this.queue.onmessage = async (event: MessageEvent<TransferRequest>) => {
            try {
                await this.processQueueItem(event)
            } catch (e) {
                logger.error("Failed to process tx: %s", e)
            }
        }
        this.initiator = await Initiator.init(sdkConfig)
    }

    async close() {
        this.queue.close()
        this.onFinishListener()
    }

    async processQueueItem(event: MessageEvent<TransferRequest>) {
        if (this.startedAtMs === 0) {
            this.startedAtMs = Date.now()
        }

        try {
            await this.handleTransaction(event.data)
        } catch (e) {
            logger.error("Unable to process tx: %s. Error:: %s", JSON.stringify(event.data), e)
        } finally {
            this.handledCount++
        }

        if (this.handledCount === this.expectedTxCount) {
            await this.close()
            logger.info("App: ---------------------")
            logger.info(
                "\nProcessing finished.\nThroughput: %d (tps)\n     Count: %d\n",
                this.getThroughput(Date.now()).toFixed(2),
                this.handledCount,
            )
        }
    }

    async handleTransaction(tx: TransferRequest) {
        const request: JsCertificationRequest = this.createNewCertRequest(tx)
        await this.initiator.certify(
            request,
            async () => await this.loadState(tx) as any,
            async (_e, request: OoRequest) => await this.installOutOfOrder(tx, request) as any
        )
    }

    getThroughput(nowMs: number): number {
        return this.handledCount / ((nowMs - this.startedAtMs) / 1_000.0)
    }

    private createNewCertRequest(tx: TransferRequest): JsCertificationRequest {
        let request: JsCertificationRequest = {
            timeoutMs: 0,
            candidate: {
                readset: [tx.from, tx.to],
                writeset: [tx.from, tx.to],
                statemap: [{ "TRANSFER": tx }]
            }
        }

        return request
    }

    private async loadState(tx: TransferRequest): Promise<CapturedState> {
        let cnn: PoolClient
        try {
            cnn = await this.database.connect()
            const result = await cnn.query(
                `SELECT
                    ba."number" as "id", ba."version" as "version", cs."version" AS snapshot_version
                FROM
                    bank_accounts ba, cohort_snapshot cs
                WHERE
                    ba."number" = $1 OR ba."number" = $2`,
                [tx.from, tx.to]
            )

            if (result.rowCount != 2) {
                throw new Error(`Unable to load bank accounts by these ids: '${tx.from}', '${tx.to}'. Query returned: ${result.rowCount} rows.`)
            }

            const items = result.rows.map(row => new CapturedItemState(row.id, Number(row.version)))
            // take snapshot from any row
            return new CapturedState(Number(result.rows[0].snapshot_version), items)
        } catch (e) {
            logger.error("BankingApp.loadState(): %s", e)
            throw e
        } finally {
            cnn?.release()
        }
    }

    private async installOutOfOrder(tx: TransferRequest, request: OoRequest): Promise<number> {
        let cnn: PoolClient
        try {
            // Params order:
            //  1 - from, 2 - to, 3 - amount
            //  4 - new_ver, 5 - safepoint
            let sql = `
            WITH bank_accounts_temp AS (
                UPDATE bank_accounts ba SET
                    "amount" =
                        (CASE
                            WHEN ba."number" = ($1)::TEXT THEN ba."amount" + ($3)::DECIMAL
                            WHEN ba."number" = ($2)::TEXT THEN ba."amount" - ($3)::DECIMAL
                        END),
                    "version" = ($4)::BIGINT
                WHERE ba."number" IN (($1)::TEXT, ($2)::TEXT)
                    AND EXISTS (SELECT 1 FROM cohort_snapshot cs WHERE cs."version" >= ($5)::BIGINT)
                    AND ba."version" < ($4)::BIGINT
                RETURNING
                    ba."number", ba."version" as "new_version", (null)::BIGINT as "version", (SELECT cs."version" FROM cohort_snapshot cs) as "snapshot"
            )
            SELECT * FROM bank_accounts_temp
            UNION
            SELECT
                ba."number", (null)::BIGINT as "new_version", ba."version" as "version", cs."version" as "snapshot"
            FROM
                bank_accounts ba, cohort_snapshot cs
            WHERE ba."number" IN (($1)::TEXT, ($2)::TEXT)`

            cnn = await this.database.connect()
            const result = await cnn.query(sql, [tx.from, tx.to, tx.amount, request.newVersion, request.safepoint])

            if (result.rowCount === 0) {
                // installed already
                return 1
            }

            // Quickly grab the snapshot to check whether safepoint condition is satisfied. Any row can be used for that.
            const snapshot = result.rows[0].snapshot
            if (snapshot < request.safepoint) {
                // safepoint condition
                return 2
            }

            // installed
            return 0

        } catch (e) {
            logger.error("BankingApp.installOutOfOrder(): %s", e)
        } finally {
            cnn?.release()
        }
    }
}