import { BroadcastChannel } from "node:worker_threads"
import { Pool, PoolClient } from "pg"

import { logger } from "./logger"

import { CapturedItemState, CapturedState, TransferRequest } from "./model"
import { Initiator, JsCertificationRequest } from "cohort_sdk_js"
import { SDK_CONFIG as sdkConfig } from "./cohort-sdk-config"

export class BankingApp {
    private startedAtMs: number = 0
    private handledCount: number = 0
    private initiator: Initiator

    constructor(
        private expectedTxCount: number,
        private database: Pool,
        private queue: BroadcastChannel) {}

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
        await this.database.end()
    }

    async processQueueItem(event: MessageEvent<TransferRequest>) {
        if (this.startedAtMs === 0) {
            this.startedAtMs = Date.now()
        }
        await this.handleTransaction(event.data)

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
            // async () => await this.loadState(tx) as any,
            async () => new CapturedState(362078,  [ new CapturedItemState("66807", 0) ]),
            (e, r) => {
                logger.info(`App: Inside OO install callback. Error: ${e}, Request: ${JSON.stringify(r, null, 2)}`)
            }
        )

        this.handledCount += 1
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
        logger.info("BankingApp.loadState(): loading state for: %s", tx)

        let cnn: PoolClient
        try {
            cnn = await this.database.connect()
            const result = await cnn.query(
                `SELECT
                    ba.*, cs."version" AS snapshot_version
                FROM
                    bank_accounts ba, cohort_snapshot cs
                WHERE
                    ba."number" = $1 OR ba."number" = $2`,
                [tx.from, tx.to]
            )

            if (result.rowCount != 2) {
                throw new Error(`Unable to load bank accounts by these ids: '${tx.from}', '${tx.to}'. Query returned: ${result.rowCount} rows.`)
            }

            const items = result.rows.map(row => new CapturedItemState(row.number, row.version))

            // tkae snapshot from any row
            const state = new CapturedState(result.rows[0].snapshot_version, items)

            logger.info("BankingApp.loadState(): Loaded: %s", JSON.stringify(state, null, 2))
            return state
        } finally {
            cnn?.release()
        }
    }
}