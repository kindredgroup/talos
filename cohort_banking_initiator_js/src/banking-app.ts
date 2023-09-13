import { BroadcastChannel } from "node:worker_threads"
import { Pool, PoolClient } from "pg"

import { Pond } from "./pond"

import { logger } from "./logger"

import { CapturedItemState, CapturedState, CertificationCandidate, CertificationRequest, TransferRequest, TransferRequestMessage } from "./model"
import { Initiator, JsCertificationRequestPayload, OutOfOrderRequest } from "cohort_sdk_js"
import { SDK_CONFIG as sdkConfig } from "./cfg/config-cohort-sdk"

export class BankingApp {
    private startedAtMs: number = 0
    private handledCount: number = 0
    private initiator: Initiator

    spans: Array<any> = []

    constructor(
        private expectedTxCount: number,
        private pond: Pond,
        private database: Pool,
        private queue: BroadcastChannel,
        private onFinishListener: (appRef: BankingApp) => any) {}

    async init() {
        this.initiator = await Initiator.init(sdkConfig)
        this.queue.onmessage = (event: MessageEvent<TransferRequestMessage>) => {
            this.pond.submit(async () => {
                try {
                    const spans = await this.processQueueItem(event)
                    this.spans.push(spans)
                } catch (e) {
                    logger.error("Failed to process tx: %s", e)
                }
            })
        }
    }

    async close() {
        this.queue.close()
        await this.pond.drain()
        this.onFinishListener(this)
    }

    async processQueueItem(event: MessageEvent<TransferRequestMessage>): Promise<any> {
        if (this.startedAtMs === 0) {
            this.startedAtMs = Date.now()
        }

        const span_s = Date.now()
        const spans = {
            enqueue: span_s - event.data.postedAtMs,
            process: 0,
            processDetails: {}
        }

        try {
            const subSpans = await this.handleTransaction(event.data.request)
            spans.processDetails = subSpans
        } catch (e) {
            logger.error("Unable to process tx: %s. Error:: %s", JSON.stringify(event.data), e)
        } finally {
            this.handledCount++
            spans.process = Date.now() - span_s
        }

        if (this.handledCount === this.expectedTxCount) {
            logger.info("App: ---------------------")
            logger.info(
                "\nProcessing finished.\nThroughput: %d (tps)\n     Count: %d\n",
                this.getThroughput(Date.now()).toFixed(2),
                this.handledCount,
            )
            await this.close()
        }

        return spans
    }

    async handleTransaction(tx: TransferRequest): Promise<any> {
        const span_s = Date.now()
        let stateEnd = 0
        let stateDuration = 0
        let ooinstallEnd = 0
        let ooinstallDuration = 0
        await this.initiator.certify(
            async () => {
                const s = Date.now()
                const newRequest = await this.createNewRequest(tx) as any
                const n = Date.now()
                stateEnd = n - span_s
                stateDuration = n - s
                return { newRequest }
            },
            async (_e, request: OutOfOrderRequest) => {
                const s = Date.now()
                const r = await this.installOutOfOrder(tx, request) as any
                const n = Date.now()
                ooinstallEnd = n - span_s
                ooinstallDuration = n - s
                return r
            }
        )

        return { stateDuration, stateEnd, ooinstallDuration, ooinstallEnd }
    }

    getThroughput(nowMs: number): number {
        return this.handledCount / ((nowMs - this.startedAtMs) / 1_000.0)
    }

    private async createNewRequest(tx: TransferRequest): Promise<JsCertificationRequestPayload> {
        const state = await this.loadState(tx)

        return {
            candidate: {
                readset: [tx.from, tx.to],
                writeset: [tx.from, tx.to],
                readvers: state.items.map(i => i.version),
                statemaps: [{ "TRANSFER": tx }],
            },
            snapshot: state.snapshotVersion,
            timeoutMs: 0,
        }
    }

    private async loadState(tx: TransferRequest): Promise<CapturedState> {
        let cnn: PoolClient
        try {
            cnn = await this.database.connect()
            const result = await cnn.query({ name: "get-state", text:
                `SELECT
                    ba."number" as "id", ba."version" as "version", cs."version" AS snapshot_version
                FROM
                    bank_accounts ba, cohort_snapshot cs
                WHERE
                    ba."number" = $1 OR ba."number" = $2`,
                values: [tx.from, tx.to] }
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

    private async installOutOfOrder(tx: TransferRequest, request: OutOfOrderRequest): Promise<number> {
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
            const result = await cnn.query({ name: "ooinstall", text: sql, values: [tx.from, tx.to, tx.amount, request.newVersion, request.safepoint] })

            if (result.rowCount === 0) {
                // installed already
                return 1
            }

            // Quickly grab the snapshot to check whether safepoint condition is satisfied. Any row can be used for that.
            const snapshot = Number(result.rows[0].snapshot)
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