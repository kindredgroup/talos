import { BroadcastChannel } from "node:worker_threads"
import { Pool, PoolClient } from "pg"
import * as _ from "lodash";

import { logger } from "./logger"

import { CapturedItemState, CapturedState, TransferRequest, TransferRequestMessage } from "./model"
import { Initiator, JsCertificationRequest, OoRequest } from "cohort_sdk_js"
import { SDK_CONFIG as sdkConfig } from "./cfg/config-cohort-sdk"

const fnFormatAccountNr = (nr: number) => {
    let asText = nr.toString()
    while (asText.length < 4) {
        asText = '0' + asText
    }

    return asText
}


export class BankingApp {
    private startedAtMs: number = 0
    handledCount: number = 0
    installedByReplicator: number = 0;
    private initiator: Initiator

    spans: Array<any> = []
    transfers: Array<TransferRequest> = []


    constructor(
        private expectedTxCount: number,
        private database: Pool,
        private queue: BroadcastChannel,
        private onFinishListener: (appRef: BankingApp) => any) { }

    async delay(time: number) {
        return new Promise(resolve => setTimeout(resolve, time));
    }

    async generate_payload(count: number, rate: number): Promise<Array<Array<TransferRequest>>> {
        let first_number = 1;
        let transfers = [];
        const startedAt = Date.now()

        for (let i = 1.0; i <= count; i++) {

            const request = new TransferRequest(fnFormatAccountNr(first_number++), fnFormatAccountNr(first_number++), 1.0,);
            transfers.push(request);

            if (i % (count * 10 / 100) === 0) {
                const elapsed = (Date.now() - startedAt) / 1000.0
                logger.info("Generated: %d, effective rate: %d", i, (i / elapsed).toFixed(2))
            }
            if (i == count) break

            const now = Date.now()
            const elapsedSec = (now - startedAt) / 1000.0
            const currentRate = i / elapsedSec
            if (currentRate > rate) {
                const targetElapsed = i / rate
                const delta = (targetElapsed - elapsedSec) * 1000
                await new Promise(resolve => setTimeout(resolve, delta));
            }
        }
        const elapsed = (Date.now() - startedAt) / 1000.0
        logger.info("\nGenerator finished.\n Generated: %d\nThroughput: %d\n   Elapsed: %d (sec)\n", count, (count / elapsed).toFixed(2), elapsed.toFixed(2))

        const chunkSize = count / 2_000;

        return _.chunk(transfers, chunkSize);
    }

    async process_payload(bucket: Array<TransferRequest>) {
        if (this.startedAtMs === 0) {
            this.startedAtMs = Date.now()
        }

        let rev_bucket = _.reverse(bucket);
        let loop_till = bucket.length;

        for (let i = 1; i <= loop_till; i++) {
            // this.handledCount++;
            try {
                // await this.delay(100)
                const request = rev_bucket.pop();
                if (!!request) {
                    if (i % 100 === 0) {
                        logger.info("Request payload ", request);
                    }


                    const spans = await this.handleTransaction(request);
                    this.spans.push(spans)

                }
            } catch (e) {
                logger.error("Failed to process tx: %s", e)
            }
            if (i % 10 == 0) {
                await this.delay(50)
            }
        }
    }

    async process_transfers() {
        while (true) {
            if (this.transfers.length > 0) {

                const chunkSize = this.transfers.length / 1_000;

                let transferChunks = _.chunk(this.transfers, chunkSize);

                this.handledCount += this.transfers.length;
                this.transfers = [];

                await Promise.all(transferChunks.map((r: TransferRequest[]) => this.process_payload(r)))

            }

            if (this.handledCount >= this.expectedTxCount) {
                logger.info("App: ---------------------")
                logger.info(
                    "\nProcessing finished.\nThroughput: %d (tps)\n     Count: %d\n",
                    this.getThroughput(Date.now()).toFixed(2),
                    this.handledCount,
                )
                await this.close()
            }

            logger.info("Current count of items processed... %d", this.handledCount);

            await this.delay(200)

        }

    }


    async init() {
        this.queue.onmessage = async (event: MessageEvent<TransferRequestMessage>) => {

            this.transfers.push(event.data.request)
        }
        this.initiator = await Initiator.init(sdkConfig)
    }

    async close() {
        this.queue.close()
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

        return this.handledCount
    }

    async handleTransaction(tx: TransferRequest): Promise<any> {
        const request: JsCertificationRequest = this.createNewCertRequest(tx)

        const span_s = Date.now()
        let state = 0
        let ooinstall = 0
        await this.initiator.certify(
            request,
            async () => {
                const r = await this.loadState(tx) as any
                state = Date.now() - span_s
                return r
            },
            async (_e, request: OoRequest) => {
                // const start_ms = Date.now();
                // await this.delay(200)
                const r = await this.installOutOfOrder(tx, request) as any
                ooinstall = Date.now() - span_s

                if (r === 1) {
                    this.installedByReplicator++
                }
                return r
            }
        )

        return { state, ooinstall }
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
            const result = await cnn.query({
                name: "get-state", text:
                    `SELECT
                        ba."number" as "id", ba."version" as "version", cs."version" AS snapshot_version
                    FROM
                        bank_accounts ba, cohort_snapshot cs
                    WHERE
                        ba."number" = $1 OR ba."number" = $2`,
                values: [tx.from, tx.to]
            })

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
        let attempts = 3;
        while (true) {
            attempts--;

            let cnn: PoolClient
            try {
                // Params order:
                //  1 - from, 2 - to, 3 - amount
                //  4 - new_ver, 5 - safepoint
                // let sql = `
                // SELECT
                //     ba."number", (null)::BIGINT as "new_version", ba."version" as "version", cs."version" as "snapshot"
                // FROM
                //     bank_accounts ba, cohort_snapshot cs
                // WHERE ba."number" IN (($1)::TEXT, ($2)::TEXT)`
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
                const result = await cnn.query({ text: sql, values: [tx.from, tx.to, tx.amount, request.newVersion, request.safepoint] })
                // const result = await cnn.query({ text: sql, values: [tx.from, tx.to] })

                if (result.rowCount === 0) {
                    // installed already
                    logger.info("Installed already.... %d ", request.newVersion)
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

                if (attempts == 0) {
                    break;
                }

                await this.delay(200);

            } finally {
                cnn?.release()
            }
        }
    }
}