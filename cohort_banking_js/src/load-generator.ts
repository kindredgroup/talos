import { BroadcastChannel, Worker, isMainThread, workerData } from "node:worker_threads"
import { logger } from "./logger"
import { TransferRequest } from "./model"

export interface ItemHandler {
    handle(item: TransferRequest): Promise<void>
}

export class LoadGenerator {
    private generatedItems: Array<TransferRequest>

    constructor(private accountsRange: number, private historySize: number | null) {
        this.generatedItems = new Array()
        this.historySize = historySize || 0
        logger.info("historySize: %d", this.historySize)
    }

    generate(): TransferRequest {
        const checkTimeout = (elapsedMs: number) => {
            if (elapsedMs >= 20_000) {
                logger.warn("should throw error: %d", elapsedMs)
                throw new Error(`Timeout (20 sec). Unable to generate a unique pair of account numbers from this range: [1, ${this.accountsRange}]`)
            }
        }

        const startedAt = Date.now()
        while (true) {
            let from = Math.floor(Math.random() * this.accountsRange) + 1
            let to: null | number = null

            while (true) {
                to = Math.floor(Math.random() * this.accountsRange) + 1
                if (to !== from) {
                    break
                }
                checkTimeout(Date.now() - startedAt)
                continue
            }

            if (this.historySize > 0) {
                const fromText = from.toString()
                const toText = to.toString()
                const duplicate = this.generatedItems.find(el => {
                    return el.from === fromText && el.to === toText || el.from === toText && el.to === fromText
                })

                if (duplicate) {
                    checkTimeout(Date.now() - startedAt)
                    continue
                }
            }

            const request = new TransferRequest(from.toString(), to.toString(), 1.0)
            if (this.historySize > 0) {
                if (this.generatedItems.length === this.historySize) {
                    this.generatedItems.shift()
                }
                this.generatedItems.push(request)
            }
            return request
        }
    }
}

export function createGeneratorService(settings: any): Worker {
    return new Worker(__filename, { workerData: { settings } })
}

if (!isMainThread) {
    const { count, channelName, rate } = workerData.settings
    const generator = new LoadGenerator(100_000, 10)
    logger.info("Load generator will generate: %d transactions", count)

    new Promise(async () => {
        const txChannel = new BroadcastChannel(channelName)
        const startedAt = Date.now()
        for (let i = 1.0; i <= count; i++) {
            const request = generator.generate()
            txChannel.postMessage(request)

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
        txChannel.close()
        logger.info("Worker has closed the channel")
    }).finally(() => {
        logger.info("Generartor is finished")
    })
}