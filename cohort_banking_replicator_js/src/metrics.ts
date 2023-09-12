import { BroadcastChannel } from "worker_threads"
import { logger } from "./logger"

const METRICS_CHANNEL_NAME = "REPLICATOR_METRICS"

export class MetricsSet {
    itemsWithoutStatemaps: number = 0
    attemptsUsed: number = 0
    noopInstallsCount: number = 0
    noopSnapshotUpdatesCount: number = 0
    installed: number = 0
    callbackGetSnapshotCount: number = 0
    callbackInstallCount: number = 0
    updatedAt: number = 0

    constructor() {}

    static merge(oldValue: MetricsSet, newValue: MetricsSet): MetricsSet {
        return {
            itemsWithoutStatemaps: newValue.itemsWithoutStatemaps + oldValue.itemsWithoutStatemaps,
            attemptsUsed: newValue.attemptsUsed + oldValue.attemptsUsed,
            noopInstallsCount: newValue.noopInstallsCount + oldValue.noopInstallsCount,
            noopSnapshotUpdatesCount: newValue.noopSnapshotUpdatesCount + oldValue.noopSnapshotUpdatesCount,
            installed: newValue.installed + oldValue.installed,
            callbackGetSnapshotCount: newValue.callbackGetSnapshotCount + oldValue.callbackGetSnapshotCount,
            callbackInstallCount: newValue.callbackInstallCount + oldValue.callbackInstallCount,
            updatedAt: Date.now()
        }
    }
}

export class MetricsCollector {
    run() {
        let metrics = new MetricsSet()
        const metricsChannel = new BroadcastChannel(METRICS_CHANNEL_NAME)
        metricsChannel.onmessage = (event: MessageEvent<MetricsSet>) => {
            metrics = MetricsSet.merge(metrics, event.data)
        }

        let startedAt = 0
        let previousMetrics: MetricsSet
        setInterval(() => {
            if (metrics.updatedAt == 0) {
                logger.error("METRICS: no data collected yet")
                return
            }

            let throughput = 0
            let currentThroughput = 0
            let dataChanged = false
            if (previousMetrics) {
                dataChanged = metrics.updatedAt > previousMetrics.updatedAt
                if (!dataChanged) {
                    return
                }

                //metrics.callbackGetSnapshotCount += previousMetrics.callbackGetSnapshotCount

                let elapsedSec = (metrics.updatedAt - startedAt) / 1000.0
                throughput = metrics.installed / elapsedSec

                elapsedSec = (metrics.updatedAt - previousMetrics.updatedAt) / 1000.0
                currentThroughput = (metrics.installed - previousMetrics.installed) / elapsedSec

            } else {
                startedAt = metrics.updatedAt
            }

            previousMetrics = { ...metrics }

            logger.warn("METRICS: %s", JSON.stringify({ ...metrics, currentThroughput, throughput }, null, 2))
        }, 10_000)
    }
}

export { METRICS_CHANNEL_NAME }