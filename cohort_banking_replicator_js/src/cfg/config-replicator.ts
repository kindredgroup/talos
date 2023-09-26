import { JsReplicatorConfig } from "cohort_sdk_client"

const replicatorConfig: JsReplicatorConfig = {
    enableStats: true,
    channelSize: 100_000,
    suffixCapacity: 10_000,
    certifierMessageReceiverCommitFreqMs: 10_000,
    statemapQueueCleanupFreqMs: 10_000,
    statemapInstallerThreadpool: 50,
}

export { replicatorConfig as REPLICATOR_CONFIG }