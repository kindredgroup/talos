import { InternalReplicator, JsKafkaConfig, JsReplicatorConfig, JsStatemapAndSnapshot, SdkErrorKind } from "cohort_sdk_js"
import { isSdkError } from "./internal"
import { TalosSdkError } from "."

export class Replicator {
    static async init(kafkaConfig: JsKafkaConfig, config: JsReplicatorConfig): Promise<Replicator> {
        try {
            return new Replicator(await InternalReplicator.init(kafkaConfig, config))
        } catch(e) {
            const reason: string = e.message
            if (isSdkError(reason)) {
                const rawError = JSON.parse(reason)
                throw new TalosSdkError(rawError.kind, rawError.reason, { cause: e })
            } else {
                throw new TalosSdkError(SdkErrorKind.Internal, e.message, { cause: e })
            }
        }
    }

    constructor(readonly impl: InternalReplicator) {}

    async run(snapshotProviderCallback: () => Promise<number>, statemapInstallerCallback: (err: Error | null, value: JsStatemapAndSnapshot) => any) {
        try {
            await this.impl.run(snapshotProviderCallback, statemapInstallerCallback)
        } catch(e) {
            const reason: string = e.message
            if (isSdkError(reason)) {
                const rawError = JSON.parse(reason)
                throw new TalosSdkError(rawError.kind, rawError.reason, { cause: e })
            } else {
                throw new TalosSdkError(SdkErrorKind.Internal, e.message, { cause: e })
            }
        }
    }
}