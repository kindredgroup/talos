import { InternalReplicator, JsKafkaConfig, JsReplicatorConfig, JsStatemapAndSnapshot, SdkErrorKind } from "@kindredgroup/cohort_sdk_js"
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

    async run(snapshotProviderCallback: () => Promise<number>, statemapInstallerCallback: (data: JsStatemapAndSnapshot) => Promise<void>) {
        try {
            // This will hide the 'error' parameter from callback (it comes from NAPI).
            const adaptedStatemapInstallerCallback = async (error: Error | null, data: JsStatemapAndSnapshot): Promise<void> => {
                if (error) {
                    throw new TalosSdkError(
                        SdkErrorKind.Internal,
                        "Call from native code into 'statemapInstallerCallback' function provided by JS has failed at the NAPI layer. See the 'cause' field for more details.",
                        { cause: error }
                    )
                } else {
                    return await statemapInstallerCallback(data)
                }
            }

            await this.impl.run(snapshotProviderCallback, adaptedStatemapInstallerCallback)
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