import { InternalInitiator, JsInitiatorConfig, OutOfOrderRequest, SdkErrorKind } from "cohort_sdk_js"
import { isSdkError } from "./internal"
import { TalosSdkError } from "."

export class Initiator {
    static async init(config: JsInitiatorConfig): Promise<Initiator> {
        try {
            return new Initiator(await InternalInitiator.init(config))
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

    constructor(readonly impl: InternalInitiator) {}

    async certify(makeNewRequestCallback: () => Promise<any>, oooCallback: (err: Error | null, value: OutOfOrderRequest) => any): Promise<void> {
        try {
            return await this.impl.certify(makeNewRequestCallback, oooCallback)
        } catch (e) {
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