import { InternalInitiator, JsInitiatorConfig, OutOfOrderRequest, SdkErrorKind, JsCertificationResponse, JsCertificationCandidateCallbackResponse } from "@kindredgroup/cohort_sdk_js"
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

    async certify(makeNewRequestCallback: () => Promise<any>, oooInstallCallback: (value: OutOfOrderRequest) => Promise<JsCertificationCandidateCallbackResponse>): Promise<JsCertificationResponse> {
        try {
            return await this.impl.certify(makeNewRequestCallback, oooInstallCallback)
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