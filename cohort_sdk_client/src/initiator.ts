import { InternalInitiator, JsInitiatorConfig, OutOfOrderRequest, SdkErrorKind, JsCertificationResponse, JsCertificationCandidateCallbackResponse, JsOutOfOrderInstallOutcome } from "@kindredgroup/cohort_sdk_js"
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

    async certify(makeNewRequestCallback: () => Promise<JsCertificationCandidateCallbackResponse>, oooInstallCallback: (oooRequest: OutOfOrderRequest) => Promise<JsOutOfOrderInstallOutcome>): Promise<JsCertificationResponse> {
        try {
            // This will hide the 'error' parameter from callback (it comes from NAPI).
            const adaptedOooInstallCallback = async (error: Error | null, oooRequest: OutOfOrderRequest): Promise<JsOutOfOrderInstallOutcome> => {
                if (error) {
                    throw new TalosSdkError(
                        SdkErrorKind.Internal,
                        "Call from native code into 'oooInstallCallback' function provided by JS has failed at the NAPI layer. See the 'cause' field for more details.",
                        { cause: error }
                    )
                } else {
                    return await oooInstallCallback(oooRequest)
                }
            }

            return await this.impl.certify(makeNewRequestCallback, adaptedOooInstallCallback)
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