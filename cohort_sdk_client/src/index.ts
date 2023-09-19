import { Initiator } from "./initiator"
import {
    JsCertificationRequestPayload,
    JsInitiatorConfig,
    JsOutOfOrderInstallOutcome,
    OutOfOrderRequest,
    SdkErrorKind,
 } from "cohort_sdk_js"

class TalosSdkError extends Error {
    constructor(readonly kind: SdkErrorKind, readonly message: string, options?: ErrorOptions) {
        super(message, options)
    }
}

export {
    Initiator,
    JsInitiatorConfig,
    JsCertificationRequestPayload,
    JsOutOfOrderInstallOutcome,
    OutOfOrderRequest,
    SdkErrorKind,
    TalosSdkError,
}