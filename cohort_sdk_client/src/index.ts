import { Initiator } from "./initiator"
import { Replicator } from "./replicator"
import {
    JsCertificationRequestPayload,
    JsInitiatorConfig,
    JsKafkaConfig,
    JsReplicatorConfig,
    JsStatemapAndSnapshot,
    JsOutOfOrderInstallOutcome,
    OutOfOrderRequest,
    SdkErrorKind,
 } from "@kindredgroup/cohort_sdk_js"

class TalosSdkError extends Error {
    constructor(readonly kind: SdkErrorKind, readonly message: string, options?: ErrorOptions) {
        super(message, options)
        this.name = "TalosSdkError"
    }
}

export {
    Initiator,
    JsInitiatorConfig,
    JsCertificationRequestPayload,
    JsKafkaConfig,
    JsReplicatorConfig,
    JsStatemapAndSnapshot,
    JsOutOfOrderInstallOutcome,
    OutOfOrderRequest,
    Replicator,
    SdkErrorKind,
    TalosSdkError,
}