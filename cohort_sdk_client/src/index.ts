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
    JsKafkaConfig,
    JsReplicatorConfig,
    JsStatemapAndSnapshot,
    JsOutOfOrderInstallOutcome,
    OutOfOrderRequest,
    Replicator,
    SdkErrorKind,
    TalosSdkError,
}