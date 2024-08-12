import { Initiator } from "./initiator"
import { Replicator } from "./replicator"
import {
    JsCertificationCandidateCallbackResponse,
    JsCertificationRequest,
    JsCertificationResponse,
    JsDecision,
    JsInitiatorConfig,
    JsKafkaAction,
    JsKafkaConfig,
    JsReplicatorConfig,
    JsResponseMetadata,
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
    JsDecision,
    JsInitiatorConfig,
    JsCertificationCandidateCallbackResponse,
    JsCertificationResponse,
    JsCertificationRequest,
    JsKafkaAction,
    JsKafkaConfig,
    JsReplicatorConfig,
    JsResponseMetadata,
    JsStatemapAndSnapshot,
    JsOutOfOrderInstallOutcome,
    OutOfOrderRequest,
    Replicator,
    SdkErrorKind,
    TalosSdkError,
}

export class CapturedItemState{
  constructor(readonly id: string, readonly version: number){}
}

export class CapturedState{
  constructor(readonly snapshotVersion: number, readonly items: CapturedItemState[]){}
}