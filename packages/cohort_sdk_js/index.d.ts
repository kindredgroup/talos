/* tslint:disable */
/* eslint-disable */

/* auto-generated by NAPI-RS */

export interface JsBackoffConfig {
  minMs: number
  maxMs: number
}
export interface JsConfig {
  backoffOnConflict: JsBackoffConfig
  retryBackoff: JsBackoffConfig
  retryAttemptsMax: number
  retryOoBackoff: JsBackoffConfig
  retryOoAttemptsMax: number
  snapshotWaitTimeoutMs: number
  agent: string
  cohort: string
  bufferSize: number
  timeoutMs: number
  brokers: string
  topic: string
  saslMechanisms?: string
  kafkaUsername?: string
  kafkaPassword?: string
  agentGroupId: string
  agentFetchWaitMaxMs: number
  agentMessageTimeoutMs: number
  agentEnqueueTimeoutMs: number
  agentLogLevel: number
  dbPoolSize: number
  dbUser: string
  dbPassword: string
  dbHost: string
  dbPort: string
  dbDatabase: string
}
export interface JsCertificationRequest {
  candidate: JsCandidateData
  timeoutMs: number
}
export interface JsCandidateData {
  readset: Array<string>
  writeset: Array<string>
  statemap?: Array<Record<string, any>>
}
export interface OoRequest {
  xid: string
  safepoint: number
  newVersion: number
  attemptNr: number
}
export class Initiator {
  static init(config: JsConfig): Promise<Initiator>
  certify(jsCertificationRequest: JsCertificationRequest, getStateCallback: (err: Error | null, value: number) => any, oooCallback: (err: Error | null, value: OoRequest) => any): Promise<string>
}
