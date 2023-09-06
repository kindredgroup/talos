export class TransferRequest {
    constructor(public from: string, public to: string, public amount: number) {}
}

export class TransferRequestMessage {
    constructor(public request: TransferRequest, public postedAtMs: number) {}
}

export class CapturedItemState {
    constructor(public id: string, public version: number) {}
}

export class CapturedState {
    constructor(public snapshotVersion: number, public items: CapturedItemState[]) {}
}

export class CertificationCandidate {
    constructor(public readset: string[], public writeset: string[], public readvers: number[], public statemaps: [Map<string, any>] | null) {}
}

export class CertificationRequest {
    constructor(public candidate: CertificationCandidate, public snapshot: number, public timeoutMs: number) {}
}
