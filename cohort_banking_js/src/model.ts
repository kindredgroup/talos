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