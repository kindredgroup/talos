import { Initiator, JsCertificationRequest, JsConfig } from "cohort_sdk_js"

console.log("Cohort JS Application")
console.log("---------------------")

let cfg: JsConfig = {
    backoffOnConflict: {
        minMs: 2,
        maxMs: 1500,
      },
      retryBackoff: {
        minMs: 2,
        maxMs: 1500,
      },
      retryAttemptsMax: 2,
      retryOoBackoff: {
        minMs: 2,
        maxMs: 1500,
      },
      retryOoAttemptsMax: 2,
      snapshotWaitTimeoutMs: 2,
      agent: "cohort-js",
      cohort: "cohort-js",
      bufferSize: 2,
      timeoutMs: 2,
      brokers: "127.0.0.1:9092",
      topic: "dev.ksp.certification",
      saslMechanisms: null,
      kafkaUsername: null,
      kafkaPassword: null,
      agentGroupId: "cohort-js",
      agentFetchWaitMaxMs: 2,
      agentMessageTimeoutMs: 2,
      agentEnqueueTimeoutMs: 2,
      agentLogLevel: 2,
      dbPoolSize: 2,
      dbUser: "postgres",
      dbPassword: "admin",
      dbHost: "127.0.0.1",
      dbPort: "5432",
      dbDatabase: "talos-sample-cohort-dev"
}

let start = async () => {
    let initiator: Initiator = await Initiator.init(cfg)
    let request: JsCertificationRequest = {
        timeoutMs: 1000,
        candidate: {
            readset: ["1"],
            writeset: ["1"],
            statemap: [{statemap_key: "statemap_value"}]
        }
    }
    initiator.certify(request,
        (e, r) => {
            console.log("param get-state callback: " + r)
            return {
                snapshot_version: 1,
                items: [
                    { id: "1", version: 1 }
                ]
            }
        },
        (e, r) => {
            console.log(`Param oo install callback. Error: ${e}, Request: ${JSON.stringify(r, null, 2)}`)
        })
}

await start()

console.log("App: Finished.")
// Promise
//     .resolve(start())
//     .catch(e => {
//         console.log("App: Error: " + e)
//     })
//     .then(result => {
//         console.log("App: Result: " + result)
//     })
