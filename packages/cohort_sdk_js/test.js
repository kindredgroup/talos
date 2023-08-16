let {Initiator} = require('./index.js');

let config = {
  retryAttemptsMax: 2,
  retryBackoffMaxMs: 2,
  retryOoBackoffMaxMs: 2,
  retryOoAttemptsMax: 2,
  agent: "string",
  cohort: "string",
  bufferSize: 2,
  timeoutMs: 2,
  brokers: "string",
  topic: "string",
  saslMechanisms: "string",
  kafkaUsername: "string",
  kafkaPassword: "string",
  agentGroupId: "string",
  agentFetchWaitMaxMs: 2,
  agentMessageTimeoutMs: 2,
  agentEnqueueTimeoutMs: 2,
  agentLogLevel: 2,
  replicatorClientId: "string",
  replicatorGroupId: "string",
  producerConfigOverrides: {},
  consumerConfigOverrides: {},
  /** Initial capacity of the suffix */
  suffixSizeMax: 2,
  /**
   * - The suffix prune threshold from when we start checking if the suffix
   * should prune.
   * - Set to None if pruning is not required.
   * - Defaults to None.
   */
  suffixPruneAtSize: 2,
  /**
   * Minimum size of suffix after prune.
   * - Defaults to None.
   */
  suffixSizeMin: 2,
  replicatorBufferSize: 2,
  dbPoolSize: 2,
  dbUser: "string",
  dbPassword: "string",
  dbHost: "string",
  dbPort: "string",
  dbDatabase: "string"
}

function install_callback() {
console.log("install_callback");
return "22"
}
console.log(typeof Initiator, install_callback);
async function main() {
let initiator = await Initiator.init(config );
console.log("cohort init in js");
await initiator.certify({
candidate: {
    readset: ["ww"],
    writeset: ["344"],
    statemap: [{d:23}]
},
timeoutMs: 34
},
install_callback
)
}

main();
console.log("after main");


