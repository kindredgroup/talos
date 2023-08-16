let {Initiator} = require('./index.js');

let config = {
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

function install_callback() {
  console.log("install_callback");
  return "22"
}

console.log(typeof Initiator, install_callback);

async function main() {
  let initiator = await Initiator.init(config);
  console.log("cohort init in js");
  await initiator.certify({
    candidate: {
      readset: ["ww"],
      writeset: ["344"],
      statemap: [{d:23}]
    },
    timeoutMs: 34
  },
  install_callback,
  () => {
    console.log("get state callback")
  })
}

main();
console.log("after main");


