use futures::executor;
///
/// The sample usage of talos agent library
///
use talos_agent::api::{AgentConfig, CandidateData, CertificationRequest, KafkaConfig, TalosAgentBuilder};

fn main() {
    let cohort = "HostForTesting";
    let cfg_agent = AgentConfig {
        agent_name: format!("agent-for-{}", cohort),
        cohort_name: cohort.to_string(),
    };

    let cfg_kafka = KafkaConfig {
        brokers: "localhost:9093".to_string(),
        enqueue_timout_ms: 10,
        message_timeout_ms: 5000,
        certification_topic: "dev.ksp.certification".to_string(),
    };

    let agent = TalosAgentBuilder::new(cfg_agent).with_kafka(cfg_kafka).build();

    let xid = uuid::Uuid::new_v4();
    let tx_data = CandidateData {
        xid: xid.to_string(),
        readset: Vec::from(["bet-a:1".to_owned(), "bet-a:2".to_owned(), "bet-a:3".to_owned()]),
        readvers: Vec::from([1, 2, 3]),
        snapshot: 5,
        writeset: Vec::from([3]),
    };

    let request = CertificationRequest {
        message_key: "12345".to_string(),
        candidate: tx_data,
    };

    let error = format!("Unable to certify transaction {xid}");
    let rsp = executor::block_on(agent.certify(request)).unwrap_or_else(|_| panic!("{}", error));
    println!("Transaction has been certified. Details: {:?}", rsp)
}
