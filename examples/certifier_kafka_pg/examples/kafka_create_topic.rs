use talos_certifier_adapters::kafka::kafka_deploy::{create_topic, KafkaDeployError, KafkaDeployStatus};

#[tokio::main]
async fn main() -> Result<(), KafkaDeployError> {
    println!("deploying kafka...");

    let status = create_topic().await?;

    match status {
        KafkaDeployStatus::TopicExists => {
            println!("Topic already exists...!!!")
        }
        KafkaDeployStatus::TopicCreated => {
            println!("Topic created successfully !!!")
        }
    }

    Ok(())
}
