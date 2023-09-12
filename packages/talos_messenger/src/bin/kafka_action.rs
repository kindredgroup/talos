use serde::{Deserialize, Deserializer}; // 1.0.130
use serde_json; // 1.0.67

#[derive(Debug)]
enum Publish {
    Kafka(Kafka),
    SNS(String),
    Unknown(String),
}

#[derive(Debug, Deserialize)]
struct Kafka {
    topic: String,
    value: serde_json::Value,
}

#[derive(Debug, Deserialize)]
struct OnCommit {
    publish: Vec<Publish>,
}

impl<'de> Deserialize<'de> for Publish {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let json: serde_json::value::Value = serde_json::value::Value::deserialize(deserializer)?;
        let typ = json.get("_typ").expect("_typ").as_str().unwrap();

        // let id = value.get("id").expect("id").as_str().unwrap();
        // let content = value.get("content").expect("content").as_str().unwrap();
        match typ {
            "KafkaMessage" => Ok(Publish::Kafka(Kafka {
                topic: json.get("topic").expect("topic").as_str().unwrap().to_owned(),
                value: json.get("value").expect("value").clone(),
            })),
            _ => Ok(Publish::Unknown(json.to_string())),
        }
    }
}

fn main() {
    let input = serde_json::json!({
            "publish": [
                {
                    "_typ": "KafkaMessage",
                    "topic": "${env}.chimera.coupons", // ${env} is substituted by the Kafka provider
                    "value": {
                        "resources": [
                            {
                                "_typ": "Coupon",
                                "id": "ksp:coupon.1:123e4567-e89b-12d3-a456-426614174000",
                                "status": "declined",
                                // rest of the coupon's attributes
                                "bets": []
                            },
                            {
                                "_typ": "CompoundBet",
                                "id": "ksp:bet.1:123e4567-e89b-12d3-a456-426614174000:0",
                                "status": "discarded",
                                // rest of the bet's attributes
                            }
                        ],
                        "actions": {
                            "_typ": "CouponDeclined",
                            "coupon": "ksp:coupon.1:123e4567-e89b-12d3-a456-426614174000"
                        }
                    }
                }
            ]
        }
    );
    let data: OnCommit = serde_json::from_value(input).unwrap();
    println!("{:?}", data);
}
