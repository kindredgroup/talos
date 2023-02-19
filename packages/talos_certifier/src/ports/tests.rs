use async_trait::async_trait;
use thiserror::Error;

use crate::{common::SharedPortTraits, errors::MessageReceiverError, message::MessageReciever};

#[derive(PartialEq, Debug)]
struct SuccessMessageStruct();

#[derive(PartialEq, Debug, Error)]
#[error("Message Error")]
struct MessageReceiveError();

struct TestMessageReceiverStruct {
    value: &'static str,
}

#[derive(Debug, PartialEq, thiserror::Error)]
enum TestMessageReceiverError {
    #[error("Some error")]
    GenericError(String),
}

#[async_trait]
impl MessageReciever for TestMessageReceiverStruct {
    type Message = SuccessMessageStruct;

    async fn consume_message(&mut self) -> Result<Self::Message, MessageReceiverError> {
        if self.value == "error" {
            return Err(MessageReceiverError {
                kind: crate::errors::MessageReceiverErrorKind::IncorrectData,
                reason: "Incorrect data".to_string(),
                data: Some(self.value.to_string()),
            });
        }

        Ok(SuccessMessageStruct())
    }

    async fn subscribe(&self) -> Result<(), MessageReceiverError> {
        Ok(())
    }
    async fn commit(&self, _vers: u64) -> Result<(), MessageReceiverError> {
        Ok(())
    }

    async fn unsubscribe(&self) {}
}

#[async_trait]
impl SharedPortTraits for TestMessageReceiverStruct {
    async fn is_healthy(&self) -> bool {
        true
    }
    async fn shutdown(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn consume_message_happy_path() {
    let mut test_message_rxr = TestMessageReceiverStruct { value: "happy" };
    assert_eq!(test_message_rxr.value, "happy");
    assert_eq!(test_message_rxr.consume_message().await.unwrap(), SuccessMessageStruct())
}

#[tokio::test]
async fn consume_message_throw_error() {
    let mut test_message_rxr = TestMessageReceiverStruct { value: "error" };
    assert_eq!(
        test_message_rxr.consume_message().await.unwrap_err().kind,
        crate::errors::MessageReceiverErrorKind::IncorrectData
    )
}
