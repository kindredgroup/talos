use thiserror::Error as ThisError;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum DecisionStoreErrorKind {
    // #[error("Error in getting decision for key={0}")]
    GetDecision,
    // #[error("Error in inserting decision payload={0}")]
    InsertDecision,
    NoRowReturned,
    // #[error("Error creating key key={0}")]
    CreateKey,
    // #[error("Error parsing ")]
    ParseError,
    // #[error("Error getting datastore client")]
    ClientError,
}

#[derive(Debug, Clone, Eq, PartialEq, ThisError)]
#[error("Decision Store Error kind={kind:?} reason={reason} data={data:?} ")]
pub struct DecisionStoreError {
    pub kind: DecisionStoreErrorKind,
    pub reason: String,
    pub data: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, ThisError)]
#[error("Message Publish Error reason={reason} data={data:?} ")]
pub struct MessagePublishError {
    pub reason: String,
    pub data: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MessageReceiverErrorKind {
    HeaderNotFound,
    IncorrectData,
    ParseError,
    ReceiveError,
    VersionZero,
    SaveVersion,
    SubscribeError,
    CommitError,
    TimedOut,
}
#[derive(Debug, Clone, Eq, PartialEq, ThisError)]
#[error("Message Receiver Error for version={version:?} with reason={reason} and data={data:?} ")]
pub struct MessageReceiverError {
    pub kind: MessageReceiverErrorKind,
    pub version: Option<u64>,
    pub reason: String,
    pub data: Option<String>,
}
