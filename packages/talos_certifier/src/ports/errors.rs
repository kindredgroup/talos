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
#[error("(Decision Store Error kind={kind:?} reason={reason} data={data:?} ")]
pub struct DecisionStoreError {
    pub kind: DecisionStoreErrorKind,
    pub reason: String,
    pub data: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq, ThisError)]
#[error("(Message Publish Error reason={reason} data={data:?} ")]
pub struct MessagePublishError {
    pub reason: String,
    pub data: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum MessageReceiverErrorKind {
    // #[error("Error receiving message")]
    ReceiveError,
    // #[error("Incorrect message received")]
    IncorrectData,
    // #[error("Version 0 is just a placeholder and will not be used in certification")]
    VersionZero,
    // #[error("Error parsing ")]
    ParseError,
    // #[error("Error saving version number")]
    SaveVersion,
    // #[error("Error Subscribing")]
    SubscribeError,
    // #[error("Error Commiting")]
    CommitError,
}
#[derive(Debug, Clone, Eq, PartialEq, ThisError)]
#[error("(Message Publish Error reason={reason} data={data:?} ")]
pub struct MessageReceiverError {
    pub kind: MessageReceiverErrorKind,
    pub reason: String,
    pub data: Option<String>,
}
