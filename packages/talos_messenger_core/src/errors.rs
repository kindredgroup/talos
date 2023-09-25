use thiserror::Error as ThisError;

pub type MessengerServiceResult = Result<(), MessengerServiceError>;

#[derive(Debug, PartialEq, Clone)]
pub enum ActionErrorKind {
    Deserialisation,
}
#[derive(Debug, ThisError, PartialEq, Clone)]
#[error("Action Error {kind:?} with reason={reason} for data={data:?}")]
pub struct ActionError {
    pub kind: ActionErrorKind,
    pub reason: String,
    pub data: String,
}

#[derive(Debug, PartialEq, Clone)]
pub enum MessengerServiceErrorKind {
    System,
    Channel,
    Messaging,
}

#[derive(Debug, ThisError, PartialEq, Clone)]
#[error("error in service={service} kind={kind:?} \n reason={reason} \n data={data:?}")]
pub struct MessengerServiceError {
    pub kind: MessengerServiceErrorKind,
    pub reason: String,
    pub data: Option<String>,
    pub service: String,
}
