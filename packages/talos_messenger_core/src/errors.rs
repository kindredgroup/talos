use thiserror::Error as ThisError;

pub type MessengerServiceResult<T = ()> = Result<T, MessengerServiceError>;

#[derive(Debug, PartialEq, Clone)]
pub enum MessengerActionErrorKind {
    Deserialisation,
    Publishing,
}
#[derive(Debug, ThisError, PartialEq, Clone)]
#[error("Messenger action error {kind:?} with reason={reason} for data={data:?}")]
pub struct MessengerActionError {
    pub kind: MessengerActionErrorKind,
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
