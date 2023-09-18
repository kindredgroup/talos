use thiserror::Error as ThisError;

pub type MessengerServiceResult = Result<(), MessengerServiceError>;

#[derive(Debug, PartialEq, Clone)]
pub enum MessengerServiceErrorKind {
    System,
}

#[derive(Debug, ThisError, PartialEq, Clone)]
#[error("error in service={service} kind={kind:?} \n reason={reason} \n data={data:?}")]
pub struct MessengerServiceError {
    pub kind: MessengerServiceErrorKind,
    pub reason: String,
    pub data: Option<String>,
    pub service: String,
}
