use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use super::{common::SharedPortTraits, errors::DecisionStoreError};

// The trait that should be implemented by any adapter that will read the message
// and pass to the domain.
#[async_trait]
pub trait DecisionStore: SharedPortTraits {
    type Decision: Serialize + DeserializeOwned;

    async fn get_decision(&self, key: String) -> Result<Option<Self::Decision>, DecisionStoreError>;
    async fn insert_decision(&self, key: String, decision: Self::Decision) -> Result<Self::Decision, DecisionStoreError>;
    async fn insert_decision_multi(&self, decisions: Vec<Self::Decision>) -> Result<Vec<Self::Decision>, DecisionStoreError>;
}
