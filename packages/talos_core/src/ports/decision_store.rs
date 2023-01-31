use async_trait::async_trait;
use dyn_clone::DynClone;
use serde::{de::DeserializeOwned, Serialize};

use super::{common::SharedPortTraits, errors::DecisionStoreError};

// The trait that should be implemented by any adapter that will read the message
// and pass to the domain.
#[async_trait]
pub trait DecisionStore: DynClone + SharedPortTraits {
    type Decision: Serialize + DeserializeOwned;

    async fn get_decision(&self, key: String) -> Result<Option<Self::Decision>, DecisionStoreError>;
    async fn insert_decision(&mut self, key: String, decision: Self::Decision) -> Result<Option<Self::Decision>, DecisionStoreError>;
}
