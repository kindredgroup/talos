use async_trait::async_trait;
use talos_certifier::model::DecisionMessage;
use talos_certifier::ports::common::SharedPortTraits;
use talos_certifier::ports::errors::DecisionStoreError;
use talos_certifier::ports::DecisionStore;

pub struct MockDataStore {}

#[async_trait]
impl DecisionStore for MockDataStore {
    type Decision = DecisionMessage;

    async fn get_decision(&self, _key: String) -> Result<Option<Self::Decision>, DecisionStoreError> {
        Ok(None)
    }

    async fn insert_decision(&self, _key: String, decision: Self::Decision) -> Result<Self::Decision, DecisionStoreError> {
        Ok(decision)
    }
    async fn insert_decision_multi(&self, decision: Vec<Self::Decision>) -> Result<Vec<Self::Decision>, DecisionStoreError> {
        Ok(decision)
    }
}

#[async_trait]
impl SharedPortTraits for MockDataStore {
    async fn is_healthy(&self) -> bool {
        true
    }

    async fn shutdown(&self) -> bool {
        true
    }
}
