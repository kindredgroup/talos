use async_trait::async_trait;

#[async_trait]
pub trait SharedPortTraits {
    async fn is_healthy(&self) -> bool;
    async fn shutdown(&self) -> bool;
}
