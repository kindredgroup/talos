use async_trait::async_trait;
use talos_core::{errors::SystemServiceError, model::decision_message::DecisionMessage, SystemMessage};
use tokio::sync::broadcast;

pub type ServiceResult<T = ()> = Result<T, SystemServiceError>;

#[derive(Debug)]
pub enum DecisionOutboxChannelMessage {
    Decision(DecisionMessage),
    OutboundServiceError(SystemServiceError),
}

#[derive(Debug, Clone)]
pub struct System {
    pub system_notifier: broadcast::Sender<SystemMessage>,
    pub is_shutdown: bool,
}

#[async_trait]
pub trait SystemService {
    async fn shutdown_service(&mut self);
    fn is_shutdown(&self) -> bool;
    async fn update_shutdown_flag(&mut self, flag: bool);
    async fn health_check(&self) -> bool;
    async fn run(&mut self) -> Result<(), SystemServiceError>;
}

// #[async_trait]
// pub trait SystemServiceRun {
//     async fn run(&mut self) -> Result<(), SystemServiceError>;
// }

// #[async_trait]
// impl<A> SystemServiceRun for A
// where
//     A: SystemService + Send + Sync,
//     <A as SystemService>::SE: Into<SystemServiceError> + Send + Sync,
//     SystemServiceError: From<<A as SystemService>::SE> + Send + Sync,
// {
//     #[allow(clippy::needless_borrow)]
//     async fn run(&mut self) -> Result<(), SystemServiceError> {
//         A::run(&mut self).await?;
//         Ok(())
//     }
// }
