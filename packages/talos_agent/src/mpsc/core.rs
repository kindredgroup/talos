use async_trait::async_trait;

use tokio::sync::mpsc::error::SendError;

#[async_trait]
pub trait Sender: Send + Sync {
    type Data: Send + Clone;

    async fn send(&self, value: Self::Data) -> Result<(), SendError<Self::Data>>;
}

#[async_trait]
pub trait Receiver: Send {
    type Data: Send;
    async fn recv(&mut self) -> Option<Self::Data>;
}

#[derive(Clone)]
pub struct SenderWrapper<T: Send + Sync + Clone> {
    pub tx: tokio::sync::mpsc::Sender<T>,
}

#[async_trait]
impl<T: Send + Sync + Clone> Sender for SenderWrapper<T> {
    type Data = T;
    async fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.tx.send(value).await
    }
}

pub struct ReceiverWrapper<T: Send> {
    pub rx: tokio::sync::mpsc::Receiver<T>,
}

#[async_trait]
impl<T: Send> Receiver for ReceiverWrapper<T> {
    type Data = T;
    async fn recv(&mut self) -> Option<T> {
        self.rx.recv().await
    }
}
