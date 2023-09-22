use futures_util::future::try_join_all;

use crate::{
    core::MessengerSystemService,
    errors::{MessengerServiceError, MessengerServiceErrorKind, MessengerServiceResult},
};

pub struct TalosMessengerService {
    // pub system: System,
    pub services: Vec<Box<dyn MessengerSystemService + Send + Sync>>,
}

impl TalosMessengerService {
    pub async fn run(self) -> MessengerServiceResult {
        let service_handle = self.services.into_iter().map(|mut service| tokio::spawn(async move { service.run().await }));

        let k = try_join_all(service_handle).await.map_err(|e| MessengerServiceError {
            kind: MessengerServiceErrorKind::System,
            reason: e.to_string(),
            data: None,
            service: "Main thread".to_string(),
        })?;

        for res in k {
            res?
        }

        Ok(())
    }

    pub async fn shutdown(self) -> MessengerServiceResult {
        let service_handle = self.services.into_iter().map(|service| tokio::spawn(async move { service.stop().await }));

        let k = try_join_all(service_handle).await.map_err(|e| MessengerServiceError {
            kind: MessengerServiceErrorKind::System,
            reason: e.to_string(),
            data: None,
            service: "Main thread".to_string(),
        })?;

        for res in k {
            res?
        }

        Ok(())
    }
}
