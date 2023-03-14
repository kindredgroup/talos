use async_trait::async_trait;
use log::info;
use tokio::sync::mpsc;

use crate::{
    core::{ServiceResult, System, SystemService},
    errors::{SystemErrorType, SystemServiceError},
    SystemMessage,
};

pub struct MonitorService {
    pub system: System,
    pub monitor_channel: mpsc::Receiver<SystemErrorType>,
}

impl MonitorService {
    pub fn new(monitor_channel: mpsc::Receiver<SystemErrorType>, system: System) -> Self {
        MonitorService { system, monitor_channel }
    }
}

#[async_trait]
impl SystemService for MonitorService {
    //** Initiate Shutdown
    async fn shutdown_service(&mut self) {
        self.system.is_shutdown = true;
        info!("Monitor Service shutdown completed!");
    }

    fn is_shutdown(&self) -> bool {
        self.system.is_shutdown
    }

    async fn update_shutdown_flag(&mut self, flag: bool) {
        self.system.is_shutdown = flag;
    }
    async fn health_check(&self) -> bool {
        true
    }

    async fn run(&mut self) -> ServiceResult {
        let mut system_channel_rx = self.system.system_notifier.subscribe();
        // while !self.is_shutdown() {
        let result = tokio::select! {
            // ** Monitor Channel messages
            monitor_message = self.monitor_channel.recv() => {
                match monitor_message {
                    Some(SystemErrorType::AdapterFailure(err)) => {
                        info!("Monitor Service received error on {} with reason= {:#?}!", err.adapter_name, err.reason);

                         Err(Box::new(SystemServiceError{
                            kind: crate::errors::SystemServiceErrorKind::SystemError(SystemErrorType::AdapterFailure(err.clone())),
                            data: None,
                            reason: err.reason,
                            service: "Monitor Service".to_owned()
                         }))
                    },
                    _ => Ok(()),
                }
            }
          // ** Received System Messages (shutdown/healthcheck).
          msg = system_channel_rx.recv() => {
            let message = msg.unwrap();

             match message {

              SystemMessage::Shutdown => {
                info!("[Decision Outbox Service] Shutdown received");
                self.shutdown_service().await;
              },
              SystemMessage::HealthCheck => {
                // info!("Health Check message received <3 <3 <3");
                let is_healthy = self.health_check().await;
                self.system.system_notifier.send(SystemMessage::HealthCheckStatus { service: "Decision_Outbox", healthy: is_healthy },).unwrap();
              },
              _ => ()
           }

           Ok(())

          }
        };
        // }

        return result;
    }
}
