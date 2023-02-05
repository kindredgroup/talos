use crate::{
    core::{System, SystemService},
    errors::SystemServiceError,
    healthcheck::{self, HealthChecks},
    SystemMessage,
};
use std::time::Duration;

use async_trait::async_trait;
use log::{debug, error, info};

pub struct HealthCheckService {
    pub healthcheck: HealthChecks,
    pub system: System,
}

impl HealthCheckService {
    pub fn new(system: System) -> Self {
        Self {
            system,
            healthcheck: healthcheck::HealthChecks::new(),
        }
    }
}

#[async_trait]
impl SystemService for HealthCheckService {
    //** Initiate Shutdown
    async fn shutdown_service(&mut self) {
        debug!("Healthcheck Service shutting down");
        self.system.is_shutdown = true;
        info!("Health Service shutdown completed!");
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

    async fn run(&mut self) -> Result<(), SystemServiceError> {
        const DURATION_BETWEEN_CHECKS: Duration = Duration::from_secs(10);

        let mut system_channel_rx = self.system.system_notifier.subscribe();

        // while !self.is_shutdown() {
        tokio::select! {
          //** Health Check request for status from services
          _ = tokio::time::sleep(DURATION_BETWEEN_CHECKS) => {

            if let Err(error) = self.system.system_notifier.send(SystemMessage::HealthCheck) {
              error!("[HEALTHCHECK]  Failed to send heartbeat with error ++ {} ++", error);
            } else {
              debug!("[HEALTHCHECK] heart beat <3");
            }
          },
         //** Health Check receive status from services
          rx_message =  system_channel_rx.recv() =>  {
            match rx_message {
              Ok(message) => {
                match message {
                  SystemMessage::Shutdown => {
                    info!("[HEALTHCHECK] Shut down received, exiting health check");
                    // break;
                  }
                  SystemMessage::HealthCheckStatus { service, healthy } => {
                    self.healthcheck.on_system_message(service, healthy).await;
                    if !healthy {
                      error!("[HEALTHCHECK] Service {} health status {}", service, healthy);
                      let _ = self.system.system_notifier.send(SystemMessage::Shutdown);
                    }
                  }
                  _ => (),
                }
              }
              Err(error) => {
                error!("[HEALTHCHECK] System channel receive error. {:?}", error)
              }
            }
          }
        }
        // }

        //TODO: any final clean ups before exiting
        // debug!("Exiting HealthCheck::run");

        Ok(())
    }
}
