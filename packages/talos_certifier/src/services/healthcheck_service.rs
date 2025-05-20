use crate::{
    core::{ServiceResult, System, SystemService},
    healthcheck::{self, HealthChecks},
    SystemMessage,
};
use std::time::Duration;

use async_trait::async_trait;
use tracing::{debug, error, info};

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
    async fn run(&mut self) -> ServiceResult {
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
