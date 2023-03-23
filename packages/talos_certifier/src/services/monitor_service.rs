use async_trait::async_trait;
use log::info;
use tokio::sync::mpsc;

use crate::{
    core::{ServiceResult, System, SystemMonitorMessage, SystemService},
    errors::{SystemErrorType, SystemServiceError},
};

pub struct MonitorService {
    pub system: System,
    pub monitor_channel: mpsc::Receiver<SystemMonitorMessage>,
}

impl MonitorService {
    pub fn new(monitor_channel: mpsc::Receiver<SystemMonitorMessage>, system: System) -> Self {
        MonitorService { system, monitor_channel }
    }
}

#[async_trait]
impl SystemService for MonitorService {
    async fn run(&mut self) -> ServiceResult {
        // ** Monitor Channel messages
        let monitor_message = self.monitor_channel.recv().await;
        match monitor_message {
            Some(SystemMonitorMessage::Failures(SystemErrorType::AdapterFailure(err))) => {
                info!("Monitor Service received error on {} with reason= {:#?}!", err.adapter_name, err.reason);

                Err(Box::new(SystemServiceError {
                    kind: crate::errors::SystemServiceErrorKind::SystemError(SystemErrorType::AdapterFailure(err.clone())),
                    data: None,
                    reason: err.reason,
                    service: "Monitor Service".to_owned(),
                }))
            }
            _ => Ok(()),
        }
    }
}
