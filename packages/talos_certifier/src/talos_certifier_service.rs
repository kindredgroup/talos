use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use crate::{core::ServiceResult, SystemMessage};
use futures_util::future::join_all;
use log::{error, info};

use crate::core::{System, SystemService};

pub struct TalosCertifierServiceBuilder {
    system: System,
    certifier_service: Option<Box<dyn SystemService + Send + Sync>>,
    services: Vec<Box<dyn SystemService + Send + Sync>>,
}

impl TalosCertifierServiceBuilder {
    pub fn new(system: System) -> Self {
        Self {
            system,
            certifier_service: None,
            services: vec![],
        }
    }

    pub fn add_adapter_service(mut self, service: Box<dyn SystemService + Send + Sync>) -> Self {
        self.services.push(service);
        self
    }

    pub fn add_health_check_service(mut self, hc_service: Box<dyn SystemService + Send + Sync>) -> Self {
        self.services.push(hc_service);
        self
    }

    pub fn add_certifier_service(mut self, certifier_service: Box<dyn SystemService + Send + Sync>) -> Self {
        self.certifier_service = Some(certifier_service);
        self
    }

    pub fn build(self) -> TalosCertifierService {
        let mut services = self.services;
        services.push(self.certifier_service.expect("Certifier Service is mandatory"));

        TalosCertifierService {
            system: self.system,
            services,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub struct TalosCertifierService {
    pub system: System,
    pub services: Vec<Box<dyn SystemService + Send + Sync>>,
    pub shutdown_flag: Arc<AtomicBool>,
}

impl TalosCertifierService {
    pub async fn run(self) -> ServiceResult {
        let service_handle = self.services.into_iter().map(|mut service| {
            tokio::spawn({
                let shutdown_notifier_cloned = self.system.system_notifier.clone();
                let mut shutdown_receiver = shutdown_notifier_cloned.subscribe();
                let shutdown_flag = Arc::clone(&self.shutdown_flag);

                async move {
                    let mut result: ServiceResult = Ok(());
                    while !shutdown_flag.load(Ordering::Relaxed) {
                        tokio::select! {
                            svc_result = service.run() => {
                                if let Err(service_error) = svc_result {
                                    error!("Error found in service=({}) !!!! {:?}", service_error.service, service_error);
                                    shutdown_notifier_cloned.send(SystemMessage::Shutdown).unwrap();
                                };
                            },
                            msg = shutdown_receiver.recv() => {
                                let message = msg.unwrap();

                                match message {
                                    SystemMessage::Shutdown => {
                                        info!("Shutdown received");
                                        let _ = &shutdown_flag.swap(true, Ordering::Relaxed);
                                    },
                                    SystemMessage::ShutdownWithError(service_error) => {
                                        info!("Shutdown received due to error");
                                        let _ = &shutdown_flag.swap(true, Ordering::Relaxed);
                                         result = Err(service_error);
                                    },

                                    _ => ()
                                }

                            }
                        }
                    }
                    result
                }
            })
        });

        let k = join_all(service_handle).await;

        for res in k {
            res.unwrap()?
        }

        Ok(())
    }
}
