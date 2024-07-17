use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
};

use crate::{
    core::{ServiceResult, SystemServiceSync},
    services::MetricsService,
    SystemMessage,
};
use futures_util::future::join_all;
use log::{error, info};
use tokio::runtime::Handle;

use crate::core::{System, SystemService};

pub struct TalosCertifierServiceBuilder {
    system: System,
    certifier_service: Option<Box<dyn SystemServiceSync + Send + Sync>>,
    services: Vec<Box<dyn SystemService + Send + Sync>>,
    pub metrics_service: Option<Box<dyn SystemService + Send + Sync>>,
}

impl TalosCertifierServiceBuilder {
    pub fn new(system: System) -> Self {
        Self {
            system,
            certifier_service: None,
            metrics_service: None,
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

    pub fn add_metric_service(mut self, metrics_service: Box<dyn SystemService + Send + Sync>) -> Self {
        self.metrics_service = Some(metrics_service);
        self
    }

    pub fn add_certifier_service(mut self, certifier_service: Box<dyn SystemServiceSync + Send + Sync>) -> Self {
        self.certifier_service = Some(certifier_service);
        self
    }

    pub fn build(self) -> TalosCertifierService {
        let mut services = self.services;
        // services.push(self.certifier_service.expect("Certifier Service is mandatory"));

        if let Some(metrics) = self.metrics_service {
            services.push(metrics);
        };

        let certifier_service = self.certifier_service.expect("Certifier Service is mandatory");

        TalosCertifierService {
            system: self.system,
            services,
            certifier_service,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        }
    }
}

pub struct TalosCertifierService {
    pub system: System,
    pub services: Vec<Box<dyn SystemService + Send + Sync>>,
    pub certifier_service: Box<dyn SystemServiceSync + Send + Sync>,
    pub shutdown_flag: Arc<AtomicBool>,
}

impl TalosCertifierService {
    pub async fn run(self) -> ServiceResult {
        let shutdown_flag = Arc::clone(&self.shutdown_flag);
        let mut certifier_service = self.certifier_service;
        let certifier_handle = thread::spawn(move || {
            while !shutdown_flag.load(Ordering::Relaxed) {
                certifier_service.run();
            }
            error!("I am out of the while loop for certifier_service");
        });

        let mut service_handle = self
            .services
            .into_iter()
            .map(|mut service| {
                let rt_handle = Handle::current();
                let shutdown_notifier_cloned = self.system.system_notifier.clone();
                let mut shutdown_receiver = shutdown_notifier_cloned.subscribe();
                let shutdown_flag = Arc::clone(&self.shutdown_flag);
                thread::spawn(move || {
                    rt_handle.block_on(async move {
                        while !shutdown_flag.load(Ordering::Relaxed) {
                            // error!("Running service");
                            service.run().await.unwrap();
                        }
                        error!("I am outside the while,,,...");
                    });
                    error!("I moved BACK from tokio to thread");
                    // handle.await.expect("Tokio task panicked!!!");
                    // let handle = rt_handle.spawn({
                    //     async move {
                    //         let mut result: ServiceResult = Ok(());
                    //             tokio::select! {
                    //                 svc_result = service.run() => {
                    //                     if let Err(service_error) = svc_result {
                    //                         error!("Error found in service=({}) !!!! {:?}", service_error.service, service_error);
                    //                         shutdown_notifier_cloned.send(SystemMessage::Shutdown).unwrap();
                    //                     };
                    //                 },
                    //                 msg = shutdown_receiver.recv() => {
                    //                     let message = msg.unwrap();

                    //                     match message {
                    //                         SystemMessage::Shutdown => {
                    //                             info!("Shutdown received");
                    //                             let _ = &shutdown_flag.swap(true, Ordering::Relaxed);
                    //                         },
                    //                         SystemMessage::ShutdownWithError(service_error) => {
                    //                             info!("Shutdown received due to error");
                    //                             let _ = &shutdown_flag.swap(true, Ordering::Relaxed);
                    //                              result = Err(service_error);
                    //                         },

                    //                         _ => ()
                    //                     }

                    //                 }
                    //             }
                    //         }
                    //         result
                    //     }
                    // });

                    // handle.await;
                })
            })
            .collect::<Vec<JoinHandle<()>>>();

        service_handle.push(certifier_handle);

        for t in service_handle {
            t.join().expect("Thread panicked!!");
        }
        // let k = join_all(service_handle).await;

        // for res in k {
        //     res.unwrap()?
        // }

        Ok(())
    }
}
