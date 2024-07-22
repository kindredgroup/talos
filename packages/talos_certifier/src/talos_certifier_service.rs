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
use tokio::runtime::{Builder, Handle};

use crate::core::{System, SystemService};

pub struct TalosCertifierServiceBuilder {
    system: System,
    certifier_service: Option<Box<dyn SystemService + Send + Sync>>,
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

    pub fn add_certifier_service(mut self, certifier_service: Box<dyn SystemService + Send + Sync>) -> Self {
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
    pub certifier_service: Box<dyn SystemService + Send + Sync>,
    pub shutdown_flag: Arc<AtomicBool>,
}

impl TalosCertifierService {
    pub async fn run(self) -> ServiceResult {
        let shutdown_flag = Arc::clone(&self.shutdown_flag);
        let mut certifier_service = self.certifier_service;
        // let certifier_handle =
        thread::spawn(move || {
            let rt = Builder::new_multi_thread()
                .worker_threads(4)
                .thread_name("my-custom-name")
                .thread_stack_size(3 * 1024 * 1024)
                .build()
                .unwrap();

            rt.block_on(async move {
                // error!("Before the loop in certifier service v2....");
                while !shutdown_flag.load(Ordering::Relaxed) {
                    let _ = certifier_service.run().await;
                }
                // error!("After the loop in certifier service v2....");
            });
        });

        // let mut service_handle: Vec<JoinHandle<()>> = self
        //     .services
        //     .into_iter()
        //     .map(|mut service| {
        //         tokio::spawn({
        //             let shutdown_notifier_cloned = self.system.system_notifier.clone();
        //             let mut shutdown_receiver = shutdown_notifier_cloned.subscribe();
        //             let shutdown_flag = Arc::clone(&self.shutdown_flag);

        //             async move {
        //                 let mut result: ServiceResult = Ok(());
        //                 while !shutdown_flag.load(Ordering::Relaxed) {
        //                     tokio::select! {
        //                         svc_result = service.run() => {
        //                             if let Err(service_error) = svc_result {
        //                                 error!("Error found in service=({}) !!!! {:?}", service_error.service, service_error);
        //                                 shutdown_notifier_cloned.send(SystemMessage::Shutdown).unwrap();
        //                             };
        //                         },
        //                         msg = shutdown_receiver.recv() => {
        //                             let message = msg.unwrap();

        //                             match message {
        //                                 SystemMessage::Shutdown => {
        //                                     info!("Shutdown received");
        //                                     let _ = &shutdown_flag.swap(true, Ordering::Relaxed);
        //                                 },
        //                                 SystemMessage::ShutdownWithError(service_error) => {
        //                                     info!("Shutdown received due to error");
        //                                     let _ = &shutdown_flag.swap(true, Ordering::Relaxed);
        //                                      result = Err(service_error);
        //                                 },

        //                                 _ => ()
        //                             }

        //                         }
        //                     }
        //                 }
        //                 result
        //             }
        //         })
        //     })
        //     .collect::<Vec<JoinHandle<()>>>();

        // // service_handle.push(certifier_handle);

        // for t in service_handle {
        //     t.join().expect("Thread panicked!!");
        // }
        // // let k = join_all(service_handle).await;

        // // for res in k {
        // //     res.unwrap()?
        // // }

        // Ok(())

        // let service_with_tokio = thread::spawn(move ||)

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
