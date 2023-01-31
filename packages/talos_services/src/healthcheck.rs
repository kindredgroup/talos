use log::error;
use std::collections::HashMap;
use std::path::PathBuf;
use std::{fs, thread};
use tokio::fs::{create_dir_all, OpenOptions};
use tokio::io::AsyncWriteExt;

//TODO it may be better to use std::env::temp_dir, although it cannot be a const then
const HEALTH_CHECK_DIR: &str = "/tmp/healthchecks";

pub struct HealthChecks {
    //TODO I think out_dir may be superfluous. It gets used only for create_dir_all, but we can make it the responsibility of the
    //caller to ensure that the dir (and its ancestors) exist.
    out_dir: PathBuf,
    out_file: PathBuf,
    status: bool,
    health_statuses: HashMap<&'static str, bool>,
    // sender: broadcast::Sender<SystemMessage>,
}

impl Drop for HealthChecks {
    fn drop(&mut self) {
        // clean up after ourselves...
        // do not try to remove the file if the thread is already panicking, lest we panic ourselves
        if !thread::panicking() {
            let _ = fs::remove_file(&self.out_file);
        }
    }
}

impl HealthChecks {
    pub fn new() -> Self {
        let out_dir = PathBuf::from(HEALTH_CHECK_DIR);
        let out_file = out_dir.join("status");

        Self {
            out_dir,
            out_file,
            status: true,
            health_statuses: HashMap::new(),
        }
    }

    async fn flush(&mut self) -> std::io::Result<()> {
        create_dir_all(&self.out_dir).await?;
        let mut file = OpenOptions::new().write(true).create(true).open(&self.out_file).await?;
        let status_byte = if self.status { '1' } else { '0' };
        file.write_all(format!("{}", status_byte).as_bytes()).await?;
        file.sync_all().await?;
        Ok(())
    }

    pub async fn on_system_message(&mut self, service: &'static str, is_healthy: bool) {
        self.health_statuses.insert(service, is_healthy);
        self.status = self.health_statuses.values().all(|v| *v);
        // info!("Healthcheck received Service:{}, Healthy:{}", service, is_healthy);
        // write health checks to disk
        if let Err(e) = self.flush().await {
            error!("Failed to write health check {}", e)
        }
    }
}

impl Default for HealthChecks {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;
    use tokio::io::AsyncReadExt;

    //TODO There was a race condition where concurrently running tests were creating a HealthChecks struct for the same temporary file.
    // (Because the tests were run at exactly the same time -- they got the same timestamp.)
    fn create_healthchecks(suffix: &str) -> HealthChecks {
        //TODO changed out_dir to be temp, and the out_file to be the timestamped file. This way, when we clean up, there are no files left.
        let out_dir = std::env::temp_dir();
        let out_file = std::env::temp_dir().join(format!(
            "hc-{}-{}-status",
            SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos(),
            suffix
        ));

        // let out_dir = std::env::temp_dir().join(format!("hc-{}-{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos(), suffix));
        // let out_file = out_dir.join("status");

        HealthChecks {
            out_dir,
            out_file,
            status: true,
            health_statuses: HashMap::new(),
        }
    }

    #[tokio::test]
    async fn test_flush_to_disk() {
        let mut h = create_healthchecks("test_flush_to_disk");
        // writes healthy
        h.flush().await.unwrap();
        let mut out = String::new();
        let _file = OpenOptions::new().read(true).open(&h.out_file).await.unwrap().read_to_string(&mut out).await;
        assert_eq!(out, "1");

        // writes unhealthy
        h.status = false;
        h.flush().await.unwrap();
        let mut out = String::new();
        let _file = OpenOptions::new().read(true).open(&h.out_file).await.unwrap().read_to_string(&mut out).await;
        assert_eq!(out, "0");
    }

    #[tokio::test]
    async fn test_receives_healthcheck_status() {
        let mut h = create_healthchecks("test_receives_healthcheck_status");
        h.on_system_message("one", true).await;

        assert!(h.health_statuses.get("one").unwrap());

        // writes healthy
        h.flush().await.unwrap();
        let mut out = String::new();
        let _file = OpenOptions::new().read(true).open(&h.out_file).await.unwrap().read_to_string(&mut out).await;
        assert_eq!(out, "1");
    }
}
