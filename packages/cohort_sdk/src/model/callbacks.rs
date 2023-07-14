use cohort::replicator::core::StatemapItem;
use futures::future::BoxFuture;

pub struct CapturedState {
    pub snapshot_version: u64,
    pub items: Vec<CapturedItemState>,
}

pub struct CapturedItemState {
    pub id: String,
    pub version: u64,
}

pub type GetStateFunction = Box<dyn Fn() -> BoxFuture<'static, CapturedState> + Sync + Send>;

// statemap: Vec<StatemapItem>, snapshot_version: u64
pub type InstallerFunction = Box<dyn Fn(Vec<StatemapItem>, u64) -> BoxFuture<'static, String> + Sync + Send>;

// xid: String, safepoint: u64, attempt: u64
pub type OutOfOrderInstallerFunction = Box<dyn Fn(String, u64, u64) -> BoxFuture<'static, String> + Sync + Send>;
