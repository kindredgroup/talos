use std::sync::Arc;

use crate::{
    bank_api::BankApi,
    model::requests::{AccountUpdateRequest, BusinessActionType, TransferRequest},
    replicator::core::StatemapItem,
    state::postgres::{data_access::PostgresApi, database::Database},
    tx_batch_executor::BatchExecutor,
};

pub struct StaticBatchExecutor {}

impl StaticBatchExecutor {
    pub async fn execute(&self, database: Arc<Database>) -> Result<(), String> {
        let accounts = BankApi::get_accounts(Arc::clone(&database)).await?;
        let account1 = accounts.iter().find(|a| a.number == "00001").unwrap();
        let account2 = accounts.iter().find(|a| a.number == "00002").unwrap();

        let version = account1.talos_state.version + 1;

        let batch: Vec<StatemapItem> = vec![
            StatemapItem::new(
                BusinessActionType::WITHDRAW.to_string(),
                version,
                AccountUpdateRequest::new("00001".to_string(), account1.balance.amount().to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::WITHDRAW.to_string(),
                version,
                AccountUpdateRequest::new("00002".to_string(), account2.balance.amount().to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::DEPOSIT.to_string(),
                version,
                AccountUpdateRequest::new("00001".to_string(), "50".to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::DEPOSIT.to_string(),
                version,
                AccountUpdateRequest::new("00002".to_string(), "50".to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::TRANSFER.to_string(),
                version,
                TransferRequest::new("00001".to_string(), "00002".to_string(), "10.51".to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::TRANSFER.to_string(),
                version,
                TransferRequest::new("00001".to_string(), "00002".to_string(), "0.49".to_string()).json(),
            ),
            StatemapItem::new(
                BusinessActionType::TRANSFER.to_string(),
                version,
                TransferRequest::new("00002".to_string(), "00001".to_string(), "1.01".to_string()).json(),
            ),
        ];

        let mut manual_tx_api = PostgresApi { client: database.get().await };

        match BatchExecutor::execute(&mut manual_tx_api, batch, Some(version)).await {
            Ok(affected_rows) => {
                log::info!("Successfully completed batch of transactions! Updated: {} rows", affected_rows);
                let accounts = BankApi::get_accounts(Arc::clone(&database)).await?;
                log::info!("New state of bank accounts is");
                for a in accounts.iter() {
                    log::info!("{}", a);
                }

                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}
