use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

use crate::model::bank_account::BankAccount;

impl Serialize for BankAccount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BankAccount", 5)?;
        state.serialize_field("number", &self.number)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("talosState", &self.talos_state)?;
        state.serialize_field("amount", &self.balance.amount().to_string())?;
        state.serialize_field("currency", &self.balance.currency().iso_alpha_code)?;
        state.end()
    }
}
