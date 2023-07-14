use std::fmt;
use std::fmt::{Display, Formatter};

use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct BankAccount {
    pub name: String,
    pub number: String,
    #[serde(with = "rust_decimal::serde::float")]
    pub balance: Decimal,
    pub version: u64,
}

impl BankAccount {
    pub fn new(name: String, number: String, balance: f32, version: u64) -> Self {
        BankAccount {
            name,
            number,
            balance: Decimal::from_f32(balance).unwrap(),
            version,
        }
    }

    pub fn increment(&mut self, amount: f32) {
        self.balance += Decimal::from_f32(amount).unwrap()
    }
}

impl Display for BankAccount {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BankAccount: [name: {}, number: {}, balance: {}, version: {}]",
            self.name, self.number, self.balance, self.version
        )
    }
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::model::bank_account::BankAccount;

    #[test]
    fn test_model() {
        assert_eq!(
            format!(
                "{}",
                BankAccount::new("TestBankAccount123456".to_string(), "123456".to_string(), 123.45, 111_u64,)
            ),
            "BankAccount: [name: TestBankAccount123456, number: 123456, balance: 123.45, version: 111]",
        );
    }

    #[test]
    fn should_increment_amount() {
        let mut a = BankAccount::new("TestBankAccount123456".to_string(), "123456".to_string(), 123.45, 111_u64);
        a.increment(100.0);
        assert_eq!(a.balance, Decimal::from_str_exact("223.45").unwrap());
    }
}
