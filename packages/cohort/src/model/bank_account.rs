use std::fmt;
use std::fmt::{Display, Formatter};

use rusty_money::iso::Currency;
use rusty_money::{iso, Money};

use crate::model::talos_state::TalosState;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BankAccount {
    pub name: String,
    pub number: String,
    pub balance: Money<'static, Currency>,
    pub talos_state: TalosState,
}

impl BankAccount {
    pub fn aud(name: String, number: String, balance: String, talos_state: TalosState) -> BankAccount {
        BankAccount {
            name,
            number,
            balance: Money::from_str(balance.as_str(), iso::AUD).unwrap(),
            talos_state,
        }
    }

    pub fn increment(&mut self, amount: Money<'static, Currency>) {
        self.balance += amount
    }
}

impl Display for BankAccount {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BankAccount: [name: {}, number: {}, balance: {}{} {}, talos: {}]",
            self.name,
            self.number,
            self.balance.currency().symbol,
            self.balance.amount(),
            self.balance.currency().iso_alpha_code,
            self.talos_state
        )
    }
}

pub fn as_money(amount: String, currency: &Currency) -> Result<Money<Currency>, String> {
    Money::from_str(amount.as_str(), currency).map_err(|e| format!("Cannot create Money instance from {}{}. Error: {}", amount, currency.iso_numeric_code, e))
}

// $coverage:ignore-start
#[cfg(test)]
mod tests {
    use crate::model::bank_account::{as_money, BankAccount};
    use crate::model::talos_state::TalosState;

    #[test]
    fn test_model() {
        assert_eq!(
            format!(
                "{}",
                BankAccount::aud(
                    "TestBankAccount123456".to_string(),
                    "123456".to_string(),
                    "123.45".to_string(),
                    TalosState { version: 111_u64 },
                )
            ),
            "BankAccount: [name: TestBankAccount123456, number: 123456, balance: $123.45 AUD, talos: TalosState: [version: 111]]",
        );
    }

    #[test]
    fn should_increment_amount() {
        let mut a = BankAccount::aud(
            "TestBankAccount123456".to_string(),
            "123456".to_string(),
            "123.45".to_string(),
            TalosState { version: 111_u64 },
        );
        a.increment(as_money("100".to_string(), a.balance.currency()).unwrap());
        assert_eq!(a.balance, as_money("223.45".to_string(), a.balance.currency()).unwrap())
    }
}
