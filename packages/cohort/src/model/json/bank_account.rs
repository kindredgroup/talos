use std::fmt;

use rusty_money::{iso, Money};
use serde::de::{MapAccess, Visitor};
use serde::{de, Deserialize, Deserializer};

use crate::model::bank_account::BankAccount;
use crate::model::talos_state::TalosState;

impl<'de> Deserialize<'de> for BankAccount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Name,
            Number,
            Amount,
            Currency,
            TalosState,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str("'name' or 'number' or 'amount' or 'currency' or 'talosState'")
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "name" => Ok(Field::Name),
                            "number" => Ok(Field::Number),
                            "amount" => Ok(Field::Amount),
                            "currency" => Ok(Field::Currency),
                            "talosState" => Ok(Field::TalosState),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct BankAccountVisitor;

        impl<'de> Visitor<'de> for BankAccountVisitor {
            type Value = BankAccount;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct BankAccount")
            }

            fn visit_map<V>(self, mut map: V) -> Result<BankAccount, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut name: Option<String> = None;
                let mut number: Option<String> = None;
                let mut amount: Option<String> = None;
                let mut currency: Option<String> = None;
                let mut talos_state: Option<TalosState> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Name => {
                            if name.is_some() {
                                return Err(de::Error::duplicate_field("name"));
                            }
                            name = Some(map.next_value()?);
                        }
                        Field::Number => {
                            if number.is_some() {
                                return Err(de::Error::duplicate_field("number"));
                            }
                            number = Some(map.next_value()?);
                        }
                        Field::Amount => {
                            if amount.is_some() {
                                return Err(de::Error::duplicate_field("amount"));
                            }
                            amount = Some(map.next_value()?);
                        }
                        Field::Currency => {
                            if currency.is_some() {
                                return Err(de::Error::duplicate_field("currency"));
                            }
                            currency = Some(map.next_value()?);
                        }
                        Field::TalosState => {
                            if talos_state.is_some() {
                                return Err(de::Error::duplicate_field("talosState"));
                            }
                            talos_state = Some(map.next_value()?);
                        }
                    }
                }
                let name = name.ok_or_else(|| de::Error::missing_field("name"))?;
                let number = number.ok_or_else(|| de::Error::missing_field("number"))?;
                let amount = amount.ok_or_else(|| de::Error::missing_field("amount"))?;
                let currency = currency.ok_or_else(|| de::Error::missing_field("currency"))?;
                let symbol = iso::find(currency.as_str());
                let talos_state = talos_state.ok_or_else(|| de::Error::missing_field("talosState"))?;
                if symbol.is_none() {
                    return Err(de::Error::unknown_variant(currency.as_str(), &["Expected ISO currency, three letters"]));
                }

                let account = BankAccount {
                    name,
                    number,
                    balance: Money::from_str(amount.as_str(), symbol.unwrap()).unwrap(),
                    talos_state,
                };

                Ok(account)
            }
        }

        const FIELDS: &[&str] = &["name", "number", "amount", "currency", "talosState"];
        deserializer.deserialize_struct("BankAccount", FIELDS, BankAccountVisitor)
    }
}
