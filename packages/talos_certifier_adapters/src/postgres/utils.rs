use serde::de::DeserializeOwned;
use serde_json::Value;
use talos_certifier::ports::errors::{DecisionStoreError, DecisionStoreErrorKind};
use uuid::Uuid;

pub fn get_uuid_key(key: &str) -> Result<Uuid, DecisionStoreError> {
    Uuid::parse_str(key).map_err(|e| DecisionStoreError {
        kind: DecisionStoreErrorKind::CreateKey,
        reason: e.to_string(),
        data: Some(key.to_owned()),
    })
}

pub fn parse_json_column<T: DeserializeOwned>(key: &str, value: Value) -> Result<T, DecisionStoreError> {
    serde_json::from_value::<T>(value).map_err(|e| DecisionStoreError {
        kind: DecisionStoreErrorKind::ParseError,
        reason: e.to_string(),
        data: Some(key.to_owned()),
    })
}

#[cfg(test)]
mod util_test {
    use serde::Deserialize;
    use serde_json::Value;
    use talos_certifier::ports::errors::{DecisionStoreError, DecisionStoreErrorKind};
    use uuid::Uuid;

    use super::{get_uuid_key, parse_json_column};

    #[test]
    fn test_get_uuid_key_success() {
        let key = "87cafd93-01e2-440b-9625-e559b4831b6c";
        let uuid_key = get_uuid_key(key).unwrap();

        assert_eq!(uuid_key, Uuid::parse_str(key).unwrap());
    }
    #[test]
    fn test_get_uuid_key_error() {
        let key = "gibberish";
        let uuid_result = get_uuid_key(key);

        assert!(uuid_result.is_err());

        assert!(matches!(
            uuid_result.unwrap_err(),
            DecisionStoreError {
                kind: DecisionStoreErrorKind::CreateKey,
                ..
            }
        ))
    }

    #[test]
    fn test_parsing_jsonb_column() {
        #[derive(Deserialize)]
        struct User {
            name: String,
            age: u32,
        }

        let json_user_data = r#"
        {
            "name": "John Doe",
            "age": 43
        }"#;

        let user_data_value: Value = serde_json::from_str(json_user_data).unwrap();

        let parse_result = parse_json_column::<User>("key-a", user_data_value).unwrap();

        assert_eq!(parse_result.name, "John Doe".to_owned());
        assert_eq!(parse_result.age, 43);
    }
    #[test]
    fn test_parsing_jsonb_column_error() {
        #[derive(Debug, Deserialize)]
        struct User {
            _name: String,
            _age: u32,
        }

        let json_user_data = r#"
        {
            "incorrect_property": "Incorrect Property",
            "age": 43
        }"#;

        let user_data_value: Value = serde_json::from_str(json_user_data).unwrap();

        let parse_error = parse_json_column::<User>("key-a", user_data_value).unwrap_err();

        assert!(matches!(
            parse_error,
            DecisionStoreError {
                kind: DecisionStoreErrorKind::ParseError,
                ..
            }
        ));
    }
}
