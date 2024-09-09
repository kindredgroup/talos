use ahash::AHashMap;

use crate::core::MessageVariant;

use super::{DecisionMessage, DecisionMessageTrait};

#[derive(Debug, Default, Clone)]
pub struct DecisionMetaHeaders {
    message_type: String,
    message_encoding: String,
    producer: String,
    major_version: u64,
}

impl DecisionMetaHeaders {
    pub fn new(major_version: u64, producer: String, message_encoding: Option<String>) -> Self {
        Self {
            message_type: MessageVariant::Decision.to_string(),
            message_encoding: message_encoding.unwrap_or("application/json".to_string()),
            major_version,
            producer,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DecisionCertHeaders {
    cert_xid: String,
    cert_version: u64,
    cert_safepoint: Option<u64>,
    cert_time: Option<String>,
    cert_agent: String,
}

impl DecisionCertHeaders {
    pub fn new(decision_message: &DecisionMessage) -> Self {
        Self {
            cert_xid: decision_message.xid.to_string(),
            cert_agent: decision_message.agent.to_string(),
            cert_time: decision_message.time.clone(),
            cert_safepoint: decision_message.safepoint,
            cert_version: decision_message.get_candidate_version(),
        }
    }
}

// region: states
#[derive(Debug, Default, Clone)]
pub struct NoMetaHeaders;

#[derive(Debug, Default, Clone)]
pub struct MetaHeaders(DecisionMetaHeaders);

#[derive(Debug, Default, Clone)]
pub struct NoCertHeaders;

#[derive(Debug, Default, Clone)]
pub struct CertHeaders(DecisionCertHeaders);

// endregion: states

#[derive(Debug, Default, Clone)]
pub struct DecisionHeaderBuilder<V, C> {
    pub meta_headers: V,
    pub cert_headers: C,
    pub additional_headers: Option<AHashMap<String, String>>,
}

impl DecisionHeaderBuilder<NoMetaHeaders, NoCertHeaders> {
    pub fn new() -> Self {
        Self {
            ..DecisionHeaderBuilder::default()
        }
    }
    pub fn with_additional_headers(additional_headers: AHashMap<String, String>) -> Self {
        Self {
            additional_headers: Some(additional_headers),
            ..DecisionHeaderBuilder::new()
        }
    }
}

impl<C> DecisionHeaderBuilder<NoMetaHeaders, C> {
    pub fn add_meta_headers(self, meta_headers: DecisionMetaHeaders) -> DecisionHeaderBuilder<MetaHeaders, C> {
        DecisionHeaderBuilder {
            meta_headers: MetaHeaders(meta_headers),
            cert_headers: self.cert_headers,
            additional_headers: self.additional_headers,
        }
    }
}

impl<V> DecisionHeaderBuilder<V, NoCertHeaders> {
    pub fn add_cert_headers(self, cert_headers: DecisionCertHeaders) -> DecisionHeaderBuilder<V, CertHeaders> {
        DecisionHeaderBuilder {
            cert_headers: CertHeaders(cert_headers),
            meta_headers: self.meta_headers,
            additional_headers: self.additional_headers,
        }
    }
}

impl DecisionHeaderBuilder<MetaHeaders, CertHeaders> {
    pub fn build(self) -> AHashMap<String, String> {
        let cert_headers = self.cert_headers.0;
        let meta_headers = self.meta_headers.0;

        let mut headers = AHashMap::new();

        // candidate headers carried over
        if let Some(candidate_headers) = self.additional_headers {
            headers.extend(candidate_headers);
        }

        // meta headers
        headers.insert("majorVersion".to_owned(), meta_headers.major_version.to_string());
        headers.insert("messageType".to_owned(), meta_headers.message_type);
        headers.insert("messageEncoding".to_owned(), meta_headers.message_encoding);
        headers.insert("producer".to_owned(), meta_headers.producer);

        // certifier specific headers
        headers.insert("certVersion".to_owned(), cert_headers.cert_version.to_string());
        headers.insert("certXid".to_owned(), cert_headers.cert_xid);
        headers.insert("certAgent".to_owned(), cert_headers.cert_agent);

        if let Some(cert_time) = cert_headers.cert_time {
            headers.insert("certTime".to_owned(), cert_time);
        }
        if let Some(cert_safepoint) = cert_headers.cert_safepoint {
            headers.insert("certSafepoint".to_owned(), cert_safepoint.to_string());
        }

        headers
    }
}

#[cfg(test)]
mod tests {

    use ahash::AHashMap;

    use crate::model::decision_headers::{DecisionCertHeaders, DecisionHeaderBuilder, DecisionMetaHeaders};

    #[test]
    fn test_decision_header_with_message_encoding_field_default() {
        let decision_meta_headers = DecisionMetaHeaders::new(1_u64, "test_producer".to_string(), None);
        let decision_cert_headers = DecisionCertHeaders {
            cert_xid: "abcd".to_string(),
            cert_version: 100,
            cert_safepoint: Some(29),
            cert_time: Some("2024-10-20.12:32:31.12323Z".to_owned()),
            cert_agent: "some-agent".to_owned(),
        };

        let decision_headers = DecisionHeaderBuilder::new()
            .add_meta_headers(decision_meta_headers.clone())
            .add_cert_headers(decision_cert_headers.clone())
            .build();

        assert_eq!(
            decision_headers.get("certXid").unwrap().to_owned(),
            decision_cert_headers.cert_xid,
            "certXid does not match"
        );
        assert_eq!(
            decision_headers.get("majorVersion").unwrap().to_owned(),
            decision_meta_headers.major_version.to_string(),
            "majorVersion doesn't match"
        );

        // test encoding is the default value of "application/json"
        assert_eq!(
            decision_headers.get("messageEncoding").unwrap().to_owned(),
            "application/json".to_string(),
            "messageEncoding does not match default application/json"
        );

        assert_eq!(
            decision_headers.get("messageEncoding").unwrap().to_owned(),
            decision_meta_headers.message_encoding,
            "messageEncoding doesn't match"
        );
    }
    #[test]
    fn test_decision_header_with_message_encoding_field_custom() {
        let decision_meta_headers = DecisionMetaHeaders::new(1_u64, "test_producer".to_string(), Some("another_encoding".to_owned()));
        let decision_cert_headers = DecisionCertHeaders {
            cert_xid: "abcd".to_string(),
            cert_version: 100,
            cert_safepoint: Some(29),
            cert_time: Some("2024-10-20.12:32:31.12323Z".to_owned()),
            cert_agent: "some-agent".to_owned(),
        };

        let decision_headers = DecisionHeaderBuilder::new()
            .add_meta_headers(decision_meta_headers.clone())
            .add_cert_headers(decision_cert_headers.clone())
            .build();

        // test encoding is not the default value of "application/json"
        assert_ne!(
            decision_headers.get("messageEncoding").unwrap().to_owned(),
            "application/json".to_string(),
            "messageEncoding must not match default application/json"
        );

        assert_eq!(
            decision_headers.get("messageEncoding").unwrap().to_owned(),
            decision_meta_headers.message_encoding,
            "messageEncoding doesn't match"
        );
    }

    #[test]
    fn test_decision_header_with_additional_headers() {
        let decision_meta_headers = DecisionMetaHeaders::new(1_u64, "test_producer".to_string(), None);
        let decision_cert_headers = DecisionCertHeaders {
            cert_xid: "abcd".to_string(),
            cert_version: 100,
            cert_safepoint: Some(29),
            cert_time: Some("2024-10-20.12:32:31.12323Z".to_owned()),
            cert_agent: "some-agent".to_owned(),
        };

        let mut additiona_headers = AHashMap::new();
        additiona_headers.insert("test-header-1".to_owned(), "test-header-1-value".to_owned());
        additiona_headers.insert("correlationId".to_owned(), "eb10b6e1-a7cf-4b44-94da-6cd007030d81".to_owned());

        let decision_headers = DecisionHeaderBuilder::with_additional_headers(additiona_headers.clone())
            .add_meta_headers(decision_meta_headers.clone())
            .add_cert_headers(decision_cert_headers.clone())
            .build();

        assert_eq!(
            decision_headers.get("correlationId").unwrap(),
            additiona_headers.get("correlationId").unwrap(),
            "correlationId must match"
        );
    }
}
