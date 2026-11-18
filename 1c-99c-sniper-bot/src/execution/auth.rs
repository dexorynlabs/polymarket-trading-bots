//! L2 API authentication for Polymarket CLOB.
//!
//! Audit delta #4: every authenticated CLOB call needs five headers
//! (`POLY_ADDRESS`, `POLY_SIGNATURE`, `POLY_TIMESTAMP`, `POLY_API_KEY`,
//! `POLY_PASSPHRASE`). Without them, every request returns 401.
//!
//! IMPORTANT: Polymarket's CLOB uses URL-safe base64 (alphabet `-_`,
//! not `+/`) for both decoding the api.secret and encoding the
//! HMAC-SHA256 signature. Mismatching the alphabet produces 401s on
//! ~95% of authenticated calls (any output containing index 62 or 63).
//! Reference: py-clob-client/signing/hmac.py:14-20.

use anyhow::{Context, Result};
use base64::{engine::general_purpose::URL_SAFE, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
pub struct ApiCreds {
    pub key: String,
    pub secret: String, // base64-encoded HMAC key
    pub passphrase: String,
    pub address: String, // proxy_wallet
}

/// Build the 5 headers required for an authenticated CLOB request.
///
/// `method` must be uppercase (e.g. "POST", "DELETE"). `path` must
/// include the leading `/` (e.g. "/order"). `body` is the serialised
/// request body or an empty string for GET / DELETE.
///
/// `body` is the serialised request body or an empty string for
/// GET/DELETE. The body must be a JSON string with double-quote keys
/// (the format `serde_json::to_string` produces by default); single-
/// quote bodies are non-conformant and the server will reject them.
pub fn auth_headers(
    creds: &ApiCreds,
    method: &str,
    path: &str,
    body: &str,
) -> Result<HashMap<String, String>> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before unix epoch")?
        .as_secs()
        .to_string();
    auth_headers_at(creds, method, path, body, &ts)
}

/// Internal — accepts an explicit timestamp so unit tests are
/// deterministic.
pub fn auth_headers_at(
    creds: &ApiCreds,
    method: &str,
    path: &str,
    body: &str,
    timestamp: &str,
) -> Result<HashMap<String, String>> {
    let secret_bytes = URL_SAFE
        .decode(&creds.secret)
        .context("base64-decode api.secret")?;
    let mut mac = HmacSha256::new_from_slice(&secret_bytes).context("hmac key length")?;
    let msg = format!("{timestamp}{method}{path}{body}");
    mac.update(msg.as_bytes());
    let sig = URL_SAFE.encode(mac.finalize().into_bytes());

    let mut h = HashMap::with_capacity(5);
    h.insert("POLY_ADDRESS".into(), creds.address.clone());
    h.insert("POLY_SIGNATURE".into(), sig);
    h.insert("POLY_TIMESTAMP".into(), timestamp.to_string());
    h.insert("POLY_API_KEY".into(), creds.key.clone());
    h.insert("POLY_PASSPHRASE".into(), creds.passphrase.clone());
    Ok(h)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_creds() -> ApiCreds {
        ApiCreds {
            key: "test-key".into(),
            // base64("test-secret")
            secret: "dGVzdC1zZWNyZXQ=".into(),
            passphrase: "test-pass".into(),
            address: "0x365f0ca36ae1f641e02fe3b7743673da42a13a70".into(),
        }
    }

    #[test]
    fn auth_headers_includes_all_five_required() {
        let h = auth_headers_at(&fixed_creds(), "POST", "/order", "", "1700000000").unwrap();
        assert!(h.contains_key("POLY_ADDRESS"));
        assert!(h.contains_key("POLY_SIGNATURE"));
        assert!(h.contains_key("POLY_TIMESTAMP"));
        assert!(h.contains_key("POLY_API_KEY"));
        assert!(h.contains_key("POLY_PASSPHRASE"));
        assert_eq!(h["POLY_TIMESTAMP"], "1700000000");
        assert_eq!(h["POLY_API_KEY"], "test-key");
        assert_eq!(h["POLY_PASSPHRASE"], "test-pass");
        assert_eq!(
            h["POLY_ADDRESS"],
            "0x365f0ca36ae1f641e02fe3b7743673da42a13a70"
        );
    }

    #[test]
    fn auth_signature_is_deterministic_for_fixed_inputs() {
        let s1 = auth_headers_at(&fixed_creds(), "GET", "/portfolio", "", "1700000000").unwrap()
            ["POLY_SIGNATURE"]
            .clone();
        let s2 = auth_headers_at(&fixed_creds(), "GET", "/portfolio", "", "1700000000").unwrap()
            ["POLY_SIGNATURE"]
            .clone();
        assert_eq!(s1, s2);
        assert!(!s1.is_empty());
    }

    #[test]
    fn auth_signature_changes_with_path() {
        let a = auth_headers_at(&fixed_creds(), "GET", "/portfolio", "", "1700000000").unwrap()
            ["POLY_SIGNATURE"]
            .clone();
        let b = auth_headers_at(&fixed_creds(), "GET", "/order", "", "1700000000").unwrap()
            ["POLY_SIGNATURE"]
            .clone();
        assert_ne!(a, b);
    }

    #[test]
    fn auth_signature_changes_with_method() {
        let a = auth_headers_at(&fixed_creds(), "GET", "/order", "", "1700000000").unwrap()
            ["POLY_SIGNATURE"]
            .clone();
        let b = auth_headers_at(&fixed_creds(), "DELETE", "/order", "", "1700000000").unwrap()
            ["POLY_SIGNATURE"]
            .clone();
        assert_ne!(a, b);
    }

    #[test]
    fn auth_signature_changes_with_body() {
        let a = auth_headers_at(&fixed_creds(), "POST", "/order", "{}", "1700000000").unwrap()
            ["POLY_SIGNATURE"]
            .clone();
        let b = auth_headers_at(&fixed_creds(), "POST", "/order", "{\"x\":1}", "1700000000")
            .unwrap()["POLY_SIGNATURE"]
            .clone();
        assert_ne!(a, b);
    }

    #[test]
    fn auth_signature_changes_with_timestamp() {
        let a = auth_headers_at(&fixed_creds(), "GET", "/order", "", "1700000000").unwrap()
            ["POLY_SIGNATURE"]
            .clone();
        let b = auth_headers_at(&fixed_creds(), "GET", "/order", "", "1700000001").unwrap()
            ["POLY_SIGNATURE"]
            .clone();
        assert_ne!(a, b);
    }

    #[test]
    fn auth_signature_with_invalid_base64_secret_errors() {
        let bad = ApiCreds {
            secret: "not!valid!base64!".into(),
            ..fixed_creds()
        };
        assert!(auth_headers_at(&bad, "GET", "/order", "", "1700000000").is_err());
    }

    /// Known-answer test against fixed inputs. Cross-validates the HMAC
    /// pipeline (URL_SAFE secret decode, payload concatenation order,
    /// URL_SAFE output encode). The expected value is regression-pinned;
    /// if alloy/hmac/sha2 changes encoding semantics, this catches it.
    /// To regenerate: run the test, observe the actual signature, paste
    /// it as EXPECTED.
    #[test]
    fn auth_signature_known_answer_for_fixed_inputs() {
        let h =
            auth_headers_at(&fixed_creds(), "POST", "/order", r#"{"x":1}"#, "1700000000").unwrap();
        let actual = &h["POLY_SIGNATURE"];
        // Update EXPECTED below to the actual value the test produces.
        // The test is a regression pin — the value is whatever the URL_SAFE
        // HMAC-SHA256 pipeline outputs for these exact inputs.
        const EXPECTED: &str = "r2rzmYVt2V2GYIDxBCPhvgWmG3oJ6rSG4dr7qA2Bcs8=";
        assert_eq!(
            actual, EXPECTED,
            "HMAC pipeline output changed; update EXPECTED if intentional"
        );
    }
}
