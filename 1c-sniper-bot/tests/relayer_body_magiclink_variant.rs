//! Asserts the Magic.link variant of SignerService::sign_merge_call still
//! produces a PROXY body (no regression from the polarity-flip in Task 0a.5).
//! Companion to recon/tests/merger_onchain_relayer.rs lines 138-183.

use onecentbot::execution::signer::SignerService;

#[test]
fn magiclink_variant_omits_safe_fields() {
    let signer = SignerService::from_pk_hex(
        "0x4747474747474747474747474747474747474747474747474747474747474747",
    )
    .unwrap();

    let body = signer
        .sign_merge_call(
            &[0xDE; 4], /* nonce = */ 7, /* gas_limit = */ 250_000,
        )
        .expect("magiclink sign succeeds");

    let v: serde_json::Value = serde_json::from_str(&body.to_json()).unwrap();
    assert_eq!(v["type"], "PROXY");

    let sp = &v["signatureParams"];
    assert!(sp.get("relayHub").is_some(), "missing Magic.link relayHub");
    assert!(sp.get("relay").is_some(), "missing Magic.link relay");
    assert!(
        sp.get("operation").is_none(),
        "Safe-style 'operation' leaked into PROXY body"
    );
    assert!(
        sp.get("safeTxGas").is_none(),
        "Safe-style 'safeTxGas' leaked into PROXY body"
    );
}
