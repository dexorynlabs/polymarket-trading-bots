//! Asserts the Safe variant of SignerService::sign_merge_call produces a
//! RelayerBody whose serialized JSON contains Safe-style signatureParams
//! (operation, safeTxGas, baseGas, gasToken, refundReceiver) and NO
//! Magic.link-style fields (relayHub, relay, relayerFee, gasLimit-with-PROXY-meaning).
//! Polarity counterpart to recon/tests/merger_onchain_relayer.rs lines 175-183.

use onecentbot::execution::signer::SignerService;

#[test]
fn safe_variant_omits_magiclink_fields() {
    let signer = SignerService::from_safe(
        "0x4747474747474747474747474747474747474747474747474747474747474747",
        "0x83eb3291367e3c61c80225e32907159010597b3d"
            .parse()
            .unwrap(),
        137,
    )
    .unwrap();

    let body = signer
        .sign_merge_call(
            /* inner_calldata = */ &[0xDE; 4], /* nonce = */ 7,
            /* gas_limit = */ 250_000,
        )
        .expect("safe sign succeeds");

    let json = body.to_json();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();

    // The exact discriminator string is unconfirmed per spec §10.2 risk #4 —
    // accept any of the candidate strings the probe (Task 0a.7) may settle on.
    let ty = v["type"].as_str().unwrap();
    assert!(
        matches!(ty, "SAFE" | "GNOSIS_SAFE" | "SAFE_V1_3_0"),
        "unknown Safe type discriminator: {ty}"
    );

    let sp = &v["signatureParams"];
    assert!(
        sp.get("operation").is_some(),
        "missing Safe-style operation"
    );
    assert!(
        sp.get("safeTxGas").is_some(),
        "missing Safe-style safeTxGas"
    );
    assert!(sp.get("baseGas").is_some(), "missing Safe-style baseGas");
    assert!(sp.get("gasToken").is_some(), "missing Safe-style gasToken");
    assert!(
        sp.get("refundReceiver").is_some(),
        "missing Safe-style refundReceiver"
    );

    assert!(
        sp.get("relayHub").is_none(),
        "Magic.link 'relayHub' leaked into Safe body"
    );
    assert!(
        sp.get("relay").is_none(),
        "Magic.link 'relay' leaked into Safe body"
    );
    assert!(
        sp.get("relayerFee").is_none(),
        "Magic.link 'relayerFee' leaked into Safe body"
    );
}
