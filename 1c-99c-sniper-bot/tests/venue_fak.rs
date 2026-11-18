use sniperbot::types::Side;
use sniperbot::execution::venue::{FakOutcome, FakRequest, ShadowVenue, Venue};

fn req(limit_px: f64, size_shares: f64) -> FakRequest {
    FakRequest {
        token_id: "tok".into(),
        slug: "btc-updown-5m-1784000000".into(),
        side: Side::Up,
        limit_px,
        size_shares,
    }
}

#[tokio::test]
async fn shadow_records_every_would_post_and_never_errors() {
    let v = ShadowVenue::new();
    let out = v.send_fak(req(0.03, 166.0)).await.unwrap();
    assert!(matches!(out, FakOutcome::Acked { .. }));
    v.send_fak(req(0.18, 27.0)).await.unwrap();
    let posts = v.posts();
    assert_eq!(posts.len(), 2);
    assert_eq!(posts[0].limit_px, 0.03);
    assert_eq!(posts[1].size_shares, 27.0);
}

#[tokio::test]
async fn shadow_order_ids_are_unique_and_marked() {
    let v = ShadowVenue::new();
    let a = v.send_fak(req(0.03, 166.0)).await.unwrap();
    let b = v.send_fak(req(0.03, 166.0)).await.unwrap();
    let (FakOutcome::Acked { order_id: ia } | FakOutcome::ZeroFill { order_id: ia }) = a;
    let (FakOutcome::Acked { order_id: ib } | FakOutcome::ZeroFill { order_id: ib }) = b;
    assert_ne!(ia, ib);
    assert!(ia.starts_with("shadow-"), "shadow ids must be unmistakable: {ia}");
}
