use onecentbot::bot::runtime::{resolve_p0, LastClose};

const SO: u64 = 1_784_000_000;

#[test]
fn p0_is_the_close_of_the_immediately_preceding_kline() {
    // The kline for second SO-1 closes at (SO-1)*1000 + 999.
    let latch = LastClose { close_px: 73_592.95, close_ts_ms: (SO - 1) * 1000 + 999 };
    assert_eq!(resolve_p0(Some(latch), SO), Some(73_592.95));
}

#[test]
fn a_non_contiguous_latch_means_the_slot_never_arms() {
    // Two seconds stale: a feed gap across the boundary.
    let latch = LastClose { close_px: 73_592.95, close_ts_ms: (SO - 2) * 1000 + 999 };
    assert_eq!(resolve_p0(Some(latch), SO), None);
    // Badly stale — this is the case that would poison D outright.
    let latch = LastClose { close_px: 70_000.0, close_ts_ms: (SO - 300) * 1000 + 999 };
    assert_eq!(resolve_p0(Some(latch), SO), None);
}

#[test]
fn a_latch_from_inside_the_slot_is_rejected() {
    // The FIRST CLOSE INSIDE the slot is the nearby-close idiom: a $3.09-mean
    // error against a $12 gate floor. It must never be accepted as p0.
    let latch = LastClose { close_px: 73_592.95, close_ts_ms: SO * 1000 + 999 };
    assert_eq!(resolve_p0(Some(latch), SO), None);
}

#[test]
fn no_latch_means_the_slot_never_arms() {
    assert_eq!(resolve_p0(None, SO), None);
}
