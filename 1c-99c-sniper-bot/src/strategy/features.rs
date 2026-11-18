//! PURE per-slot feature series. Direct port of `features()` in
//! the off-line signal-sweep script — the semantics must match EXACTLY; the
//! cards were validated with these definitions and no others (spec §4:
//! "Do not add corrections").
//!
//! Only the subset the shipped cards use is ported (D, v2, v3, sigma60, zD).
//! The harness also computes e2/ext/se/bounce/bf; no shipped card in spec §5
//! reads them, so porting them would be dead code.

use crate::types::Side;

pub const SLOT_SECS: usize = 300;
const SIGMA_WINDOW: usize = 60;

/// Per-side features at one second. `d > 0` means THIS side is losing by `d`
/// dollars; velocities are dollars-per-second of deficit CLOSING, so positive
/// means recovering.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SideFeatures {
    pub d: f64,
    pub v2: f64,
    pub v3: f64,
    pub sigma60: f64,
    pub z_d: f64,
    pub tau: f64,
}

/// The slot's 1s closes plus `p0`. Slot-local by construction: sigma60 resets
/// at every slot open, exactly as the harness computes it per slot array. No
/// cross-slot history is needed or wanted.
#[derive(Debug, Clone)]
pub struct SlotSeries {
    p0: f64,
    closes: Vec<f64>,
}

impl SlotSeries {
    /// `p0` is the close of the kline IMMEDIATELY PRECEDING slot open — NOT
    /// the first close inside the slot. See design §4.1: the harness uses the
    /// kline `open`, which `SpotClose1s` does not carry; the preceding close
    /// matches it with p50 $0.00 / p90 $0.01, while the first in-slot close is
    /// off by a $3.09 mean against a $12 gate floor.
    pub fn new(p0: f64) -> Self {
        Self { p0, closes: Vec::with_capacity(SLOT_SECS) }
    }

    /// Append the close for second `t`. Contiguity is enforced: a gap must
    /// surface as an error so the caller can disarm rather than silently
    /// computing velocities across a hole.
    pub fn push(&mut self, t: usize, close_px: f64) -> anyhow::Result<()> {
        anyhow::ensure!(
            t == self.closes.len(),
            "kline gap: expected t={}, got t={}",
            self.closes.len(),
            t
        );
        anyhow::ensure!(t < SLOT_SECS, "t={t} past slot end");
        anyhow::ensure!(close_px.is_finite(), "close_px is not finite: {close_px}");
        self.closes.push(close_px);
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.closes.len()
    }

    /// The latched slot `p0`. Read by the tick path, which forms its own
    /// deficit `-(tick_px - p0) * sign` from a trade price rather than from a
    /// bar close — the SAME p0, never a re-derived one (`runtime::resolve_p0`
    /// is the only thing allowed to produce it).
    pub fn p0(&self) -> f64 {
        self.p0
    }

    pub fn is_empty(&self) -> bool {
        self.closes.is_empty()
    }

    fn d_at(&self, t: usize, sign: f64) -> f64 {
        (self.p0 - self.closes[t]) * sign
    }

    /// Population std of the 60 close-to-close diffs `c[i] - c[i-1]` for
    /// `i` in `(t-59)..=t`. Matches the harness's `sig[W:]`, which is valid
    /// from `t = 60`. Population (divide by N), not sample.
    pub fn sigma60(&self, t: usize) -> Option<f64> {
        if t < SIGMA_WINDOW || t >= self.closes.len() {
            return None;
        }
        let (mut s1, mut s2) = (0.0f64, 0.0f64);
        for i in (t - SIGMA_WINDOW + 1)..=t {
            let d = self.closes[i] - self.closes[i - 1];
            s1 += d;
            s2 += d * d;
        }
        let n = SIGMA_WINDOW as f64;
        let (m1, m2) = (s1 / n, s2 / n);
        Some((m2 - m1 * m1).max(0.0).sqrt())
    }

    /// Features for `side` at second `t`. `None` until both the 3s velocity
    /// window and the 60s sigma window are available (t >= 60). Every shipped
    /// card gates on `t >= 150`, so this never binds in practice.
    pub fn side(&self, t: usize, side: Side) -> Option<SideFeatures> {
        if t >= self.closes.len() || t < 3 {
            return None;
        }
        let sign = match side {
            Side::Up => 1.0,
            Side::Down => -1.0,
        };
        let d = self.d_at(t, sign);
        let v2 = (self.d_at(t - 2, sign) - d) / 2.0;
        let v3 = (self.d_at(t - 3, sign) - d) / 3.0;
        let sigma60 = self.sigma60(t)?;
        let tau = (SLOT_SECS as f64 - t as f64).max(1.0);
        // Harness: `np.where(sig > 1e-9, D / (sig * sqrt(tau)), inf)`.
        let z_d = if sigma60 > 1e-9 { d / (sigma60 * tau.sqrt()) } else { f64::INFINITY };
        Some(SideFeatures { d, v2, v3, sigma60, z_d, tau })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Side;

    /// Mirrors `_step_series()` in the signal-sweep script's test file:
    /// up-side deficit of $40 from t=150, closing to $30 at t=190.
    fn step_series() -> SlotSeries {
        let mut s = SlotSeries::new(100.0);
        for t in 0..SLOT_SECS {
            let c = if t >= 190 { 70.0 } else if t >= 150 { 60.0 } else { 100.0 };
            s.push(t, c).unwrap();
        }
        s
    }

    // Port of test_features_velocities_match_harness_convention.
    #[test]
    fn velocities_match_harness_convention() {
        let s = step_series();
        assert_eq!(s.side(189, Side::Up).unwrap().d, 40.0);
        let f = s.side(190, Side::Up).unwrap();
        assert_eq!(f.d, 30.0);
        // v_w(t) = (D(t-w) - D(t)) / w
        assert!((f.v3 - 10.0 / 3.0).abs() < 1e-9, "v3 = {}", f.v3);
        assert!((f.v2 - 5.0).abs() < 1e-9, "v2 = {}", f.v2);
        // one second later the step has fully rolled out of the 2s window
        assert_eq!(s.side(192, Side::Up).unwrap().v2, 0.0);
    }

    // Port of test_features_sigma_and_zd, using the closed-form step series
    // rather than numpy's RNG (the RNG fixture is exercised in Task 8).
    #[test]
    fn sigma60_is_population_std_of_the_trailing_60_diffs() {
        let s = step_series();
        // Diffs over i in 191..=250 are all zero -> sigma 0.
        assert_eq!(s.sigma60(250).unwrap(), 0.0);
        // At t=200 the window i in 141..=200 contains exactly two non-zero
        // diffs: -40 (at i=150) and +10 (at i=190).
        let sig = s.sigma60(200).unwrap();
        let diffs: Vec<f64> = (141..=200)
            .map(|i| if i == 150 { -40.0 } else if i == 190 { 10.0 } else { 0.0 })
            .collect();
        let m1: f64 = diffs.iter().sum::<f64>() / 60.0;
        let m2: f64 = diffs.iter().map(|d| d * d).sum::<f64>() / 60.0;
        assert!((sig - (m2 - m1 * m1).sqrt()).abs() < 1e-9);
    }

    #[test]
    fn sigma60_is_none_before_t60_and_zd_is_infinite_when_sigma_degenerate() {
        let s = step_series();
        assert!(s.sigma60(59).is_none(), "sigma60 needs 60 diffs, valid from t=60");
        assert!(s.sigma60(60).is_some());
        // t=250: sigma is exactly 0 -> zD = +inf (spec §4: "no trade")
        let f = s.side(250, Side::Up).unwrap();
        assert_eq!(f.sigma60, 0.0);
        assert!(f.z_d.is_infinite() && f.z_d.is_sign_positive());
    }

    #[test]
    fn tau_is_seconds_remaining_floored_at_one() {
        let s = step_series();
        assert_eq!(s.side(200, Side::Up).unwrap().tau, 100.0);
        assert_eq!(s.side(299, Side::Up).unwrap().tau, 1.0);
    }

    #[test]
    fn down_side_is_the_sign_mirror_of_up() {
        let s = step_series();
        let up = s.side(190, Side::Up).unwrap();
        let dn = s.side(190, Side::Down).unwrap();
        assert_eq!(dn.d, -up.d);
        assert_eq!(dn.v3, -up.v3);
        // sigma is sign-free
        assert_eq!(dn.sigma60, up.sigma60);
    }

    #[test]
    fn push_rejects_a_gap() {
        let mut s = SlotSeries::new(100.0);
        s.push(0, 100.0).unwrap();
        let err = s.push(2, 100.0).unwrap_err().to_string();
        assert!(err.contains("gap"), "got: {err}");
    }

    #[test]
    fn push_rejects_a_non_finite_close() {
        let mut s = SlotSeries::new(100.0);
        let err = s.push(0, f64::NAN).unwrap_err().to_string();
        assert!(err.contains("not finite"), "got: {err}");
    }

    // Every diff in this series is non-zero, so unlike step_series() the
    // window edges are pinned: shrinking the loop by one diff (still divided
    // by SIGMA_WINDOW) changes the result. See finding-1 report.
    fn wavy_series() -> SlotSeries {
        let mut s = SlotSeries::new(100.0);
        for t in 0..SLOT_SECS {
            let c = 100.0 + (t as f64 * 0.7).sin() * 3.0;
            s.push(t, c).unwrap();
        }
        s
    }

    #[test]
    fn sigma60_matches_an_independently_computed_population_std_with_all_nonzero_diffs() {
        let s = wavy_series();
        let t = 200;
        let closes: Vec<f64> =
            (0..SLOT_SECS).map(|i| 100.0 + (i as f64 * 0.7).sin() * 3.0).collect();
        let diffs: Vec<f64> = ((t - 59)..=t).map(|i| closes[i] - closes[i - 1]).collect();
        assert_eq!(diffs.len(), 60);
        assert!(diffs.iter().all(|d| d.abs() > 1e-9), "expected every diff to be non-zero");
        let m1: f64 = diffs.iter().sum::<f64>() / 60.0;
        let m2: f64 = diffs.iter().map(|d| d * d).sum::<f64>() / 60.0;
        let expected = (m2 - m1 * m1).sqrt();
        let got = s.sigma60(t).unwrap();
        assert!((got - expected).abs() < 1e-9, "got {got}, expected {expected}");
    }
}
