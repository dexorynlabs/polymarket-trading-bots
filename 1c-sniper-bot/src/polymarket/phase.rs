// src/quoter/phase.rs

//! Per-slot phase clock based on wall-clock time elapsed since
//! `market_start_ts`. Strategy §9 lifecycle playbook + §15 Module 3.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotPhase {
    /// Before market_start_ts; book may still be populating.
    PreArm,
    /// T+0 to T+calm_end_secs. Tight dead-band, full cadence.
    Calm,
    /// T+calm_end_secs to T+mid_end_secs. Tightening, watching for jumps.
    Mid,
    /// T+mid_end_secs to T+shock_risk_end_secs. Reduced posture, kill triggers tightened.
    ShockRisk,
    /// T+shock_risk_end_secs to T+slot_duration_secs. Hard pull region.
    Endgame,
    /// After T+slot_duration_secs. Position-only.
    Resolved,
}

#[derive(Debug, Clone, Copy)]
pub struct PhaseClockCfg {
    pub slot_duration_secs: i64,
    pub calm_end_secs: i64,
    pub mid_end_secs: i64,
    pub shock_risk_end_secs: i64,
}

impl PhaseClockCfg {
    pub fn default_5m() -> Self {
        Self {
            slot_duration_secs: 300,
            calm_end_secs: 90,
            mid_end_secs: 150,
            shock_risk_end_secs: 240,
        }
    }
    pub fn default_15m() -> Self {
        Self {
            slot_duration_secs: 900,
            calm_end_secs: 270,
            mid_end_secs: 450,
            shock_risk_end_secs: 720,
        }
    }
    pub fn default_1h() -> Self {
        Self {
            slot_duration_secs: 3600,
            calm_end_secs: 1080,
            mid_end_secs: 1800,
            shock_risk_end_secs: 2880,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PhaseClock {
    market_start_ts: u64,
    cfg: PhaseClockCfg,
}

impl PhaseClock {
    pub fn new(market_start_ts: u64, cfg: PhaseClockCfg) -> Self {
        Self {
            market_start_ts,
            cfg,
        }
    }

    pub fn phase_at(&self, now_ts: u64) -> SlotPhase {
        let elapsed = (now_ts as i64) - (self.market_start_ts as i64);
        if elapsed < 0 {
            return SlotPhase::PreArm;
        }
        if elapsed < self.cfg.calm_end_secs {
            return SlotPhase::Calm;
        }
        if elapsed < self.cfg.mid_end_secs {
            return SlotPhase::Mid;
        }
        if elapsed < self.cfg.shock_risk_end_secs {
            return SlotPhase::ShockRisk;
        }
        if elapsed <= self.cfg.slot_duration_secs {
            return SlotPhase::Endgame;
        }
        SlotPhase::Resolved
    }

    pub fn elapsed_secs(&self, now_ts: u64) -> i64 {
        (now_ts as i64) - (self.market_start_ts as i64)
    }

    pub fn cfg(&self) -> &PhaseClockCfg {
        &self.cfg
    }
}

/// Per-phase tuning values, materialized once at slot spawn from TOML.
#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PhaseValues {
    pub dead_band_cents: f64,
    pub min_replace_interval_secs: u64,
    pub trade_rate_kill_per_sec: f64,
    pub no_new_posts: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PhaseSpecificCfg {
    pub calm: PhaseValues,
    pub mid: PhaseValues,
    pub shock_risk: PhaseValues,
    pub endgame: PhaseValues,
}

impl Default for PhaseSpecificCfg {
    fn default() -> Self {
        Self::default_5m()
    }
}

impl PhaseSpecificCfg {
    pub fn default_5m() -> Self {
        Self {
            calm: PhaseValues {
                dead_band_cents: 0.6,
                min_replace_interval_secs: 5,
                trade_rate_kill_per_sec: 1.2,
                no_new_posts: false,
            },
            mid: PhaseValues {
                dead_band_cents: 0.5,
                min_replace_interval_secs: 3,
                trade_rate_kill_per_sec: 1.2,
                no_new_posts: false,
            },
            shock_risk: PhaseValues {
                dead_band_cents: 0.5,
                min_replace_interval_secs: 3,
                trade_rate_kill_per_sec: 0.9,
                no_new_posts: false,
            },
            endgame: PhaseValues {
                dead_band_cents: 0.5,
                min_replace_interval_secs: 3,
                trade_rate_kill_per_sec: 0.9,
                no_new_posts: true,
            },
        }
    }

    pub fn for_phase(&self, phase: SlotPhase) -> PhaseValues {
        match phase {
            SlotPhase::PreArm | SlotPhase::Calm => self.calm,
            SlotPhase::Mid => self.mid,
            SlotPhase::ShockRisk => self.shock_risk,
            SlotPhase::Endgame | SlotPhase::Resolved => self.endgame,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_5m_slot_phases_correctly() {
        let cfg = PhaseClockCfg::default_5m();
        let market_start: u64 = 1_780_000_000;
        let clock = PhaseClock::new(market_start, cfg);

        assert_eq!(clock.phase_at(market_start - 1), SlotPhase::PreArm);
        assert_eq!(clock.phase_at(market_start + 0), SlotPhase::Calm);
        assert_eq!(clock.phase_at(market_start + 89), SlotPhase::Calm);
        assert_eq!(clock.phase_at(market_start + 90), SlotPhase::Mid);
        assert_eq!(clock.phase_at(market_start + 149), SlotPhase::Mid);
        assert_eq!(clock.phase_at(market_start + 150), SlotPhase::ShockRisk);
        assert_eq!(clock.phase_at(market_start + 239), SlotPhase::ShockRisk);
        assert_eq!(clock.phase_at(market_start + 240), SlotPhase::Endgame);
        assert_eq!(clock.phase_at(market_start + 300), SlotPhase::Endgame);
        assert_eq!(clock.phase_at(market_start + 301), SlotPhase::Resolved);
    }

    #[test]
    fn scales_to_15m_slot() {
        let cfg = PhaseClockCfg::default_15m();
        let clock = PhaseClock::new(0, cfg);
        assert_eq!(clock.phase_at(269), SlotPhase::Calm);
        assert_eq!(clock.phase_at(270), SlotPhase::Mid);
        assert_eq!(clock.phase_at(449), SlotPhase::Mid);
        assert_eq!(clock.phase_at(450), SlotPhase::ShockRisk);
        assert_eq!(clock.phase_at(720), SlotPhase::Endgame);
        assert_eq!(clock.phase_at(901), SlotPhase::Resolved);
    }

    #[test]
    fn elapsed_secs_is_correct() {
        let clock = PhaseClock::new(1_000_000_u64, PhaseClockCfg::default_5m());
        assert_eq!(clock.elapsed_secs(1_000_042), 42);
        assert_eq!(clock.elapsed_secs(999_999), -1);
    }

    #[test]
    fn phase_specific_cfg_returns_calm_values_in_calm_phase() {
        let phase_cfg = PhaseSpecificCfg::default_5m();
        let calm = phase_cfg.for_phase(SlotPhase::Calm);
        assert_eq!(calm.dead_band_cents, 0.6);
        assert_eq!(calm.min_replace_interval_secs, 5);
    }

    #[test]
    fn phase_specific_cfg_tightens_in_mid_and_shock() {
        let phase_cfg = PhaseSpecificCfg::default_5m();
        let mid = phase_cfg.for_phase(SlotPhase::Mid);
        let shock = phase_cfg.for_phase(SlotPhase::ShockRisk);
        assert_eq!(mid.dead_band_cents, 0.5);
        assert_eq!(mid.min_replace_interval_secs, 3);
        assert_eq!(shock.dead_band_cents, 0.5);
        assert_eq!(shock.trade_rate_kill_per_sec, 0.9);
    }

    #[test]
    fn phase_specific_cfg_endgame_blocks_new_posts() {
        let phase_cfg = PhaseSpecificCfg::default_5m();
        let end = phase_cfg.for_phase(SlotPhase::Endgame);
        assert!(end.no_new_posts, "endgame must forbid new posts");
    }
}
