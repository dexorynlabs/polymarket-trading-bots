//! Generic rolling time-window buffer keyed by microsecond timestamp.
//! Used by BookState (Module 2) for mid samples, trade rates, and depth-MA.

use std::collections::VecDeque;

/// Stores `(value, ts_micros)` pairs, evicts entries older than `window_secs`.
#[derive(Debug, Clone)]
pub struct RollingWindow<T> {
    window_secs: u64,
    buf: VecDeque<(T, u64)>,
}

impl<T: Copy + PartialOrd> RollingWindow<T> {
    pub fn new(window_secs: u64) -> Self {
        Self {
            window_secs,
            buf: VecDeque::new(),
        }
    }

    pub fn push(&mut self, value: T, ts_micros: u64) {
        self.buf.push_back((value, ts_micros));
        self.evict_older_than(ts_micros);
    }

    pub fn evict_older_than(&mut self, now_micros: u64) {
        let threshold = now_micros.saturating_sub(self.window_secs * 1_000_000);
        while let Some(&(_, ts)) = self.buf.front() {
            if ts < threshold {
                self.buf.pop_front();
            } else {
                break;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.buf.len()
    }
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn min(&self) -> Option<T> {
        self.buf
            .iter()
            .map(|(v, _)| *v)
            .reduce(|a, b| if a < b { a } else { b })
    }
    pub fn max(&self) -> Option<T> {
        self.buf
            .iter()
            .map(|(v, _)| *v)
            .reduce(|a, b| if a > b { a } else { b })
    }

    /// Iterate values whose timestamp >= cutoff_micros, in insertion order.
    pub fn iter_recent(&self, cutoff_micros: u64) -> impl Iterator<Item = T> + '_ {
        self.buf
            .iter()
            .filter(move |(_, ts)| *ts >= cutoff_micros)
            .map(|(v, _)| *v)
    }
}

impl RollingWindow<f64> {
    pub fn avg(&self) -> Option<f64> {
        if self.buf.is_empty() {
            return None;
        }
        let sum: f64 = self.buf.iter().map(|(v, _)| v).sum();
        Some(sum / self.buf.len() as f64)
    }
}

impl RollingWindow<u32> {
    pub fn count(&self) -> usize {
        self.buf.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pushes_and_evicts_by_age() {
        let mut win = RollingWindow::<f64>::new(60);
        win.push(0.50, 0); // ts_micros = 0
        win.push(0.51, 1_000_000); // 1s later
        win.push(0.52, 30_000_000); // 30s
        assert_eq!(win.len(), 3);
        win.evict_older_than(31_000_000); // current=31s, window=60s, threshold=-29s — keeps all
        assert_eq!(win.len(), 3);
        win.evict_older_than(95_000_000); // current=95s, threshold=35s — evicts 0s, 1s, AND 30s
        assert_eq!(win.len(), 0);
    }

    #[test]
    fn computes_min_max_avg() {
        let mut win = RollingWindow::<f64>::new(60);
        win.push(0.40, 0);
        win.push(0.60, 1_000_000);
        win.push(0.50, 2_000_000);
        assert_eq!(win.min(), Some(0.40));
        assert_eq!(win.max(), Some(0.60));
        assert!((win.avg().unwrap() - 0.50).abs() < 1e-9);
    }

    #[test]
    fn empty_window_returns_none() {
        let win = RollingWindow::<f64>::new(60);
        assert_eq!(win.min(), None);
        assert_eq!(win.max(), None);
        assert_eq!(win.avg(), None);
    }
}
