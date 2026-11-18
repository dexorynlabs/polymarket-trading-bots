//! Bounded reservoir sample store with Vitter's Algorithm R for unbiased
//! sampling beyond capacity. Deterministic with a fixed seed.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const RESERVOIR_CAPACITY: usize = 10_000;

pub struct BoundedReservoir {
    samples: Vec<f64>,
    capacity: usize,
    seen: u64,
    rng: StdRng,
}

impl BoundedReservoir {
    pub fn new(seed: u64) -> Self {
        Self {
            samples: Vec::with_capacity(RESERVOIR_CAPACITY),
            capacity: RESERVOIR_CAPACITY,
            seen: 0,
            rng: StdRng::seed_from_u64(seed),
        }
    }

    pub fn push(&mut self, x: f64) {
        if self.samples.len() < self.capacity {
            self.samples.push(x);
        } else {
            // Algorithm R: replace at index j ∈ [0, seen] with prob capacity / seen.
            let j = self.rng.gen_range(0..=self.seen);
            if (j as usize) < self.capacity {
                self.samples[j as usize] = x;
            }
        }
        self.seen += 1;
    }

    pub fn count(&self) -> u64 {
        self.seen
    }

    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Sort-on-call p50 / p99. Returns None if no samples seen.
    pub fn percentiles(&self) -> Option<(f64, f64)> {
        if self.samples.is_empty() {
            return None;
        }
        let mut sorted = self.samples.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let p50 = sorted[sorted.len() / 2];
        let p99_idx = ((sorted.len() as f64) * 0.99).min(sorted.len() as f64 - 1.0) as usize;
        let p99 = sorted[p99_idx];
        Some((p50, p99))
    }
}

impl Default for BoundedReservoir {
    fn default() -> Self {
        Self::new(0x52454e9a)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservoir_holds_under_capacity() {
        let mut r = BoundedReservoir::new(42);
        for i in 0..100 {
            r.push(i as f64);
        }
        assert_eq!(r.count(), 100);
        assert_eq!(r.sample_count(), 100);
        let (p50, p99) = r.percentiles().unwrap();
        assert!((p50 - 50.0).abs() < 1.0);
        assert!((p99 - 99.0).abs() < 1.0);
    }

    #[test]
    fn reservoir_samples_uniformly_above_capacity() {
        let mut r = BoundedReservoir::new(42);
        for i in 0..1_000_000 {
            let x = (i as f64) / 1_000_000.0;
            r.push(x);
        }
        assert_eq!(r.count(), 1_000_000);
        assert_eq!(r.sample_count(), 10_000);
        let (p50, _) = r.percentiles().unwrap();
        assert!((p50 - 0.5).abs() < 0.05, "p50 = {}, expected ~0.5", p50);
    }

    #[test]
    fn reservoir_deterministic_with_same_seed() {
        let mut r1 = BoundedReservoir::new(42);
        let mut r2 = BoundedReservoir::new(42);
        for i in 0..50_000 {
            r1.push(i as f64);
            r2.push(i as f64);
        }
        assert_eq!(r1.percentiles(), r2.percentiles());
    }

    #[test]
    fn reservoir_empty_returns_none() {
        let r = BoundedReservoir::new(0);
        assert_eq!(r.percentiles(), None);
    }
}
