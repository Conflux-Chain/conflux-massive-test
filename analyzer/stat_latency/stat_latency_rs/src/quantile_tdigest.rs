use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

use tdigests::TDigest;

#[derive(Debug, Clone, Copy, PartialEq)]
struct F64Ord(f64);

impl Eq for F64Ord {}

impl PartialOrd for F64Ord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for F64Ord {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.partial_cmp(&other.0).unwrap_or(Ordering::Equal)
    }
}

#[derive(Debug)]
pub struct TDigestQuantileState {
    digest: Option<TDigest>,
    high_tail_cap: usize,
    high_tail: BinaryHeap<Reverse<F64Ord>>,
}

impl TDigestQuantileState {
    pub fn new(expected_count: usize) -> Self {
        let high_tail_cap = ((expected_count as f64) * 0.1).ceil() as usize + 1;
        Self {
            digest: None,
            high_tail_cap: high_tail_cap.max(1),
            high_tail: BinaryHeap::new(),
        }
    }

    pub fn insert(&mut self, x: f64, count: u32) {
        let incoming = TDigest::from_values(vec![x]);
        let mut merged = match self.digest.take() {
            Some(existing) => existing.merge(&incoming),
            None => incoming,
        };
        if count % 1024 == 0 {
            merged.compress(2000);
        }
        self.digest = Some(merged);

        self.high_tail.push(Reverse(F64Ord(x)));
        if self.high_tail.len() > self.high_tail_cap {
            let _ = self.high_tail.pop();
        }
    }

    pub fn quantile(&self, q: f64, count: u32) -> f64 {
        if q >= 0.9 {
            if let Some(v) = self.high_quantile_exact_from_tail(q, count) {
                return v;
            }
        }
        self.digest
            .as_ref()
            .map(|d| d.estimate_quantile(q))
            .unwrap_or(f64::NAN)
    }

    fn high_quantile_exact_from_tail(&self, q: f64, count: u32) -> Option<f64> {
        if count == 0 || self.high_tail.is_empty() {
            return None;
        }
        let n = count as usize;
        let idx = ((n - 1) as f64 * q) as usize;
        let rank_from_top = (n - 1).saturating_sub(idx);

        if rank_from_top >= self.high_tail.len() {
            return None;
        }

        let mut desc: Vec<f64> = self.high_tail.iter().map(|x| x.0 .0).collect();
        desc.sort_by(|a, b| b.partial_cmp(a).unwrap_or(Ordering::Equal));
        desc.get(rank_from_top).copied()
    }
}
