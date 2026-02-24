use crate::model::NodePercentile;
use std::cmp::Ordering;
use tdigests::TDigest;

#[derive(Debug, Clone, Copy)]
pub enum QuantileImpl {
    Brute,
    TDigest,
}

fn exact_quantile(values: &[f64], q: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let idx = ((sorted.len() - 1) as f64 * q) as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[derive(Debug)]
pub struct QuantileAgg {
    pub count: u32,
    sum: f64,
    min: f64,
    max: f64,
    impl_kind: QuantileImpl,
    values: Vec<f64>,
    digest: Option<TDigest>,
}

impl QuantileAgg {
    pub fn new(impl_kind: QuantileImpl) -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            impl_kind,
            values: Vec::new(),
            digest: None,
        }
    }

    pub fn insert(&mut self, x: f64) {
        if x.is_nan() {
            return;
        }
        self.count += 1;
        self.sum += x;
        self.min = self.min.min(x);
        self.max = self.max.max(x);
        match self.impl_kind {
            QuantileImpl::Brute => self.values.push(x),
            QuantileImpl::TDigest => {
                let incoming = TDigest::from_values(vec![x]);
                let mut merged = match self.digest.take() {
                    Some(existing) => existing.merge(&incoming),
                    None => incoming,
                };
                if self.count % 1024 == 0 {
                    merged.compress(200);
                }
                self.digest = Some(merged);
            }
        }
    }

    pub fn value_for(&self, p: NodePercentile) -> f64 {
        match p {
            NodePercentile::Min => self.min,
            NodePercentile::Max => self.max,
            NodePercentile::Avg => match self.count {
                0 => f64::NAN,
                _ => (self.sum / (self.count as f64) * 100.0).round() / 100.0,
            },
            NodePercentile::P10 => self.quantile(0.1),
            NodePercentile::P30 => self.quantile(0.3),
            NodePercentile::P50 => self.quantile(0.5),
            NodePercentile::P80 => self.quantile(0.8),
            NodePercentile::P90 => self.quantile(0.9),
            NodePercentile::P95 => self.quantile(0.95),
            NodePercentile::P99 => self.quantile(0.99),
            NodePercentile::P999 => self.quantile(0.999),
        }
    }

    fn quantile(&self, q: f64) -> f64 {
        match self.impl_kind {
            QuantileImpl::Brute => exact_quantile(&self.values, q),
            QuantileImpl::TDigest => self
                .digest
                .as_ref()
                .map(|d| d.estimate_quantile(q))
                .unwrap_or(f64::NAN),
        }
    }
}
