use crate::model::NodePercentile;
use crate::quantile_brute::BruteQuantileState;
use crate::quantile_tdigest::TDigestQuantileState;

#[derive(Debug, Clone, Copy)]
pub enum QuantileImpl {
    Brute,
    TDigest,
}

#[derive(Debug)]
enum QuantileBackend {
    Brute(BruteQuantileState),
    TDigest(TDigestQuantileState),
}

#[derive(Debug)]
pub struct QuantileAgg {
    pub count: u32,
    sum: f64,
    min: f64,
    max: f64,
    backend: QuantileBackend,
}

impl QuantileAgg {
    pub fn new(impl_kind: QuantileImpl, expected_count: usize) -> Self {
        let backend = match impl_kind {
            QuantileImpl::Brute => QuantileBackend::Brute(BruteQuantileState::new()),
            QuantileImpl::TDigest => {
                QuantileBackend::TDigest(TDigestQuantileState::new(expected_count))
            }
        };
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            backend,
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
        match &mut self.backend {
            QuantileBackend::Brute(state) => state.insert(x),
            QuantileBackend::TDigest(state) => state.insert(x, self.count),
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
        match &self.backend {
            QuantileBackend::Brute(state) => state.quantile(q),
            QuantileBackend::TDigest(state) => state.quantile(q),
        }
    }
}
