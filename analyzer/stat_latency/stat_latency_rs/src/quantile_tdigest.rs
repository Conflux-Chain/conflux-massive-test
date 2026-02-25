use tdigests::TDigest;

#[derive(Debug)]
pub struct TDigestQuantileState {
    digest: Option<TDigest>,
}

impl TDigestQuantileState {
    pub fn new(_expected_count: usize) -> Self {
        Self { digest: None }
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
    }

    pub fn quantile(&self, q: f64) -> f64 {
        self.digest
            .as_ref()
            .map(|d| d.estimate_quantile(q))
            .unwrap_or(f64::NAN)
    }
}
