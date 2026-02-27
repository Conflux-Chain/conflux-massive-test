use tdigests::TDigest;

#[derive(Debug)]
pub struct TDigestQuantileState {
    digest: Option<TDigest>,
    buffer: Vec<f64>,
}

impl TDigestQuantileState {
    pub fn new(_expected_count: usize) -> Self {
        Self {
            digest: None,
            buffer: vec![],
        }
    }

    pub fn insert(&mut self, x: f64) {
        self.buffer.push(x);
        if self.buffer.len() >= 200 {
            self.merge();
        }
    }

    pub fn merge(&mut self) {
        if self.buffer.is_empty() {
            return;
        }

        let incoming =
            TDigest::from_values(std::mem::replace(&mut self.buffer, Vec::with_capacity(300)));
        let mut merged = match self.digest.take() {
            Some(existing) => existing.merge(&incoming),
            None => incoming,
        };
        merged.compress(2000);
        self.digest = Some(merged);
    }

    pub fn quantile(&self, q: f64) -> f64 {
        self.digest
            .as_ref()
            .map(|d| d.estimate_quantile(q))
            .unwrap_or(f64::NAN)
    }
}
