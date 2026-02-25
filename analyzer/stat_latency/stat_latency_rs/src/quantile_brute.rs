use std::cmp::Ordering;

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
pub struct BruteQuantileState {
    values: Vec<f64>,
}

impl BruteQuantileState {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    pub fn insert(&mut self, x: f64) {
        self.values.push(x);
    }

    pub fn quantile(&self, q: f64) -> f64 {
        exact_quantile(&self.values, q)
    }
}
