use std::cmp::Ordering;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Statistics {
    pub avg: f64,
    pub p10: f64,
    pub p30: f64,
    pub p50: f64,
    pub p80: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
    pub cnt: usize,
}

pub fn statistics_from_sorted(data: &[f64]) -> Statistics {
    if data.is_empty() {
        return Statistics {
            avg: f64::NAN,
            p10: f64::NAN,
            p30: f64::NAN,
            p50: f64::NAN,
            p80: f64::NAN,
            p90: f64::NAN,
            p95: f64::NAN,
            p99: f64::NAN,
            p999: f64::NAN,
            max: f64::NAN,
            cnt: 0,
        };
    }

    let cnt = data.len();
    let sum: f64 = data.iter().sum();
    let avg = (sum / (cnt as f64) * 100.0).round() / 100.0;
    let pick = |q: f64| -> f64 {
        if cnt == 1 {
            return data[0];
        }
        let q = q.clamp(0.0, 1.0);
        let h = (cnt - 1) as f64 * q;
        let lo = h.floor() as usize;
        let hi = h.ceil() as usize;
        if lo == hi {
            return data[lo];
        }
        let w = h - (lo as f64);
        data[lo] + (data[hi] - data[lo]) * w
    };

    Statistics {
        avg,
        p10: pick(0.1),
        p30: pick(0.3),
        p50: pick(0.5),
        p80: pick(0.8),
        p90: pick(0.9),
        p95: pick(0.95),
        p99: pick(0.99),
        p999: pick(0.999),
        max: *data.last().unwrap(),
        cnt,
    }
}

pub fn statistics_from_vec(mut data: Vec<f64>) -> Statistics {
    data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    statistics_from_sorted(&data)
}

pub fn f64_from_stat(map: &HashMap<String, serde_json::Value>, key: &str) -> Option<f64> {
    map.get(key).and_then(|v| v.as_f64())
}
