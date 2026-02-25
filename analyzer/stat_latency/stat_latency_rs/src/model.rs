use crate::quantile::QuantileAgg;
use ethereum_types::H256;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;

fn parse_h256(s: &str) -> Result<H256, String> {
    match H256::from_str(s) {
        Ok(v) => Ok(v),
        Err(_) => {
            let prefixed = format!("0x{}", s);
            H256::from_str(&prefixed).map_err(|e| format!("invalid hash '{}': {}", s, e))
        }
    }
}

fn deserialize_h256_map<'de, D, V>(deserializer: D) -> Result<HashMap<H256, V>, D::Error>
where
    D: serde::Deserializer<'de>,
    V: Deserialize<'de>,
{
    let raw: HashMap<String, V> = HashMap::deserialize(deserializer)?;
    let mut out = HashMap::with_capacity(raw.len());
    for (k, v) in raw {
        let key = parse_h256(&k).map_err(serde::de::Error::custom)?;
        out.insert(key, v);
    }
    Ok(out)
}

fn deserialize_h256_vec<'de, D>(deserializer: D) -> Result<Vec<H256>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw: Vec<String> = Vec::deserialize(deserializer)?;
    let mut out = Vec::with_capacity(raw.len());
    for item in raw {
        out.push(parse_h256(&item).map_err(serde::de::Error::custom)?);
    }
    Ok(out)
}

#[derive(Debug, Deserialize, Default)]
pub struct HostBlocksLog {
    #[serde(default, deserialize_with = "deserialize_h256_map")]
    pub blocks: HashMap<H256, BlockJson>,
    #[serde(default, deserialize_with = "deserialize_h256_map")]
    pub txs: HashMap<H256, TxJson>,
    #[serde(default)]
    pub sync_cons_gap_stats: Vec<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    pub by_block_ratio: Vec<f64>,
}

#[derive(Debug, Deserialize, Default)]
pub struct BlockJson {
    #[serde(default)]
    pub timestamp: i64,
    #[serde(default)]
    pub txs: i64,
    #[serde(default)]
    pub size: i64,
    #[serde(default, deserialize_with = "deserialize_h256_vec")]
    pub referees: Vec<H256>,
    #[serde(default)]
    pub latencies: HashMap<String, Vec<f64>>,
}

#[derive(Debug, Deserialize, Default)]
pub struct TxJson {
    #[serde(default)]
    pub received_timestamps: Vec<f64>,
    #[serde(default)]
    pub packed_timestamps: Vec<Option<f64>>,
    #[serde(default)]
    pub ready_pool_timestamps: Vec<Option<f64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NodePercentile {
    Min,
    Avg,
    P10,
    P30,
    P50,
    P80,
    P90,
    P95,
    P99,
    P999,
    Max,
}

impl NodePercentile {
    pub fn all_in_order() -> &'static [NodePercentile] {
        use NodePercentile::*;
        &[Min, Avg, P10, P30, P50, P80, P90, P95, P99, P999, Max]
    }

    pub fn name(self) -> &'static str {
        match self {
            NodePercentile::Min => "Min",
            NodePercentile::Avg => "Avg",
            NodePercentile::P10 => "P10",
            NodePercentile::P30 => "P30",
            NodePercentile::P50 => "P50",
            NodePercentile::P80 => "P80",
            NodePercentile::P90 => "P90",
            NodePercentile::P95 => "P95",
            NodePercentile::P99 => "P99",
            NodePercentile::P999 => "P999",
            NodePercentile::Max => "Max",
        }
    }

    pub fn q(self) -> Option<f64> {
        match self {
            NodePercentile::Min => Some(0.0),
            NodePercentile::Avg => None,
            NodePercentile::P10 => Some(0.1),
            NodePercentile::P30 => Some(0.3),
            NodePercentile::P50 => Some(0.5),
            NodePercentile::P80 => Some(0.8),
            NodePercentile::P90 => Some(0.9),
            NodePercentile::P95 => Some(0.95),
            NodePercentile::P99 => Some(0.99),
            NodePercentile::P999 => Some(0.999),
            NodePercentile::Max => Some(1.0),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct BlockInfo {
    pub timestamp: i64,
    pub txs: i64,
    pub size: i64,
    pub referee_count: i64,
}

#[derive(Debug, Default)]
pub struct TxAgg {
    pub received: Vec<f32>,
    pub packed: Vec<f32>,
    pub ready: Vec<f32>,
}

#[derive(Debug, Default)]
pub struct AnalysisData {
    pub node_count: usize,
    pub sync_gap_avg: Vec<f64>,
    pub sync_gap_p50: Vec<f64>,
    pub sync_gap_p90: Vec<f64>,
    pub sync_gap_p99: Vec<f64>,
    pub sync_gap_max: Vec<f64>,
    pub by_block_ratio: Vec<f64>,
    pub tx_wait_to_be_packed: Vec<f64>,
    pub blocks: HashMap<H256, BlockInfo>,
    pub block_dists: HashMap<H256, HashMap<String, QuantileAgg>>,
    pub txs: HashMap<H256, TxAgg>,
}

#[derive(Debug, Default)]
pub struct TxAnalysis {
    pub min_tx_packed_to_block_latency: Vec<f64>,
    pub min_tx_to_ready_pool_latency: Vec<f64>,
    pub slowest_packed_hash: Option<H256>,
}

#[derive(Debug, Default)]
pub struct BlockScalars {
    pub block_txs: Vec<f64>,
    pub block_size: Vec<f64>,
    pub block_referees: Vec<f64>,
    pub intervals: Vec<f64>,
    pub tx_sum: i64,
    pub duration: i64,
}
