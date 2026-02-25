use ethereum_types::H256;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

use crate::model::{AnalysisData, BlockScalars, NodePercentile, TxAnalysis};

fn collect_tx_node_percentiles(latencies: &[f64]) -> HashMap<NodePercentile, f64> {
    let pick = |q: f64| -> f64 {
        let idx = ((latencies.len() - 1) as f64 * q) as usize;
        latencies[idx.min(latencies.len() - 1)]
    };
    let sum: f64 = latencies.iter().sum();
    let avg = (sum / (latencies.len() as f64) * 100.0).round() / 100.0;

    let mut out = HashMap::new();
    for p in NodePercentile::all_in_order() {
        let v = match p {
            NodePercentile::Min => *latencies.first().unwrap(),
            NodePercentile::Max => *latencies.last().unwrap(),
            NodePercentile::Avg => avg,
            _ => pick(p.q().unwrap()),
        };
        out.insert(*p, v);
    }
    out
}

fn min_recv_and_latency(values: &[f32], baseline: f64) -> Vec<f64> {
    let mut latencies: Vec<f64> = values.iter().map(|t| (*t as f64) - baseline).collect();
    latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    latencies
}

pub fn analyze_txs(data: &AnalysisData) -> TxAnalysis {
    let mut missing_tx = 0usize;
    let mut unpacked_tx = 0usize;
    let mut result = TxAnalysis::default();
    let mut best: Option<(H256, f64)> = None;

    for (h, tx) in &data.txs {
        if tx.received.len() != data.node_count {
            missing_tx += 1;
        }
        if tx.packed.is_empty() {
            unpacked_tx += 1;
        }
        if tx.packed.is_empty() {
            continue;
        }

        let min_recv = tx.received.iter().copied().fold(f32::INFINITY, f32::min) as f64;
        let min_packed = tx.packed.iter().copied().fold(f32::INFINITY, f32::min) as f64;
        let latency = min_packed - min_recv;
        result.min_tx_packed_to_block_latency.push(latency);

        match best {
            None => best = Some((*h, latency)),
            Some((_, cur)) if latency > cur => best = Some((*h, latency)),
            _ => {}
        }

        if !tx.ready.is_empty() {
            let min_ready = tx.ready.iter().copied().fold(f32::INFINITY, f32::min) as f64;
            result
                .min_tx_to_ready_pool_latency
                .push(min_ready - min_recv);
        }
    }

    println!(
        "Removed tx count (txs have not fully propagated) {}",
        missing_tx
    );
    println!("Unpacked tx count {}", unpacked_tx);
    println!("Total tx count {}", data.txs.len());
    result.slowest_packed_hash = best.map(|(h, _)| h);
    result
}

fn should_require_90pct(k: &str, is_default: bool, pivot_keys: &HashSet<&'static str>) -> bool {
    if is_default {
        pivot_keys.contains(k)
    } else {
        true
    }
}

pub fn build_block_row_values(
    data: &AnalysisData,
    default_keys: &HashSet<&'static str>,
    pivot_keys: &HashSet<&'static str>,
) -> (HashMap<String, Vec<f64>>, BTreeSet<String>) {
    let mut row_values: HashMap<String, Vec<f64>> = HashMap::new();
    let mut custom_keys: BTreeSet<String> = BTreeSet::new();

    for per_key in data.block_dists.values() {
        for k in per_key.keys() {
            if !default_keys.contains(k.as_str()) {
                custom_keys.insert(k.clone());
            }
        }
    }

    for per_key in data.block_dists.values() {
        for (k, agg) in per_key {
            let is_default = default_keys.contains(k.as_str());
            if should_require_90pct(k, is_default, pivot_keys) {
                let threshold = (0.9 * (data.node_count as f64)).floor() as u32;
                if agg.count < threshold {
                    continue;
                }
            }

            for p in NodePercentile::all_in_order() {
                let row_key = format!("{}::{}", k, p.name());
                row_values
                    .entry(row_key)
                    .or_insert_with(Vec::new)
                    .push(agg.value_for(*p));
            }
        }
    }

    (row_values, custom_keys)
}

pub fn build_tx_rows(
    data: &AnalysisData,
) -> (
    HashMap<NodePercentile, Vec<f64>>,
    HashMap<NodePercentile, Vec<f64>>,
) {
    let mut tx_latency_rows: HashMap<NodePercentile, Vec<f64>> = HashMap::new();
    let mut tx_packed_rows: HashMap<NodePercentile, Vec<f64>> = HashMap::new();

    for tx in data.txs.values() {
        if tx.received.len() == data.node_count {
            let min_recv = tx.received.iter().copied().fold(f32::INFINITY, f32::min) as f64;
            let latencies = min_recv_and_latency(&tx.received, min_recv);
            let per = collect_tx_node_percentiles(&latencies);
            for p in NodePercentile::all_in_order() {
                tx_latency_rows
                    .entry(*p)
                    .or_insert_with(Vec::new)
                    .push(*per.get(p).unwrap());
            }
        }

        if !tx.packed.is_empty() {
            let min_recv = tx.received.iter().copied().fold(f32::INFINITY, f32::min) as f64;
            let latencies = min_recv_and_latency(&tx.packed, min_recv);
            let per = collect_tx_node_percentiles(&latencies);
            for p in NodePercentile::all_in_order() {
                tx_packed_rows
                    .entry(*p)
                    .or_insert_with(Vec::new)
                    .push(*per.get(p).unwrap());
            }
        }
    }

    (tx_latency_rows, tx_packed_rows)
}

pub fn collect_block_scalars(data: &AnalysisData) -> BlockScalars {
    let mut block_txs: Vec<f64> = Vec::new();
    let mut block_size: Vec<f64> = Vec::new();
    let mut block_referees: Vec<f64> = Vec::new();
    let mut block_timestamps: Vec<i64> = Vec::new();
    let mut max_time: i64 = 0;
    let mut min_time: i64 = i64::MAX;

    for b in data.blocks.values() {
        block_txs.push(b.txs as f64);
        block_size.push(b.size as f64);
        block_referees.push(b.referee_count as f64);
        block_timestamps.push(b.timestamp);
        if b.txs > 0 {
            min_time = min_time.min(b.timestamp);
            max_time = max_time.max(b.timestamp);
        }
    }

    block_timestamps.sort();
    let mut intervals: Vec<f64> = Vec::new();
    for w in block_timestamps.windows(2) {
        intervals.push((w[1] - w[0]) as f64);
    }

    BlockScalars {
        block_txs,
        block_size,
        block_referees,
        intervals,
        tx_sum: data.blocks.values().map(|b| b.txs).sum(),
        duration: max_time.saturating_sub(min_time),
    }
}

pub fn print_throughput_and_slowest(scalars: &BlockScalars, slowest_packed_hash: &Option<H256>) {
    println!("{} txs generated", scalars.tx_sum);
    match scalars.duration <= 0 {
        true => {
            println!("Test duration is 0.00 seconds");
            println!("Throughput is N/A (duration is 0)");
        }
        false => {
            println!("Test duration is {:.2} seconds", scalars.duration as f64);
            println!(
                "Throughput is {}",
                (scalars.tx_sum as f64) / (scalars.duration as f64)
            );
        }
    }
    if let Some(h) = slowest_packed_hash {
        println!("Slowest packed transaction hash: {:#x}", h);
    }
}
