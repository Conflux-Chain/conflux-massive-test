use prettytable::{Cell, Row, Table};
use std::collections::{BTreeSet, HashMap};

use crate::model::{AnalysisData, BlockScalars, NodePercentile, TxAnalysis};
use crate::stats::{statistics_from_vec, Statistics};

pub fn build_table_title() -> Table {
    let mut table = Table::new();
    table.set_titles(Row::new(vec![
        Cell::new("name_tmp"),
        Cell::new("Avg"),
        Cell::new("P10"),
        Cell::new("P30"),
        Cell::new("P50"),
        Cell::new("P80"),
        Cell::new("P90"),
        Cell::new("P95"),
        Cell::new("P99"),
        Cell::new("P999"),
        Cell::new("Max"),
        Cell::new("Cnt"),
    ]));
    table
}

pub fn add_block_rows(table: &mut Table, row_values: &mut HashMap<String, Vec<f64>>) {
    for t in ["Receive", "Sync", "Cons"] {
        for p in NodePercentile::all_in_order() {
            let metric = format!("block broadcast latency ({}/{})", t, p.name());
            let key = format!("{}::{}", t, p.name());
            let stats = statistics_from_vec(row_values.remove(&key).unwrap_or_default());
            table.add_row(row_from_stats(metric, stats, Some("%.2f")));
        }
    }

    for t in [
        "HeaderReady",
        "BodyReady",
        "SyncGraph",
        "ConsensusGraphStart",
        "ConsensusGraphReady",
        "ComputeEpoch",
        "NotifyTxPool",
        "TxPoolUpdated",
    ] {
        for p in NodePercentile::all_in_order() {
            let metric = format!("block event elapsed ({}/{})", t, p.name());
            let key = format!("{}::{}", t, p.name());
            let stats = statistics_from_vec(row_values.remove(&key).unwrap_or_default());
            table.add_row(row_from_stats(metric, stats, Some("%.2f")));
        }
    }
}

pub fn add_custom_block_rows(
    table: &mut Table,
    row_values: &mut HashMap<String, Vec<f64>>,
    custom_keys: &BTreeSet<String>,
) {
    for t in custom_keys {
        for p in NodePercentile::all_in_order() {
            let metric = format!("custom block event elapsed ({}/{})", t, p.name());
            let key = format!("{}::{}", t, p.name());
            let stats = statistics_from_vec(row_values.remove(&key).unwrap_or_default());
            table.add_row(row_from_stats(metric, stats, Some("%.2f")));
        }
    }
}

pub fn add_tx_rows(
    table: &mut Table,
    tx_latency_rows: &mut HashMap<NodePercentile, Vec<f64>>,
    tx_packed_rows: &mut HashMap<NodePercentile, Vec<f64>>,
    tx_analysis: &TxAnalysis,
    data: &AnalysisData,
) {
    if !tx_latency_rows
        .get(&NodePercentile::Avg)
        .map(|v| !v.is_empty())
        .unwrap_or(false)
    {
        return;
    }

    for p in NodePercentile::all_in_order() {
        let metric = format!("tx broadcast latency ({})", p.name());
        let stats = statistics_from_vec(tx_latency_rows.remove(p).unwrap_or_default());
        table.add_row(row_from_stats(metric, stats, Some("%.2f")));
    }

    for p in NodePercentile::all_in_order() {
        let metric = format!("tx packed to block latency ({})", p.name());
        let stats = statistics_from_vec(tx_packed_rows.remove(p).unwrap_or_default());
        table.add_row(row_from_stats(metric, stats, Some("%.2f")));
    }

    table.add_row(row_from_stats(
        "min tx packed to block latency".to_string(),
        statistics_from_vec(tx_analysis.min_tx_packed_to_block_latency.clone()),
        Some("%.2f"),
    ));
    table.add_row(row_from_stats(
        "min tx to ready pool latency".to_string(),
        statistics_from_vec(tx_analysis.min_tx_to_ready_pool_latency.clone()),
        Some("%.2f"),
    ));
    table.add_row(row_from_stats(
        "by_block_ratio".to_string(),
        statistics_from_vec(data.by_block_ratio.clone()),
        Some("%.2f"),
    ));
    table.add_row(row_from_stats(
        "Tx wait to be packed elasped time".to_string(),
        statistics_from_vec(data.tx_wait_to_be_packed.clone()),
        Some("%.2f"),
    ));
}

pub fn add_block_scalar_rows(table: &mut Table, scalars: &BlockScalars) {
    table.add_row(row_from_stats(
        "block txs".to_string(),
        statistics_from_vec(scalars.block_txs.clone()),
        None,
    ));
    table.add_row(row_from_stats(
        "block size".to_string(),
        statistics_from_vec(scalars.block_size.clone()),
        None,
    ));
    table.add_row(row_from_stats(
        "block referees".to_string(),
        statistics_from_vec(scalars.block_referees.clone()),
        None,
    ));
    table.add_row(row_from_stats(
        "block generation interval".to_string(),
        statistics_from_vec(scalars.intervals.clone()),
        Some("%.2f"),
    ));
}

pub fn add_sync_gap_rows(table: &mut Table, data: &AnalysisData) {
    table.add_row(row_from_stats(
        "node sync/cons gap (Avg)".to_string(),
        statistics_from_vec(data.sync_gap_avg.clone()),
        None,
    ));
    table.add_row(row_from_stats(
        "node sync/cons gap (P50)".to_string(),
        statistics_from_vec(data.sync_gap_p50.clone()),
        None,
    ));
    table.add_row(row_from_stats(
        "node sync/cons gap (P90)".to_string(),
        statistics_from_vec(data.sync_gap_p90.clone()),
        None,
    ));
    table.add_row(row_from_stats(
        "node sync/cons gap (P99)".to_string(),
        statistics_from_vec(data.sync_gap_p99.clone()),
        None,
    ));
    table.add_row(row_from_stats(
        "node sync/cons gap (Max)".to_string(),
        statistics_from_vec(data.sync_gap_max.clone()),
        None,
    ));
}

fn row_from_stats(name: String, s: Statistics, fmt: Option<&str>) -> Row {
    let f = |v: f64| -> String {
        if v.is_nan() {
            return "nan".to_string();
        }
        match fmt {
            Some("%.2f") => format!("{:.2}", v),
            _ => {
                if (v - v.round()).abs() < 1e-9 {
                    format!("{}", v as i64)
                } else {
                    format!("{:.2}", v)
                }
            }
        }
    };

    Row::new(vec![
        Cell::new(&name),
        Cell::new(&f(s.avg)),
        Cell::new(&f(s.p10)),
        Cell::new(&f(s.p30)),
        Cell::new(&f(s.p50)),
        Cell::new(&f(s.p80)),
        Cell::new(&f(s.p90)),
        Cell::new(&f(s.p95)),
        Cell::new(&f(s.p99)),
        Cell::new(&f(s.p999)),
        Cell::new(&f(s.max)),
        Cell::new(&format!("{}", s.cnt)),
    ])
}
