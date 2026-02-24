mod analyzer;
mod args;
mod config;
mod host_processing;
mod io_utils;
mod model;
mod quantile;
mod report;
mod stats;

use anyhow::{anyhow, Result};
use clap::Parser;
use std::time::Instant;

use analyzer::{
    analyze_txs, build_block_row_values, build_tx_rows, collect_block_scalars,
    print_throughput_and_slowest,
};
use args::{Args, QuantileImplArg};
use config::{default_latency_key_names, pivot_event_key_names};
use host_processing::{load_and_merge_hosts, validate_and_filter_blocks};
use model::AnalysisData;
use quantile::QuantileImpl;
use report::{
    add_block_rows, add_block_scalar_rows, add_custom_block_rows, add_sync_gap_rows, add_tx_rows,
    build_table_title,
};

fn main() -> Result<()> {
    let profile_enabled = std::env::var("STAT_LATENCY_PROFILE")
        .ok()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    let t0 = Instant::now();

    let args = Args::parse();
    if !args.log_path.exists() {
        return Err(anyhow!("log path not found: {}", args.log_path.display()));
    }

    let default_keys = default_latency_key_names();
    let pivot_keys = pivot_event_key_names();
    let quantile_impl = match args.quantile_impl {
        QuantileImplArg::Brute => QuantileImpl::Brute,
        QuantileImplArg::Tdigest => QuantileImpl::TDigest,
    };
    let mut data = AnalysisData::default();
    let t_load = Instant::now();
    load_and_merge_hosts(&args.log_path, &mut data, quantile_impl)?;
    if profile_enabled {
        eprintln!("[profile] load_and_merge_hosts: {:.3}s", t_load.elapsed().as_secs_f64());
    }

    if data.node_count == 0 {
        return Err(anyhow!("no nodes found (sync_cons_gap_stats empty)"));
    }

    validate_and_filter_blocks(&mut data, args.max_blocks);
    println!("{} nodes in total", data.node_count);
    println!("{} blocks generated", data.blocks.len());

    let t_analyze = Instant::now();
    let tx_analysis = analyze_txs(&data);
    let (mut row_values, custom_keys) = build_block_row_values(&data, &default_keys, &pivot_keys);
    let (mut tx_latency_rows, mut tx_packed_rows) = build_tx_rows(&data);
    if profile_enabled {
        eprintln!("[profile] analyze/build rows: {:.3}s", t_analyze.elapsed().as_secs_f64());
    }

    let t_report = Instant::now();
    let scalars = collect_block_scalars(&data);
    print_throughput_and_slowest(&scalars, &tx_analysis.slowest_packed_hash);

    let mut table = build_table_title();
    add_block_rows(&mut table, &mut row_values);
    add_custom_block_rows(&mut table, &mut row_values, &custom_keys);
    add_tx_rows(
        &mut table,
        &mut tx_latency_rows,
        &mut tx_packed_rows,
        &tx_analysis,
        &data,
    );
    add_block_scalar_rows(&mut table, &scalars);
    add_sync_gap_rows(&mut table, &data);
    table.printstd();
    if profile_enabled {
        eprintln!("[profile] render table/print: {:.3}s", t_report.elapsed().as_secs_f64());
        eprintln!("[profile] total main: {:.3}s", t0.elapsed().as_secs_f64());
    }

    Ok(())
}
