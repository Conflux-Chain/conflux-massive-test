use anyhow::{anyhow, Result};
use ethereum_types::H256;
use std::collections::HashMap;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::thread;

use crate::io_utils::{load_host_log_from_archive, load_host_log_from_path, scan_logs};
use crate::model::{AnalysisData, BlockInfo, HostBlocksLog, TxAgg};
use crate::quantile::{QuantileAgg, QuantileImpl};
use crate::stats::f64_from_stat;

fn merge_sync_gap_stats(data: &mut AnalysisData, stats: Vec<HashMap<String, serde_json::Value>>) {
    data.node_count += stats.len();
    for stat_map in stats {
        for (key, bucket) in [
            ("Avg", &mut data.sync_gap_avg),
            ("P50", &mut data.sync_gap_p50),
            ("P90", &mut data.sync_gap_p90),
            ("P99", &mut data.sync_gap_p99),
            ("Max", &mut data.sync_gap_max),
        ] {
            match f64_from_stat(&stat_map, key) {
                Some(v) => bucket.push(v),
                None => {}
            }
        }
    }
}

fn merge_host_blocks(
    data: &mut AnalysisData,
    host_blocks: HashMap<H256, crate::model::BlockJson>,
    quantile_impl: QuantileImpl,
) {
    for (block_hash, b) in host_blocks {
        let entry = data
            .blocks
            .entry(block_hash)
            .or_insert_with(BlockInfo::default);
        if entry.timestamp == 0 && b.timestamp != 0 {
            entry.timestamp = b.timestamp;
        }
        if entry.txs == 0 && b.txs != 0 {
            entry.txs = b.txs;
        }
        if entry.size == 0 && b.size != 0 {
            entry.size = b.size;
        }
        if entry.referee_count == 0 && !b.referees.is_empty() {
            entry.referee_count = b.referees.len() as i64;
        }
        let per_block = data.block_dists.entry(block_hash).or_insert_with(HashMap::new);
        for (k, vs) in b.latencies {
            let agg = per_block
                .entry(k)
                .or_insert_with(|| QuantileAgg::new(quantile_impl));
            for v in vs {
                agg.insert(v);
            }
        }
    }
}

fn merge_host_txs(data: &mut AnalysisData, host_txs: HashMap<H256, crate::model::TxJson>) {
    for (tx_hash, tx) in host_txs {
        let tx_entry = data.txs.entry(tx_hash).or_insert_with(TxAgg::default);
        let mut local_received_min: Option<f64> = None;
        for ts in tx.received_timestamps {
            tx_entry.received.push(ts as f32);
            local_received_min = Some(match local_received_min {
                None => ts,
                Some(cur) => cur.min(ts),
            });
        }

        let mut first_packed: Option<f64> = None;
        for ts in tx.packed_timestamps {
            if let Some(t) = ts {
                tx_entry.packed.push(t as f32);
                if first_packed.is_none() {
                    first_packed = Some(t);
                }
            }
        }

        for ts in tx.ready_pool_timestamps {
            if let Some(t) = ts {
                tx_entry.ready.push(t as f32);
            }
        }

        if let (Some(packed_ts), Some(min_recv)) = (first_packed, local_received_min) {
            data.tx_wait_to_be_packed.push(packed_ts - min_recv);
        }
    }
}

fn merge_host_data(data: &mut AnalysisData, host: HostBlocksLog, quantile_impl: QuantileImpl) {
    merge_sync_gap_stats(data, host.sync_cons_gap_stats);
    data.by_block_ratio.extend(host.by_block_ratio);
    merge_host_blocks(data, host.blocks, quantile_impl);
    merge_host_txs(data, host.txs);
}

#[derive(Debug, Clone)]
enum LogSource {
    Plain(PathBuf),
    Archive(PathBuf),
}

fn load_source(source: &LogSource) -> Result<HostBlocksLog> {
    match source {
        LogSource::Plain(p) => load_host_log_from_path(p),
        LogSource::Archive(p) => load_host_log_from_archive(p),
    }
}

fn collect_sources(log_path: &Path) -> Result<Vec<LogSource>> {
    let (blocks_logs, archives) = scan_logs(log_path)?;
    if blocks_logs.is_empty() && archives.is_empty() {
        return Err(anyhow!(
            "No host logs found under: {} (expected blocks.log files or .7z archives)",
            log_path.display()
        ));
    }

    let mut sources = Vec::with_capacity(blocks_logs.len() + archives.len());
    for p in blocks_logs {
        sources.push(LogSource::Plain(p));
    }
    for p in archives {
        sources.push(LogSource::Archive(p));
    }
    Ok(sources)
}

pub fn load_and_merge_hosts(
    log_path: &Path,
    data: &mut AnalysisData,
    quantile_impl: QuantileImpl,
) -> Result<()> {
    let sources = collect_sources(log_path)?;
    let mut host_processed: usize = 0;
    let total_hosts = sources.len();

    let mut worker_count = thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .max(1)
        .min(8)
        .min(total_hosts.max(1));
    if let Ok(override_workers) = std::env::var("STAT_LATENCY_WORKERS") {
        if let Ok(n) = override_workers.parse::<usize>() {
            worker_count = n.max(1).min(total_hosts.max(1));
        }
    }

    if worker_count == 1 {
        for source in &sources {
            let host = load_source(source)?;
            merge_host_data(data, host, quantile_impl);
            host_processed += 1;
            if host_processed % 100 == 0 {
                eprintln!("processed {}/{} hosts...", host_processed, total_hosts);
            }
        }
        return Ok(());
    }

    let shared_sources = Arc::new(sources);
    let next_index = Arc::new(AtomicUsize::new(0));
    let (tx, rx) = mpsc::sync_channel::<Result<HostBlocksLog>>(worker_count * 2);
    let mut handles = Vec::with_capacity(worker_count);

    for _ in 0..worker_count {
        let tx = tx.clone();
        let shared_sources = Arc::clone(&shared_sources);
        let next_index = Arc::clone(&next_index);
        handles.push(thread::spawn(move || {
            loop {
                let idx = next_index.fetch_add(1, Ordering::Relaxed);
                if idx >= shared_sources.len() {
                    break;
                }
                if tx.send(load_source(&shared_sources[idx])).is_err() {
                    break;
                }
            }
        }));
    }
    drop(tx);

    for result in rx {
        let host = result?;
        merge_host_data(data, host, quantile_impl);
        host_processed += 1;
        if host_processed % 100 == 0 {
            eprintln!("processed {}/{} hosts...", host_processed, total_hosts);
        }
        if host_processed == total_hosts {
            break;
        }
    }

    for handle in handles {
        let _ = handle.join();
    }

    Ok(())
}

pub fn validate_and_filter_blocks(data: &mut AnalysisData, max_blocks: Option<usize>) {
    let mut removed_blocks: Vec<H256> = Vec::new();
    for (block_hash, per_key) in &data.block_dists {
        if let Some(sync) = per_key.get("Sync") {
            if sync.count as usize != data.node_count {
                removed_blocks.push(*block_hash);
            }
        } else {
            removed_blocks.push(*block_hash);
        }
    }

    for h in &removed_blocks {
        if let Some(per_key) = data.block_dists.get(h) {
            let sync_cnt = per_key.get("Sync").map(|a| a.count).unwrap_or(0);
            println!(
                "sync graph missed block {}: received = {}, total = {}",
                format!("{:#x}", h),
                sync_cnt,
                data.node_count
            );
        }
        data.block_dists.remove(h);
        data.blocks.remove(h);
    }

    if let Some(n) = max_blocks {
        let mut pairs: Vec<(H256, i64)> = data
            .blocks
            .iter()
            .map(|(h, b)| (*h, b.timestamp))
            .collect();
        pairs.sort_by(|a, b| a.1.cmp(&b.1));
        if pairs.len() > n {
            let keep: std::collections::HashSet<H256> =
                pairs.into_iter().take(n).map(|p| p.0).collect();
            data.blocks.retain(|h, _| keep.contains(h));
            data.block_dists.retain(|h, _| keep.contains(h));
            println!(
                "Limiting analysis to earliest {} blocks (remaining blocks: {})",
                n,
                data.blocks.len()
            );
        }
    }
}
