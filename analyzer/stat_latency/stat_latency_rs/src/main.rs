use anyhow::{anyhow, Context, Result};
use clap::Parser;
use prettytable::{Cell, Row, Table};
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::ffi::OsStr;
use std::fs;
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};

use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(about = "Analyze Conflux massive-test latency logs (memory-optimized)")]
struct Args {
    /// Log directory containing host subdirs with blocks.log or output*.7z
    #[arg(short = 'l', long = "log-path")]
    log_path: PathBuf,

    /// Only analyze the earliest N blocks (optional)
    #[arg(short = 'n', long = "max-blocks")]
    max_blocks: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct HostBlocksLog {
    #[serde(default)]
    blocks: HashMap<String, BlockJson>,
    #[serde(default)]
    txs: HashMap<String, TxJson>,
    #[serde(default)]
    sync_cons_gap_stats: Vec<HashMap<String, serde_json::Value>>,
    #[serde(default)]
    by_block_ratio: Vec<f64>,
}

#[derive(Debug, Deserialize, Default)]
struct BlockJson {
    #[serde(default)]
    timestamp: i64,
    #[serde(default)]
    txs: i64,
    #[serde(default)]
    size: i64,
    #[serde(default)]
    referees: Vec<String>,
    #[serde(default)]
    latencies: HashMap<String, Vec<f64>>,
}

#[derive(Debug, Deserialize, Default)]
struct TxJson {
    #[serde(default)]
    received_timestamps: Vec<f64>,
    #[serde(default)]
    packed_timestamps: Vec<Option<f64>>,
    #[serde(default)]
    ready_pool_timestamps: Vec<Option<f64>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum NodePercentile {
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
    fn all_in_order() -> &'static [NodePercentile] {
        use NodePercentile::*;
        &[Min, Avg, P10, P30, P50, P80, P90, P95, P99, P999, Max]
    }

    fn name(self) -> &'static str {
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

    fn q(self) -> Option<f64> {
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

/// Streaming quantile estimator using the P² algorithm (one quantile per instance).
///
/// This intentionally uses constant memory regardless of sample size.
#[derive(Debug, Clone)]
struct P2Quantile {
    p: f64,
    count: usize,
    init: Vec<f64>,
    q: [f64; 5],
    n: [i32; 5],
    np: [f64; 5],
    dn: [f64; 5],
}

impl P2Quantile {
    fn new(p: f64) -> Self {
        Self {
            p,
            count: 0,
            init: Vec::with_capacity(5),
            q: [0.0; 5],
            n: [0; 5],
            np: [0.0; 5],
            dn: [0.0; 5],
        }
    }

    fn insert(&mut self, x: f64) {
        if self.count < 5 {
            self.init.push(x);
            self.count += 1;
            if self.count == 5 {
                self.init.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                for i in 0..5 {
                    self.q[i] = self.init[i];
                    self.n[i] = (i as i32) + 1;
                }
                let p = self.p;
                self.np = [1.0, 1.0 + 2.0 * p, 1.0 + 4.0 * p, 3.0 + 2.0 * p, 5.0];
                self.dn = [0.0, p / 2.0, p, (1.0 + p) / 2.0, 1.0];
            }
            return;
        }

        self.count += 1;

        // Find cell k.
        let k: usize;
        if x < self.q[0] {
            self.q[0] = x;
            k = 0;
        } else if x < self.q[1] {
            k = 0;
        } else if x < self.q[2] {
            k = 1;
        } else if x < self.q[3] {
            k = 2;
        } else if x <= self.q[4] {
            k = 3;
        } else {
            self.q[4] = x;
            k = 3;
        }

        // Increment positions.
        for i in (k + 1)..5 {
            self.n[i] += 1;
        }
        for i in 0..5 {
            self.np[i] += self.dn[i];
        }

        // Adjust heights of markers 2..4.
        for i in 1..4 {
            let d = self.np[i] - (self.n[i] as f64);
            if (d >= 1.0 && (self.n[i + 1] - self.n[i]) > 1)
                || (d <= -1.0 && (self.n[i - 1] - self.n[i]) < -1)
            {
                let ds: i32 = if d >= 0.0 { 1 } else { -1 };
                let n_im1 = self.n[i - 1] as f64;
                let n_i = self.n[i] as f64;
                let n_ip1 = self.n[i + 1] as f64;

                let q_im1 = self.q[i - 1];
                let q_i = self.q[i];
                let q_ip1 = self.q[i + 1];

                let numerator = (ds as f64)
                    * ((n_i - n_im1 + (ds as f64)) * (q_ip1 - q_i) / (n_ip1 - n_i)
                        + (n_ip1 - n_i - (ds as f64)) * (q_i - q_im1) / (n_i - n_im1));
                let q_parabolic = q_i + numerator / (n_ip1 - n_im1);

                // If parabolic prediction is unreasonable, use linear.
                let q_new = if q_parabolic > q_im1 && q_parabolic < q_ip1 {
                    q_parabolic
                } else {
                    let j = (i as i32 + ds) as usize;
                    q_i + (ds as f64) * (self.q[j] - q_i) / ((self.n[j] - self.n[i]) as f64)
                };

                self.q[i] = q_new;
                self.n[i] += ds;
            }
        }
    }

    fn estimate(&self) -> f64 {
        if self.count == 0 {
            return f64::NAN;
        }
        if self.count < 5 {
            let mut tmp = self.init.clone();
            tmp.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            let idx = ((tmp.len() - 1) as f64 * self.p).round() as usize;
            return tmp[idx.min(tmp.len() - 1)];
        }
        // Marker 3 approximates the p-quantile.
        self.q[2]
    }
}

#[derive(Debug, Clone)]
struct QuantileAgg {
    count: u32,
    sum: f64,
    min: f64,
    max: f64,
    p10: P2Quantile,
    p30: P2Quantile,
    p50: P2Quantile,
    p80: P2Quantile,
    p90: P2Quantile,
    p95: P2Quantile,
    p99: P2Quantile,
    p999: P2Quantile,
}

impl QuantileAgg {
    fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            p10: P2Quantile::new(0.1),
            p30: P2Quantile::new(0.3),
            p50: P2Quantile::new(0.5),
            p80: P2Quantile::new(0.8),
            p90: P2Quantile::new(0.9),
            p95: P2Quantile::new(0.95),
            p99: P2Quantile::new(0.99),
            p999: P2Quantile::new(0.999),
        }
    }

    fn insert(&mut self, x: f64) {
        if x.is_nan() {
            return;
        }
        self.count += 1;
        self.sum += x;
        if x < self.min {
            self.min = x;
        }
        if x > self.max {
            self.max = x;
        }
        self.p10.insert(x);
        self.p30.insert(x);
        self.p50.insert(x);
        self.p80.insert(x);
        self.p90.insert(x);
        self.p95.insert(x);
        self.p99.insert(x);
        self.p999.insert(x);
    }

    fn value_for(&self, p: NodePercentile) -> f64 {
        match p {
            NodePercentile::Min => self.min,
            NodePercentile::Max => self.max,
            NodePercentile::Avg => {
                if self.count == 0 {
                    f64::NAN
                } else {
                    (self.sum / (self.count as f64) * 100.0).round() / 100.0
                }
            }
            NodePercentile::P10 => self.p10.estimate(),
            NodePercentile::P30 => self.p30.estimate(),
            NodePercentile::P50 => self.p50.estimate(),
            NodePercentile::P80 => self.p80.estimate(),
            NodePercentile::P90 => self.p90.estimate(),
            NodePercentile::P95 => self.p95.estimate(),
            NodePercentile::P99 => self.p99.estimate(),
            NodePercentile::P999 => self.p999.estimate(),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct BlockInfo {
    timestamp: i64,
    txs: i64,
    size: i64,
    referee_count: i64,
}

#[derive(Debug, Default)]
struct TxAgg {
    received: Vec<f32>,
    packed: Vec<f32>,
    ready: Vec<f32>,
}

fn default_latency_key_names() -> HashSet<&'static str> {
    let mut set = HashSet::new();
    // BlockLatencyType
    set.insert("Receive");
    set.insert("Sync");
    set.insert("Cons");

    // BlockEventRecordType
    set.insert("HeaderReady");
    set.insert("BodyReady");
    set.insert("SyncGraph");
    set.insert("ConsensusGraphStart");
    set.insert("ConsensusGraphReady");
    set.insert("ComputeEpoch");
    set.insert("NotifyTxPool");
    set.insert("TxPoolUpdated");

    set
}

fn pivot_event_key_names() -> HashSet<&'static str> {
    let mut set = HashSet::new();
    set.insert("ComputeEpoch");
    set.insert("NotifyTxPool");
    set.insert("TxPoolUpdated");
    set
}

fn scan_logs(log_dir: &Path) -> Result<(Vec<PathBuf>, Vec<PathBuf>)> {
    let mut blocks_logs = Vec::new();
    let mut dirs_with_blocks_log: HashSet<PathBuf> = HashSet::new();

    for entry in WalkDir::new(log_dir).follow_links(false) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        if entry.file_name() == OsStr::new("blocks.log") {
            let path = entry.path().to_path_buf();
            blocks_logs.push(path.clone());
            if let Some(parent) = path.parent() {
                dirs_with_blocks_log.insert(parent.to_path_buf());
            }
        }
    }

    let mut archives = Vec::new();
    for entry in WalkDir::new(log_dir).follow_links(false) {
        let entry = entry?;
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if path.extension() == Some(OsStr::new("7z")) {
            let parent = path.parent().unwrap_or(log_dir);
            if !dirs_with_blocks_log.contains(parent) {
                archives.push(path.to_path_buf());
            }
        }
    }

    blocks_logs.sort();
    archives.sort();
    Ok((blocks_logs, archives))
}

fn extract_blocks_log_from_7z(archive_path: &Path) -> Result<Vec<u8>> {
    // Fast path: most archives in this repo store blocks.log at output0/blocks.log.
    if let Ok(bytes) = extract_member_from_7z(archive_path, "output0/blocks.log") {
        return Ok(bytes);
    }

    // Fallback: list archive and pick the shortest path ending with blocks.log.
    let mut file = fs::File::open(archive_path)
        .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
    
    let pos = file.stream_position().with_context(|| format!("failed to get stream position for {}", archive_path.display()))?;
    let len = file.seek(SeekFrom::End(0)).with_context(|| format!("failed to seek to end for {}", archive_path.display()))?;
    file.seek(SeekFrom::Start(pos)).with_context(|| format!("failed to seek to start for {}", archive_path.display()))?;
    
    let password = sevenz_rust::Password::empty();
    let mut seven = sevenz_rust::SevenZReader::new(file, len, password)
        .with_context(|| format!("failed to create 7z reader for {}", archive_path.display()))?;

    let mut candidates: Vec<String> = Vec::new();
    seven.for_each_entries(|entry, _| {
        if entry.name().ends_with("blocks.log") {
            candidates.push(entry.name().to_string());
        }
        Ok(true)
    }).with_context(|| format!("failed to iterate entries in {}", archive_path.display()))?;

    if candidates.is_empty() {
        return Err(anyhow!(
            "no blocks.log found in archive {}",
            archive_path.display()
        ));
    }

    candidates.sort_by(|a, b| {
        let la = a.len();
        let lb = b.len();
        la.cmp(&lb).then_with(|| a.cmp(b))
    });
    let member = &candidates[0];
    extract_member_from_7z(archive_path, member)
}

fn extract_member_from_7z(archive_path: &Path, member: &str) -> Result<Vec<u8>> {
    let mut file = fs::File::open(archive_path)
        .with_context(|| format!("failed to open archive {}", archive_path.display()))?;
    
    let pos = file.stream_position().with_context(|| format!("failed to get stream position for {}", archive_path.display()))?;
    let len = file.seek(SeekFrom::End(0)).with_context(|| format!("failed to seek to end for {}", archive_path.display()))?;
    file.seek(SeekFrom::Start(pos)).with_context(|| format!("failed to seek to start for {}", archive_path.display()))?;
    
    let password = sevenz_rust::Password::empty();
    let mut seven = sevenz_rust::SevenZReader::new(file, len, password)
        .with_context(|| format!("failed to create 7z reader for {}", archive_path.display()))?;

    let mut result: Option<Vec<u8>> = None;
    seven.for_each_entries(|entry, reader| {
        if entry.name() == member {
            let mut out = Vec::new();
            reader.read_to_end(&mut out)?;
            result = Some(out);
        }
        Ok(true)
    }).with_context(|| format!("failed to read content of {} from {}", member, archive_path.display()))?;

    result.ok_or_else(|| anyhow!(
        "member {} not found in archive {}",
        member,
        archive_path.display()
    ))
}

fn load_host_log_from_path(path: &Path) -> Result<HostBlocksLog> {
    let data = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let host: HostBlocksLog = serde_json::from_slice(&data)
        .with_context(|| format!("parse JSON from {}", path.display()))?;
    Ok(host)
}

fn load_host_log_from_archive(path: &Path) -> Result<HostBlocksLog> {
    let data = extract_blocks_log_from_7z(path)?;
    let host: HostBlocksLog = serde_json::from_slice(&data)
        .with_context(|| format!("parse JSON from {} (blocks.log in archive)", path.display()))?;
    Ok(host)
}

#[derive(Debug, Clone)]
struct Statistics {
    avg: f64,
    p10: f64,
    p30: f64,
    p50: f64,
    p80: f64,
    p90: f64,
    p95: f64,
    p99: f64,
    p999: f64,
    max: f64,
    cnt: usize,
}

fn statistics_from_sorted(data: &[f64]) -> Statistics {
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
        let idx = ((cnt - 1) as f64 * q) as usize;
        data[idx.min(cnt - 1)]
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

fn statistics_from_vec(mut data: Vec<f64>) -> Statistics {
    data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    statistics_from_sorted(&data)
}

fn f64_from_stat(map: &HashMap<String, serde_json::Value>, key: &str) -> Option<f64> {
    map.get(key).and_then(|v| v.as_f64())
}

fn main() -> Result<()> {
    let args = Args::parse();

    if !args.log_path.exists() {
        return Err(anyhow!("log path not found: {}", args.log_path.display()));
    }

    let default_keys = default_latency_key_names();
    let pivot_keys = pivot_event_key_names();

    let (blocks_logs, archives) = scan_logs(&args.log_path)?;
    if blocks_logs.is_empty() && archives.is_empty() {
        return Err(anyhow!(
            "No host logs found under: {} (expected blocks.log files or .7z archives)",
            args.log_path.display()
        ));
    }

    // Global accumulators
    let mut node_count: usize = 0;
    let mut sync_gap_avg: Vec<f64> = Vec::new();
    let mut sync_gap_p50: Vec<f64> = Vec::new();
    let mut sync_gap_p90: Vec<f64> = Vec::new();
    let mut sync_gap_p99: Vec<f64> = Vec::new();
    let mut sync_gap_max: Vec<f64> = Vec::new();

    let mut by_block_ratio: Vec<f64> = Vec::new();
    let mut tx_wait_to_be_packed: Vec<f64> = Vec::new();

    let mut blocks: HashMap<String, BlockInfo> = HashMap::new();
    let mut block_dists: HashMap<String, HashMap<String, QuantileAgg>> = HashMap::new();

    let mut txs: HashMap<String, TxAgg> = HashMap::new();
    let mut min_tx_packed_to_block_latency: Vec<f64> = Vec::new();
    let mut min_tx_to_ready_pool_latency: Vec<f64> = Vec::new();
    let mut slowest_packed_hash: Option<String> = None;
    let mut slowest_packed_latency: f64 = f64::NEG_INFINITY;

    let mut host_processed: usize = 0;
    let total_hosts = blocks_logs.len() + archives.len();

    let mut process_host = |host: HostBlocksLog| {
        // nodes
        node_count += host.sync_cons_gap_stats.len();
        for stat_map in host.sync_cons_gap_stats {
            if let Some(v) = f64_from_stat(&stat_map, "Avg") {
                sync_gap_avg.push(v);
            }
            if let Some(v) = f64_from_stat(&stat_map, "P50") {
                sync_gap_p50.push(v);
            }
            if let Some(v) = f64_from_stat(&stat_map, "P90") {
                sync_gap_p90.push(v);
            }
            if let Some(v) = f64_from_stat(&stat_map, "P99") {
                sync_gap_p99.push(v);
            }
            if let Some(v) = f64_from_stat(&stat_map, "Max") {
                sync_gap_max.push(v);
            }
        }

        // by_block_ratio
        by_block_ratio.extend(host.by_block_ratio);

        // blocks
        for (block_hash, b) in host.blocks {
            let entry = blocks.entry(block_hash.clone()).or_insert_with(BlockInfo::default);
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

            let per_block = block_dists.entry(block_hash).or_insert_with(HashMap::new);
            for (k, vs) in b.latencies {
                let agg = per_block.entry(k).or_insert_with(QuantileAgg::new);
                for v in vs {
                    agg.insert(v);
                }
            }
        }

        // txs
        for (tx_hash, tx) in host.txs {
            let tx_entry = txs.entry(tx_hash).or_insert_with(TxAgg::default);
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

            // tx_wait_to_be_packed_time (per-node sample)
            if let (Some(packed_ts), Some(min_recv)) = (first_packed, local_received_min) {
                // Replicate Python add_host: packed_ts - min(received_timestamps_of_this_host).
                tx_wait_to_be_packed.push(packed_ts - min_recv);
            }
        }
    };

    for p in blocks_logs {
        let host = load_host_log_from_path(&p)?;
        process_host(host);
        host_processed += 1;
        if host_processed % 100 == 0 {
            eprintln!("processed {}/{} hosts...", host_processed, total_hosts);
        }
    }

    for p in archives {
        let host = load_host_log_from_archive(&p)?;
        process_host(host);
        host_processed += 1;
        if host_processed % 100 == 0 {
            eprintln!("processed {}/{} hosts...", host_processed, total_hosts);
        }
    }

    if node_count == 0 {
        return Err(anyhow!("no nodes found (sync_cons_gap_stats empty)"));
    }

    // Validate blocks: remove blocks missing Sync for any node.
    let mut removed_blocks: Vec<String> = Vec::new();
    for (block_hash, per_key) in &block_dists {
        if let Some(sync) = per_key.get("Sync") {
            if sync.count as usize != node_count {
                removed_blocks.push(block_hash.clone());
            }
        } else {
            removed_blocks.push(block_hash.clone());
        }
    }

    for h in &removed_blocks {
        // Match Python's behavior (prints per missing block)
        if let Some(per_key) = block_dists.get(h) {
            let sync_cnt = per_key.get("Sync").map(|a| a.count).unwrap_or(0);
            println!(
                "sync graph missed block {}: received = {}, total = {}",
                h, sync_cnt, node_count
            );
        }
        block_dists.remove(h);
        blocks.remove(h);
    }

    // Apply max_blocks (earliest N by timestamp)
    if let Some(n) = args.max_blocks {
        let mut pairs: Vec<(String, i64)> = blocks
            .iter()
            .map(|(h, b)| (h.clone(), b.timestamp))
            .collect();
        pairs.sort_by(|a, b| a.1.cmp(&b.1));
        if pairs.len() > n {
            let keep: HashSet<String> = pairs.into_iter().take(n).map(|p| p.0).collect();
            blocks.retain(|h, _| keep.contains(h));
            block_dists.retain(|h, _| keep.contains(h));
            println!(
                "Limiting analysis to earliest {} blocks (remaining blocks: {})",
                n,
                blocks.len()
            );
        }
    }

    println!("{} nodes in total", node_count);
    println!("{} blocks generated", blocks.len());

    // Validate txs similar to Python
    let mut missing_tx = 0usize;
    let mut unpacked_tx = 0usize;
    for tx in txs.values() {
        if tx.received.len() != node_count {
            missing_tx += 1;
        }
        if tx.packed.is_empty() {
            unpacked_tx += 1;
        }
        if !tx.packed.is_empty() {
            let min_recv = tx
                .received
                .iter()
                .copied()
                .fold(f32::INFINITY, f32::min) as f64;
            let min_packed = tx.packed.iter().copied().fold(f32::INFINITY, f32::min) as f64;
            let latency = min_packed - min_recv;
            min_tx_packed_to_block_latency.push(latency);
            if latency > slowest_packed_latency {
                slowest_packed_latency = latency;
                // NOTE: we don’t keep hashes in TxAgg; slowest hash reported only when available.
                // We set it later in a second pass below.
            }
        }
        if !tx.ready.is_empty() {
            let min_recv = tx
                .received
                .iter()
                .copied()
                .fold(f32::INFINITY, f32::min) as f64;
            let min_ready = tx.ready.iter().copied().fold(f32::INFINITY, f32::min) as f64;
            min_tx_to_ready_pool_latency.push(min_ready - min_recv);
        }
    }

    println!("Removed tx count (txs have not fully propagated) {}", missing_tx);
    println!("Unpacked tx count {}", unpacked_tx);
    println!("Total tx count {}", txs.len());

    // Determine slowest packed tx hash (exactly like Python argmax over min packed latency)
    if !min_tx_packed_to_block_latency.is_empty() {
        let mut best: Option<(&String, f64)> = None;
        for (h, tx) in &txs {
            if tx.packed.is_empty() {
                continue;
            }
            let min_recv = tx
                .received
                .iter()
                .copied()
                .fold(f32::INFINITY, f32::min) as f64;
            let min_packed = tx.packed.iter().copied().fold(f32::INFINITY, f32::min) as f64;
            let latency = min_packed - min_recv;
            match best {
                None => best = Some((h, latency)),
                Some((_, cur)) if latency > cur => best = Some((h, latency)),
                _ => {}
            }
        }
        if let Some((h, _)) = best {
            slowest_packed_hash = Some(h.clone());
        }
    }

    // Build row data: metric -> Vec(values across blocks/txs/etc)
    let mut row_values: HashMap<String, Vec<f64>> = HashMap::new();

    // Helper to push values.
    let mut push_row = |key: String, v: f64| {
        row_values.entry(key).or_insert_with(Vec::new).push(v);
    };

    // Prepare custom key list.
    let mut custom_keys: BTreeSet<String> = BTreeSet::new();
    for per_key in block_dists.values() {
        for k in per_key.keys() {
            if !default_keys.contains(k.as_str()) {
                custom_keys.insert(k.clone());
            }
        }
    }

    let require_90pct = |k: &str, is_default: bool| -> bool {
        if is_default {
            pivot_keys.contains(k)
        } else {
            true
        }
    };

    // Per-block latency stats -> per-row values.
    for (block_hash, per_key) in &block_dists {
        let _ = block_hash;
        for (k, agg) in per_key {
            let is_default = default_keys.contains(k.as_str());
            if require_90pct(k, is_default) {
                let threshold = (0.9 * (node_count as f64)).floor() as u32;
                if agg.count < threshold {
                    continue;
                }
            }

            for p in NodePercentile::all_in_order() {
                let v = agg.value_for(*p);
                let row_key = format!("{}::{p_name}", k, p_name = p.name());
                push_row(row_key, v);
            }
        }
    }

    // Tx broadcast latency rows: tx broadcast latency (P(n))
    // Need per-tx node-latencies distribution (exact; tx sample count is manageable).
    for p in NodePercentile::all_in_order() {
        // skip Min: Python includes it in node_percentiles, but table rows include it.
        let _ = p;
    }

    // Gather per-tx stats across txs.
    let mut tx_latency_rows: HashMap<NodePercentile, Vec<f64>> = HashMap::new();
    let mut tx_packed_rows: HashMap<NodePercentile, Vec<f64>> = HashMap::new();

    for tx in txs.values() {
        if tx.received.len() == node_count {
            let min_recv = tx
                .received
                .iter()
                .copied()
                .fold(f32::INFINITY, f32::min) as f64;
            let mut latencies: Vec<f64> = tx
                .received
                .iter()
                .map(|t| (*t as f64) - min_recv)
                .collect();
            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

            // Build node-level exact stats.
            let pick = |q: f64| -> f64 {
                let idx = ((latencies.len() - 1) as f64 * q) as usize;
                latencies[idx.min(latencies.len() - 1)]
            };
            let sum: f64 = latencies.iter().sum();
            let avg = (sum / (latencies.len() as f64) * 100.0).round() / 100.0;

            for p in NodePercentile::all_in_order() {
                let v = match p {
                    NodePercentile::Min => *latencies.first().unwrap(),
                    NodePercentile::Max => *latencies.last().unwrap(),
                    NodePercentile::Avg => avg,
                    _ => pick(p.q().unwrap()),
                };
                tx_latency_rows.entry(*p).or_insert_with(Vec::new).push(v);
            }
        }

        if !tx.packed.is_empty() {
            let min_recv = tx
                .received
                .iter()
                .copied()
                .fold(f32::INFINITY, f32::min) as f64;
            let mut latencies: Vec<f64> = tx
                .packed
                .iter()
                .map(|t| (*t as f64) - min_recv)
                .collect();
            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));

            let pick = |q: f64| -> f64 {
                let idx = ((latencies.len() - 1) as f64 * q) as usize;
                latencies[idx.min(latencies.len() - 1)]
            };
            let sum: f64 = latencies.iter().sum();
            let avg = (sum / (latencies.len() as f64) * 100.0).round() / 100.0;

            for p in NodePercentile::all_in_order() {
                let v = match p {
                    NodePercentile::Min => *latencies.first().unwrap(),
                    NodePercentile::Max => *latencies.last().unwrap(),
                    NodePercentile::Avg => avg,
                    _ => pick(p.q().unwrap()),
                };
                tx_packed_rows.entry(*p).or_insert_with(Vec::new).push(v);
            }
        }
    }

    // Block-derived scalar lists
    let mut block_txs: Vec<f64> = Vec::new();
    let mut block_size: Vec<f64> = Vec::new();
    let mut block_referees: Vec<f64> = Vec::new();
    let mut block_timestamps: Vec<i64> = Vec::new();
    let mut max_time: i64 = 0;
    let mut min_time: i64 = i64::MAX;

    for b in blocks.values() {
        block_txs.push(b.txs as f64);
        block_size.push(b.size as f64);
        block_referees.push(b.referee_count as f64);
        block_timestamps.push(b.timestamp);
        if b.txs > 0 {
            if b.timestamp < min_time {
                min_time = b.timestamp;
            }
            if b.timestamp > max_time {
                max_time = b.timestamp;
            }
        }
    }

    block_timestamps.sort();
    let mut intervals: Vec<f64> = Vec::new();
    for w in block_timestamps.windows(2) {
        intervals.push((w[1] - w[0]) as f64);
    }

    let tx_sum: i64 = blocks.values().map(|b| b.txs).sum();
    println!("{} txs generated", tx_sum);
    let duration = max_time.saturating_sub(min_time);
    if duration <= 0 {
        println!("Test duration is 0.00 seconds");
        println!("Throughput is N/A (duration is 0)");
    } else {
        println!("Test duration is {:.2} seconds", duration as f64);
        println!("Throughput is {}", (tx_sum as f64) / (duration as f64));
    }
    if let Some(h) = &slowest_packed_hash {
        println!("Slowest packed transaction hash: {}", h);
    }

    // Render the final table
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

    // Block broadcast latency rows
    for t in ["Receive", "Sync", "Cons"] {
        for p in NodePercentile::all_in_order() {
            let metric = format!("block broadcast latency ({}/{})", t, p.name());
            let key = format!("{}::{}", t, p.name());
            let stats = statistics_from_vec(row_values.remove(&key).unwrap_or_default());
            table.add_row(row_from_stats(metric, stats, Some("%.2f")));
        }
    }

    // Block event elapsed
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

    // Custom block events
    for t in &custom_keys {
        for p in NodePercentile::all_in_order() {
            let metric = format!("custom block event elapsed ({}/{})", t, p.name());
            let key = format!("{}::{}", t, p.name());
            let stats = statistics_from_vec(row_values.remove(&key).unwrap_or_default());
            table.add_row(row_from_stats(metric, stats, Some("%.2f")));
        }
    }

    // Tx rows (only if any fully propagated tx exists, to match Python's gating)
    if tx_latency_rows
        .get(&NodePercentile::Avg)
        .map(|v| !v.is_empty())
        .unwrap_or(false)
    {
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
            statistics_from_vec(min_tx_packed_to_block_latency.clone()),
            Some("%.2f"),
        ));

        table.add_row(row_from_stats(
            "min tx to ready pool latency".to_string(),
            statistics_from_vec(min_tx_to_ready_pool_latency.clone()),
            Some("%.2f"),
        ));

        table.add_row(row_from_stats(
            "by_block_ratio".to_string(),
            statistics_from_vec(by_block_ratio.clone()),
            Some("%.2f"),
        ));

        table.add_row(row_from_stats(
            "Tx wait to be packed elasped time".to_string(),
            statistics_from_vec(tx_wait_to_be_packed.clone()),
            Some("%.2f"),
        ));
    }

    table.add_row(row_from_stats(
        "block txs".to_string(),
        statistics_from_vec(block_txs),
        None,
    ));
    table.add_row(row_from_stats(
        "block size".to_string(),
        statistics_from_vec(block_size),
        None,
    ));
    table.add_row(row_from_stats(
        "block referees".to_string(),
        statistics_from_vec(block_referees),
        None,
    ));
    table.add_row(row_from_stats(
        "block generation interval".to_string(),
        statistics_from_vec(intervals),
        Some("%.2f"),
    ));

    // sync/cons gap rows
    table.add_row(row_from_stats(
        "node sync/cons gap (Avg)".to_string(),
        statistics_from_vec(sync_gap_avg),
        None,
    ));
    table.add_row(row_from_stats(
        "node sync/cons gap (P50)".to_string(),
        statistics_from_vec(sync_gap_p50),
        None,
    ));
    table.add_row(row_from_stats(
        "node sync/cons gap (P90)".to_string(),
        statistics_from_vec(sync_gap_p90),
        None,
    ));
    table.add_row(row_from_stats(
        "node sync/cons gap (P99)".to_string(),
        statistics_from_vec(sync_gap_p99),
        None,
    ));
    table.add_row(row_from_stats(
        "node sync/cons gap (Max)".to_string(),
        statistics_from_vec(sync_gap_max),
        None,
    ));

    table.printstd();

    Ok(())
}

fn row_from_stats(name: String, s: Statistics, fmt: Option<&str>) -> Row {
    // fmt is only used to decide float formatting style; we keep output close to Python.
    let f = |v: f64| -> String {
        if v.is_nan() {
            return "nan".to_string();
        }
        match fmt {
            Some("%.2f") => format!("{:.2}", v),
            _ => {
                // Default: keep 2 decimals for avg; others as integer-ish if close.
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
