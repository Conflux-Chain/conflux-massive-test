use clap::{Parser, ValueEnum};
use std::path::PathBuf;

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum QuantileImplArg {
    Brute,
    Tdigest,
}

#[derive(Parser, Debug)]
#[command(about = "Analyze Conflux massive-test latency logs (memory-optimized)")]
pub struct Args {
    /// Log directory containing host subdirs with blocks.log or output*.7z
    #[arg(short = 'l', long = "log-path")]
    pub log_path: PathBuf,

    /// Only analyze the earliest N blocks (optional)
    #[arg(short = 'n', long = "max-blocks")]
    pub max_blocks: Option<usize>,

    /// Quantile implementation:
    /// brute (exact, 1.6 GB memory for 2000 hosts * 2000 blocks)
    /// tdigest (approximate and slower, very low memory; 1%+ inaccuracy for P99, max, etc.)
    #[arg(long = "quantile-impl", value_enum, default_value_t = QuantileImplArg::Brute)]
    pub quantile_impl: QuantileImplArg,
}
