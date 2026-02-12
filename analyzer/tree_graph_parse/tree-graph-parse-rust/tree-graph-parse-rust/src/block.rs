use chrono::{DateTime, Utc};
use ethereum_types::H256;
use std::{collections::BTreeSet, str::FromStr};

use crate::{graph::Graph, utils::time_series::TimeSeries};

macro_rules! regex {
    ($pattern:expr) => {{
        use regex::Regex;
        use std::sync::OnceLock;

        static REGEX: OnceLock<Regex> = OnceLock::new();
        REGEX.get_or_init(|| Regex::new($pattern).unwrap())
    }};
}

#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub struct Block {
    pub id: usize,
    pub height: u64,
    pub hash: H256,
    pub parent_hash: Option<H256>,
    pub referee_hashes: BTreeSet<H256>,
    pub timestamp: u64,
    pub log_timestamp: u64,
    pub tx_count: u64,
    pub block_size: u64,

    // Lazy computed fields
    pub children: Vec<H256>,

    pub epoch_block: Option<H256>,
    pub epoch_set: Option<BTreeSet<H256>>,

    pub past_set_size: u64,

    pub subtree_size: u64,
    pub subtree_size_series: Option<TimeSeries<u16>>,
    pub subtree_adv_series: Option<TimeSeries<i16>>,
}

impl Block {
    pub(super) fn new(
        height: u64, hash: H256, parent_hash: H256, referee_hashes: BTreeSet<H256>, timestamp: u64,
        log_timestamp: u64, tx_count: u64, block_size: u64, id: usize,
    ) -> Self {
        Block {
            id,
            height,
            hash,
            parent_hash: Some(parent_hash),
            referee_hashes,
            timestamp,
            log_timestamp,
            tx_count,
            block_size,
            subtree_size: 0,
            subtree_size_series: None,
            epoch_block: None,
            children: Vec::new(),
            epoch_set: None,
            past_set_size: 0,
            subtree_adv_series: None,
        }
    }

    pub(super) fn genesis_block(hash: H256) -> Self {
        Block {
            id: 0,
            hash,
            ..Default::default()
        }
    }

    pub(super) fn parse_log_line(line: &str, id: usize) -> Self {
        let log_time_caps =
            regex!(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2}|Z)")
                .captures(line)
                .unwrap();
        let log_time_str = &log_time_caps[0];
        let log_timestamp = DateTime::parse_from_rfc3339(log_time_str)
            .unwrap()
            .with_timezone(&Utc)
            .timestamp() as u64;

        // Parse height
        let height_caps = regex!(r"height: (\d+)").captures(line).unwrap();
        let height = height_caps[1].parse::<u64>().unwrap();

        // Parse hash
        let hash_caps = regex!(r"hash: Some\((0x[a-f0-9]+)\)")
            .captures(line)
            .unwrap();
        let block_hash = H256::from_str(hash_caps[1].as_ref()).unwrap();

        // Parse parent hash
        let parent_caps = regex!(r"parent_hash: (0x[a-f0-9]+)")
            .captures(line)
            .unwrap();
        let parent_hash = H256::from_str(parent_caps[1].as_ref()).unwrap();

        // Parse referee hashes
        let referee_caps = regex!(r"referee_hashes: \[(.*?)\]").captures(line).unwrap();
        let referee_str = &referee_caps[1];
        let referee_hashes: BTreeSet<H256> = if !referee_str.is_empty() {
            referee_str
                .split(',')
                .map(|h| H256::from_str(h.trim()).unwrap())
                .collect()
        } else {
            Default::default()
        };

        // Parse timestamp
        let timestamp_caps = regex!(r"timestamp: (\d+)").captures(line).unwrap();
        let timestamp = timestamp_caps[1].parse::<u64>().unwrap();

        // Parse tx_count and block_size
        let tx_count_caps = regex!(r"tx_count=(\d+)").captures(line).unwrap();
        let tx_count = tx_count_caps[1].parse::<u64>().unwrap();

        let block_size_caps = regex!(r"block_size=(\d+)").captures(line).unwrap();
        let block_size = block_size_caps[1].parse::<u64>().unwrap();

        Block::new(
            height,
            block_hash,
            parent_hash,
            referee_hashes,
            timestamp,
            log_timestamp,
            tx_count,
            block_size,
            id,
        )
    }

    pub fn sib_subtree_size(&self, graph: &Graph) -> u64 {
        self.children
            .get(1)
            .map_or(0, |h| graph.get_block(h).unwrap().subtree_size)
    }

    pub fn all_sib_subtree_size(&self, graph: &Graph) -> u64 {
        self.children[1..]
            .iter()
            .map(|h| graph.get_block(h).unwrap().subtree_size)
            .sum()
    }

    pub fn max_child(&self) -> Option<H256> { self.children.first().cloned() }

    pub fn epoch_size(&self) -> usize { 1 + self.epoch_set.as_ref().map_or(0, |x| x.len()) }
}
