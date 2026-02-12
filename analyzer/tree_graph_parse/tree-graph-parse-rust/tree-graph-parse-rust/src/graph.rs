use anyhow::bail;
use ethereum_types::H256;
use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, Write},
};

use crate::{
    block::Block, graph_computer::GraphComputer, load, math::normal_confirmation_risk,
    utils::time_series::TimeSeries,
};

#[allow(dead_code)]
pub struct Graph {
    pub(super) block_map: HashMap<H256, Block>,
    pub(super) root_hash: H256,
}

impl Graph {
    pub fn load(file_or_path: &str) -> Result<Self, anyhow::Error> {
        let reader = load::open_conflux_log(file_or_path)?;

        let mut root_hash: Option<H256> = None;
        let mut block_map: HashMap<H256, Block> = Default::default();

        let mut next_id = 1;

        for line in reader.lines() {
            let line = line?;
            if !line.contains("new block inserted into graph") {
                continue;
            }
            let block = Block::parse_log_line(&line, next_id);
            next_id += 1;

            if block.height != 1 {
                block_map.insert(block.hash, block);
                continue;
            }

            let Some(parent_hash) = block.parent_hash else {
                bail!("block {:?} has no parent hash", block.hash)
            };

            match root_hash.as_ref() {
                Some(&h) if h != parent_hash => {
                    bail!("Inconsistent genesis hash");
                }
                None => {
                    root_hash = Some(parent_hash);
                    block_map.insert(parent_hash, Block::genesis_block(parent_hash));
                }
                _ => {}
            }

            block_map.insert(block.hash, block);
        }

        let Some(root_hash) = root_hash else {
            bail!("No root hash");
        };

        let unready_graph = GraphComputer::new(Self {
            block_map,
            root_hash,
        });
        unready_graph.finalize()
    }

    pub fn blocks(&self) -> impl Iterator<Item = &Block> + '_ { self.block_map.values() }

    pub fn genesis_block(&self) -> &Block { self.block_map.get(&self.root_hash).unwrap() }

    pub fn root_hash(&self) -> H256 { self.root_hash }

    pub fn get_block(&self, hash: &H256) -> Option<&Block> { self.block_map.get(hash) }

    pub fn get_block_mut(&mut self, hash: &H256) -> Option<&mut Block> {
        self.block_map.get_mut(hash)
    }

    pub fn get_parent(&self, block: &Block) -> Option<&Block> {
        block.parent_hash.map(|h| self.get_block(&h).unwrap())
    }

    pub fn pivot_chain(&self) -> Vec<&Block> {
        let mut chain = Vec::new();
        let mut current = self.genesis_block();

        loop {
            chain.push(current);
            let Some(child_hash) = current.max_child() else {
                break;
            };
            current = self.block_map.get(&child_hash).unwrap();
        }

        chain
    }

    pub fn get_referees(&self, block: &Block) -> Vec<&Block> {
        block
            .referee_hashes
            .iter()
            .map(|hash| self.get_block(hash).unwrap())
            .collect()
    }

    pub fn epoch_span(&self, block: &Block) -> u64 {
        let mut min_timestamp = u64::MAX;
        self.iter_epochs(block, |b| min_timestamp = min_timestamp.min(b.timestamp));
        block.timestamp - min_timestamp
    }

    pub fn avg_epoch_time(&self, block: &Block) -> f64 {
        let mut timestamp_sum = 0.;
        self.iter_epochs(block, |b| {
            timestamp_sum += (block.timestamp - b.timestamp) as f64;
        });
        timestamp_sum / block.epoch_size() as f64
    }

    pub fn avg_confirm_time(&self, adv_percent: usize, risk_threshold: f64) -> (f64, u64) {
        let mut total_confirm_time = 0.;
        let mut block_cnt = 0;
        for block in self.pivot_chain() {
            if block.height == 0 {
                continue;
            }

            let Some((time_elapsed, ..)) =
                self.confirmation_risk(block, adv_percent, risk_threshold)
            else {
                continue;
            };

            total_confirm_time +=
                (time_elapsed as f64 + self.avg_epoch_time(block)) * block.epoch_size() as f64;
            block_cnt += block.epoch_size();
        }
        (total_confirm_time / block_cnt as f64, block_cnt as u64)
    }

    fn iter_epochs(&self, block: &Block, mut visitor: impl FnMut(&Block)) {
        assert!(block.epoch_block.is_some());
        if let Some(set) = block.epoch_set.as_ref() {
            for h in set.iter() {
                visitor(self.get_block(h).unwrap());
            }
        }
        visitor(block)
    }

    pub fn export_edges(&self, filename: &str) -> Result<(), anyhow::Error> {
        let mut edges = Vec::new();
        for (_, block) in &self.block_map {
            if let Some(parent_hash) = &block.parent_hash {
                edges.push((parent_hash.clone(), block.hash.clone()));
            }
        }

        let mut file = File::create(filename)?;
        for (parent, child) in edges {
            writeln!(file, "{},{}", parent, child)?;
        }
        Ok(())
    }

    pub fn export_indices(&self, filename: &str) -> Result<(), anyhow::Error> {
        let mut file = File::create(filename)?;
        for (idx, hash) in self.block_map.keys().enumerate() {
            writeln!(file, "{},{}", hash, idx)?;
        }
        Ok(())
    }
}

mod confirmation {
    use super::*;

    impl Graph {
        pub fn confirmation_risk(
            &self, block: &Block, adv_percent: usize, risk_threshold: f64,
        ) -> Option<(u64, u64, u64, f64)> {
            let &(confirm_time_offset, risk) = self
                .confirmation_risk_series(block, adv_percent)
                .iter()
                .find(|(_, risk)| *risk < risk_threshold as f32)?;

            let confirm_time = block.timestamp + confirm_time_offset;

            let parent = self.get_parent(block).unwrap();

            let total_blocks = self.genesis_block().subtree_size_series.as_ref().unwrap();
            let sib_adv_blocks = parent.subtree_adv_series.as_ref().unwrap();

            let total_block = *total_blocks.at(confirm_time).unwrap() as u64;
            let m = total_block + 1 - parent.past_set_size as u64;
            let k = *sib_adv_blocks.at(confirm_time).unwrap() as u64;
            Some((confirm_time_offset, m, k, risk as f64))
        }

        pub fn confirmation_risk_series(
            &self, block: &Block, adv_percent: usize,
        ) -> Vec<(u64, f32)> {
            let parent = self.get_parent(block).unwrap();
            let total_blocks = self.genesis_block().subtree_size_series.as_ref().unwrap();
            let sib_adv_blocks = parent.subtree_adv_series.as_ref().unwrap();
            let mut confirmation_series =
                TimeSeries::tuple_cartesian_map(total_blocks, sib_adv_blocks, |total, sib_adv| {
                    if *sib_adv? <= 0 {
                        return Some(1.);
                    }
                    let m = *total? as usize + 1 - parent.past_set_size as usize;
                    let n = *sib_adv? as usize;
                    Some(normal_confirmation_risk(adv_percent, m, n).max(1e-12))
                });

            confirmation_series.reduce();

            confirmation_series
                .iter()
                .skip_while(|(_, risk)| **risk >= 0.5)
                .map(|(ts, risk)| (ts - block.timestamp, *risk))
                .collect()
        }
    }
}
