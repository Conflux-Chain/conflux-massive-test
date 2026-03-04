use std::collections::{BTreeSet, HashMap};

use anyhow::bail;
use ethereum_types::H256;

use crate::{
    block::Block,
    graph::Graph,
    utils::{bitmap::Bitmap, time_series::TimeSeries},
};

pub struct GraphComputer(Graph);

impl GraphComputer {
    pub fn new(graph: Graph) -> Self { Self(graph) }

    pub fn finalize(mut self) -> anyhow::Result<Graph> {
        self.check_block_hash()?;

        let root_hash = self.0.root_hash();

        self.set_parent();

        self.apply_block(&root_hash, |g, b| {
            g.calculate_subtree_size(b);
        });

        self.apply_block(&root_hash, |g, b| g.sort_children(b));

        let pivot_hashes: Vec<_> = self.0.pivot_chain().into_iter().map(|b| b.hash).collect();
        for pivot_hash in pivot_hashes {
            self.apply_block(&pivot_hash, |g, b| {
                g.mark_epoch(b, pivot_hash);
            });
        }

        self.set_block_by_map(self.compute_past_set_bitmap(), |block, bitmap| {
            block.past_set_size = bitmap.count() as u64;
        });

        self.set_block_by_map(self.compute_subtree_adv(), |block, adv_series| {
            block.subtree_adv_series = Some(adv_series);
        });

        Ok(self.0)
    }

    fn check_block_hash(&self) -> anyhow::Result<()> {
        let graph = &self.0;
        for block in graph.block_map.values() {
            if let Some(h) = block.parent_hash {
                if h != graph.root_hash() && !graph.block_map.contains_key(&h) {
                    bail!("block hash {:?} has no block", h)
                }
            }
        }
        Ok(())
    }

    fn set_parent(&mut self) {
        let pairs: Vec<(H256, H256)> = self
            .0
            .block_map
            .iter()
            .filter_map(|(hash, block)| block.parent_hash.map(|p| (*hash, p)))
            .collect();

        for (hash, parent_hash) in pairs {
            self.0
                .block_map
                .get_mut(&parent_hash)
                .unwrap()
                .children
                .push(hash);
        }
    }

    fn calculate_subtree_size<'a>(&mut self, block: &mut Block) -> (u64, TimeSeries<u16>) {
        if block.subtree_size > 0 {
            return (
                block.subtree_size,
                block.subtree_size_series.clone().unwrap(),
            );
        }

        // Calculate subtree_size for all children first
        let mut children_sum = 1;
        let mut subtree_timeseries = if block.log_timestamp > 0 {
            vec![TimeSeries::new(block.log_timestamp, 1u16)]
        } else {
            vec![]
        };

        for child_hash in &block.children {
            self.apply_block(child_hash, |graph, child| {
                let (child_size, child_series) = graph.calculate_subtree_size(child);
                subtree_timeseries.push(child_series);
                children_sum += child_size;
            });
        }

        let mut subtree_size_series =
            TimeSeries::array_cartesian_map(&subtree_timeseries, |children_series| {
                Some(
                    children_series
                        .iter()
                        .filter_map(|x| x.copied())
                        .sum::<u16>(),
                )
            });
        subtree_size_series.reduce();

        // Current node's subtree_size = 1 + sum of all children's subtree_size
        block.subtree_size = children_sum;

        block.subtree_size_series = Some(subtree_size_series);

        (
            block.subtree_size,
            block.subtree_size_series.clone().unwrap(),
        )
    }

    fn sort_children(&mut self, block: &mut Block) {
        block.children.sort_by(|a, b| {
            let a_size = self.get_block(a).subtree_size;
            let b_size = self.get_block(b).subtree_size;
            b_size.cmp(&a_size)
        });

        for child_hash in &block.children {
            self.apply_block(child_hash, |graph, child| {
                graph.sort_children(child);
            });
        }
    }

    fn mark_epoch(&mut self, block: &mut Block, epoch_hash: H256) -> BTreeSet<H256> {
        if block.epoch_block.is_some() {
            return Default::default();
        }

        block.epoch_block = Some(epoch_hash);

        let mut epoch_set: BTreeSet<H256> = Default::default();

        for referee_hash in &block.referee_hashes {
            self.apply_block(referee_hash, |g, b| {
                epoch_set.extend(g.mark_epoch(b, epoch_hash));
            });
        }

        if block.hash == epoch_hash {
            block.epoch_set = Some(epoch_set);
            Default::default()
        } else {
            epoch_set.insert(block.hash);
            epoch_set
        }
    }

    fn compute_past_set_bitmap(&self) -> HashMap<H256, Bitmap> {
        let mut graph_bitmaps: HashMap<H256, Bitmap> = HashMap::new();
        let mut working_stack: Vec<H256> = Vec::new();
        let mut keys_iter = self.0.block_map.keys();

        loop {
            let hash = if let Some(hash) = working_stack.pop() {
                hash
            } else if let Some(hash) = keys_iter.next() {
                *hash
            } else {
                return graph_bitmaps;
            };

            if graph_bitmaps.contains_key(&hash) {
                continue;
            }

            let block = self.get_block(&hash);
            let mut bitmap_collector = PastsetCollector::new();
            for hash in block.referee_hashes.iter() {
                bitmap_collector.insert(*hash, &graph_bitmaps);
            }
            if let Some(parent_hash) = block.parent_hash {
                bitmap_collector.insert(parent_hash, &graph_bitmaps)
            }

            match bitmap_collector.into_result() {
                PastsetCollectResult::Ready(mut bitmap) => {
                    bitmap.set(block.id);
                    graph_bitmaps.insert(hash, bitmap);
                }
                PastsetCollectResult::Pending(hashes) => {
                    working_stack.push(hash);
                    working_stack.extend(hashes);
                    continue;
                }
            }
        }
    }

    fn compute_subtree_adv(&self) -> HashMap<H256, TimeSeries<i16>> {
        let mut answer: HashMap<H256, TimeSeries<i16>> = Default::default();
        for block in self.0.pivot_chain() {
            if block.children.is_empty() {
                continue;
            }

            let child_subtree_size_series: Vec<_> = block
                .children
                .iter()
                .map(|hash| self.get_block(hash).subtree_size_series.as_ref().unwrap())
                .collect();

            let subtree_adv_series =
                TimeSeries::array_cartesian_map(&child_subtree_size_series, |weights| {
                    let best_child_weight = *weights[0]? as i16;

                    let max_sib_weight = weights[1..]
                        .iter()
                        .filter_map(|x| x.copied())
                        .max()
                        .unwrap_or(0) as i16;

                    Some(best_child_weight - max_sib_weight)
                });

            answer.insert(block.hash, subtree_adv_series);
        }
        answer
    }

    fn apply_block(&mut self, hash: &H256, mut f: impl FnMut(&mut Self, &mut Block)) {
        let Some(mut block) = self.0.block_map.remove(hash) else {
            return;
        };
        f(self, &mut block);
        self.0.block_map.insert(*hash, block);
    }

    fn set_block_by_map<T>(
        &mut self, mut map: HashMap<H256, T>, set_block: impl Fn(&mut Block, T),
    ) {
        for (hash, block) in self.0.block_map.iter_mut() {
            if let Some(val) = map.remove(hash) {
                set_block(block, val);
            }
        }
    }

    fn get_block(&self, hash: &H256) -> &Block { self.0.block_map.get(hash).unwrap() }
}

enum PastsetCollector<'a> {
    ReadyBitmaps(Vec<&'a Bitmap>),
    PendingHashes(Vec<H256>),
}

enum PastsetCollectResult {
    Ready(Bitmap),
    Pending(Vec<H256>),
}

impl<'a> PastsetCollector<'a> {
    pub fn new() -> Self { Self::ReadyBitmaps(vec![]) }

    pub fn insert(&mut self, hash: H256, graph_bitmaps: &'a HashMap<H256, Bitmap>) {
        use PastsetCollector::*;
        match (&mut *self, graph_bitmaps.get(&hash)) {
            (ReadyBitmaps(ref mut bitmaps), Some(bitmap)) => {
                bitmaps.push(bitmap);
            }
            (ReadyBitmaps(_), None) => {
                *self = PendingHashes(vec![hash]);
            }
            (PendingHashes(ref mut hashes), None) => {
                hashes.push(hash);
            }
            (PendingHashes(_), Some(_)) => {}
        }
    }

    pub fn into_result(self) -> PastsetCollectResult {
        use PastsetCollectResult::*;

        match self {
            PastsetCollector::ReadyBitmaps(bitmaps) => {
                Ready(bitmaps.iter().copied().fold(Bitmap::new(), |mut acc, e| {
                    acc.combine(e);
                    acc
                }))
            }
            PastsetCollector::PendingHashes(hashes) => Pending(hashes),
        }
    }
}
