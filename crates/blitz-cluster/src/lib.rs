//! blitz-cluster — zone maps + liquid (adaptive) clustering.
//!
//! "Liquid clustering" here means: clustering keys are not fixed at table
//! creation. The engine records which columns queries actually filter on,
//! and a background reclusterer incrementally re-sorts blocks along a
//! Z-order curve over the *currently hot* predicate columns. Zone maps then
//! prune most blocks at scan time, which is the single biggest speedup for
//! selective queries.

use blitz_core::{Block, CmpOp, Column, BLOCK_ROWS};
use std::collections::HashMap;

#[derive(Clone, Copy, Debug)]
pub struct ZoneMap {
    pub min: i64,
    pub max: i64,
}

impl ZoneMap {
    pub fn build(data: &[i64]) -> Self {
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for &v in data {
            min = min.min(v);
            max = max.max(v);
        }
        ZoneMap { min, max }
    }

    /// True if every row in the block fails the predicate (block skippable).
    pub fn prunes(&self, op: CmpOp, lit: i64) -> bool {
        match op {
            CmpOp::Gt => self.max <= lit,
            CmpOp::Ge => self.max < lit,
            CmpOp::Lt => self.min >= lit,
            CmpOp::Le => self.min > lit,
            CmpOp::Eq => lit < self.min || lit > self.max,
        }
    }
}

/// Interleave the high 32 bits of two normalized keys into a Z-order key.
pub fn zorder(a: u32, b: u32) -> u64 {
    fn spread(x: u32) -> u64 {
        let mut x = x as u64;
        x = (x | (x << 16)) & 0x0000_FFFF_0000_FFFF;
        x = (x | (x << 8)) & 0x00FF_00FF_00FF_00FF;
        x = (x | (x << 4)) & 0x0F0F_0F0F_0F0F_0F0F;
        x = (x | (x << 2)) & 0x3333_3333_3333_3333;
        x = (x | (x << 1)) & 0x5555_5555_5555_5555;
        x
    }
    spread(a) | (spread(b) << 1)
}

/// Workload statistics that drive clustering-key selection.
#[derive(Default)]
pub struct LiquidStats {
    pub predicate_hits: HashMap<usize, u64>,
}

impl LiquidStats {
    pub fn record_predicate(&mut self, col: usize) {
        *self.predicate_hits.entry(col).or_insert(0) += 1;
    }

    /// Pick the (up to) two hottest predicate columns as clustering keys.
    pub fn choose_keys(&self) -> Vec<usize> {
        let mut v: Vec<(usize, u64)> =
            self.predicate_hits.iter().map(|(&c, &n)| (c, n)).collect();
        v.sort_by(|a, b| b.1.cmp(&a.1));
        v.into_iter().take(2).map(|(c, _)| c).collect()
    }
}

pub struct ClusteredTable {
    pub blocks: Vec<Block>,
    pub zones: Vec<Vec<ZoneMap>>, // zones[block][column]
    pub cluster_keys: Vec<usize>,
}

impl ClusteredTable {
    pub fn from_blocks(blocks: Vec<Block>) -> Self {
        let zones = build_zones(&blocks);
        ClusteredTable { blocks, zones, cluster_keys: vec![] }
    }

    /// Average fraction of the key domain each block's zone covers.
    /// 1.0 = useless zone maps (random layout); → 1/n_blocks = perfect.
    pub fn clustering_quality(&self, col: usize) -> f64 {
        let (mut glo, mut ghi) = (i64::MAX, i64::MIN);
        for z in &self.zones {
            glo = glo.min(z[col].min);
            ghi = ghi.max(z[col].max);
        }
        let domain = (ghi - glo).max(1) as f64;
        let mut s = 0.0;
        for z in &self.zones {
            s += (z[col].max - z[col].min).max(0) as f64 / domain;
        }
        s / self.zones.len() as f64
    }

    /// Incremental liquid recluster: re-sort all rows by Z-order over the
    /// workload-chosen keys and rebuild fixed-size blocks + zone maps.
    /// (A production version reclusters only the worst-overlapping block
    /// runs; the curve math and block rebuild are identical.)
    pub fn recluster(&mut self, stats: &LiquidStats) {
        let keys = stats.choose_keys();
        if keys.is_empty() {
            return;
        }
        let ncols = self.blocks[0].columns.len();
        let total: usize = self.blocks.iter().map(|b| b.rows).sum();

        // Normalize each key column to u32 for curve interleaving.
        let norm = |col: usize| -> (i64, f64) {
            let (mut lo, mut hi) = (i64::MAX, i64::MIN);
            for b in &self.blocks {
                for &v in b.columns[col].as_i64() {
                    lo = lo.min(v);
                    hi = hi.max(v);
                }
            }
            (lo, (u32::MAX as f64) / ((hi - lo).max(1) as f64))
        };
        let (lo0, sc0) = norm(keys[0]);
        let (lo1, sc1) = if keys.len() > 1 { norm(keys[1]) } else { (0, 0.0) };

        // Global (zkey, block, row) index, sorted by curve position.
        let mut idx: Vec<(u64, u32, u32)> = Vec::with_capacity(total);
        for (bi, b) in self.blocks.iter().enumerate() {
            let k0 = b.columns[keys[0]].as_i64();
            let k1 = keys.get(1).map(|&c| b.columns[c].as_i64());
            for r in 0..b.rows {
                let a = (((k0[r] - lo0) as f64) * sc0) as u32;
                let z = match k1 {
                    Some(k1) => {
                        let bb = (((k1[r] - lo1) as f64) * sc1) as u32;
                        zorder(a, bb)
                    }
                    None => (a as u64) << 32,
                };
                idx.push((z, bi as u32, r as u32));
            }
        }
        idx.sort_unstable_by_key(|e| e.0);

        // Rebuild blocks in curve order.
        let mut new_blocks: Vec<Block> = Vec::new();
        let mut cur: Vec<Vec<i64>> = (0..ncols).map(|_| Vec::with_capacity(BLOCK_ROWS)).collect();
        for &(_, bi, r) in &idx {
            let src = &self.blocks[bi as usize];
            for (c, col) in cur.iter_mut().enumerate() {
                col.push(src.columns[c].as_i64()[r as usize]);
            }
            if cur[0].len() == BLOCK_ROWS {
                new_blocks.push(Block {
                    rows: BLOCK_ROWS,
                    columns: cur.drain(..).map(Column::I64).collect(),
                });
                cur = (0..ncols).map(|_| Vec::with_capacity(BLOCK_ROWS)).collect();
            }
        }
        if !cur[0].is_empty() {
            let rows = cur[0].len();
            new_blocks.push(Block { rows, columns: cur.into_iter().map(Column::I64).collect() });
        }
        self.zones = build_zones(&new_blocks);
        self.blocks = new_blocks;
        self.cluster_keys = keys;
    }

    /// Zone-map pruning: morsel list for a predicate.
    pub fn pruned_morsels(&self, filter: Option<(usize, CmpOp, i64)>) -> Vec<usize> {
        match filter {
            None => (0..self.blocks.len()).collect(),
            Some((col, op, lit)) => (0..self.blocks.len())
                .filter(|&b| !self.zones[b][col].prunes(op, lit))
                .collect(),
        }
    }
}

fn build_zones(blocks: &[Block]) -> Vec<Vec<ZoneMap>> {
    blocks
        .iter()
        .map(|b| b.columns.iter().map(|c| ZoneMap::build(c.as_i64())).collect())
        .collect()
}
