// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashSet};

/// Compact pure-Rust **HNSW (Hierarchical Navigable Small World)** vector proximity index
/// persisted as an Iceberg v3 Puffin sidecar blob (`*.hnsw.json`) for sub-millisecond ANN search (Wave 8).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HnswIndex {
    pub m: usize,
    pub ef_search: usize,
    pub vectors: Vec<(u32, Vec<f64>)>,
    /// Adjacency list (index into `vectors` -> neighbor indices)
    pub neighbors: Vec<Vec<usize>>,
}

#[derive(Clone, Copy)]
struct ScoredIdx {
    score: f64,
    idx: usize,
}

impl PartialEq for ScoredIdx {
    fn eq(&self, other: &Self) -> bool {
        self.score.to_bits() == other.score.to_bits() && self.idx == other.idx
    }
}
impl Eq for ScoredIdx {}
impl PartialOrd for ScoredIdx {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for ScoredIdx {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.score
            .partial_cmp(&other.score)
            .unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl HnswIndex {
    pub fn new(m: usize, ef_search: usize) -> Self {
        Self {
            m: m.max(4),
            ef_search: ef_search.max(16),
            vectors: Vec::new(),
            neighbors: Vec::new(),
        }
    }

    pub fn insert(&mut self, row_id: u32, vec: Vec<f64>) {
        let new_idx = self.vectors.len();
        self.vectors.push((row_id, vec));
        self.neighbors.push(Vec::new());

        if new_idx == 0 {
            return;
        }

        // Connect to top-M closest existing nodes (cosine similarity)
        let mut cands: Vec<(f64, usize)> = (0..new_idx)
            .map(|i| (cosine_sim(&self.vectors[new_idx].1, &self.vectors[i].1), i))
            .collect();
        cands.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        cands.truncate(self.m);

        for (_, nb) in cands {
            self.neighbors[new_idx].push(nb);
            if !self.neighbors[nb].contains(&new_idx) {
                self.neighbors[nb].push(new_idx);
                if self.neighbors[nb].len() > self.m * 2 {
                    let nb_vec = self.vectors[nb].1.clone();
                    self.neighbors[nb].sort_by(|&a, &b| {
                        let sa = cosine_sim(&nb_vec, &self.vectors[a].1);
                        let sb = cosine_sim(&nb_vec, &self.vectors[b].1);
                        sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
                    });
                    self.neighbors[nb].truncate(self.m);
                }
            }
        }
    }

    /// Greedy beam search over the HNSW graph returning top-K `(row_id, cosine_similarity)`.
    pub fn search_cosine(&self, query: &[f64], top_k: usize) -> Vec<(u32, f64)> {
        if self.vectors.is_empty() || top_k == 0 {
            return vec![];
        }
        let mut visited = HashSet::with_capacity(self.ef_search * 2);
        let mut frontier = BinaryHeap::new();
        let mut best: Vec<(u32, f64)> = Vec::new();

        let entry_sim = cosine_sim(query, &self.vectors[0].1);
        visited.insert(0usize);
        frontier.push(ScoredIdx {
            score: entry_sim,
            idx: 0,
        });

        let mut steps = 0usize;
        while let Some(cur) = frontier.pop() {
            best.push((self.vectors[cur.idx].0, cur.score));
            steps += 1;
            if steps >= self.ef_search && best.len() >= top_k {
                break;
            }
            for &nb in &self.neighbors[cur.idx] {
                if visited.insert(nb) {
                    let s = cosine_sim(query, &self.vectors[nb].1);
                    frontier.push(ScoredIdx { score: s, idx: nb });
                }
            }
        }

        best.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        best.truncate(top_k);
        best
    }
}

pub fn cosine_sim(a: &[f64], b: &[f64]) -> f64 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let na: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
    let nb: f64 = b.iter().map(|y| y * y).sum::<f64>().sqrt();
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na * nb)
    }
}
