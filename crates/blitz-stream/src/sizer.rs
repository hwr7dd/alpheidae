//! Map backlog (lag) → worker count for one epoch.

#[derive(Clone, Debug)]
pub struct SizerConfig {
    /// Target records each worker should handle in an epoch (rough).
    pub records_per_worker: u64,
    pub max_workers: usize,
    pub min_workers: usize,
    /// Max records packed into one stealable work unit.
    pub max_records_per_unit: u64,
}

impl Default for SizerConfig {
    fn default() -> Self {
        SizerConfig {
            records_per_worker: 10_000,
            max_workers: 64,
            min_workers: 1,
            max_records_per_unit: 4_096,
        }
    }
}

/// Size workers from lag — no historical busy-fraction required.
///
/// `N = clamp(ceil(lag / records_per_worker), min, max)`
pub fn size_workers(lag: u64, cfg: &SizerConfig) -> usize {
    if lag == 0 {
        return 0;
    }
    let per = cfg.records_per_worker.max(1);
    let n = ((lag + per - 1) / per) as usize;
    n.clamp(cfg.min_workers, cfg.max_workers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scales_with_lag() {
        let cfg = SizerConfig {
            records_per_worker: 100,
            max_workers: 8,
            min_workers: 1,
            max_records_per_unit: 50,
        };
        assert_eq!(size_workers(0, &cfg), 0);
        assert_eq!(size_workers(50, &cfg), 1);
        assert_eq!(size_workers(250, &cfg), 3);
        assert_eq!(size_workers(10_000, &cfg), 8);
    }
}
