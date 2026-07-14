use crate::prolly::error::Error;

/// Runtime-only worker limit for independent construction work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BuildParallelism {
    threads: usize,
}

impl BuildParallelism {
    pub fn new(threads: usize) -> Result<Self, Error> {
        if threads == 0 {
            return Err(Error::InvalidProximityConfig {
                reason: "build parallelism must be greater than zero".to_owned(),
            });
        }
        Ok(Self { threads })
    }

    pub const fn serial() -> Self {
        Self { threads: 1 }
    }

    pub const fn threads(self) -> usize {
        self.threads
    }
}

impl Default for BuildParallelism {
    fn default() -> Self {
        Self {
            threads: rayon::current_num_threads().max(1),
        }
    }
}
