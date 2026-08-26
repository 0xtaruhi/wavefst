use std::num::NonZeroUsize;

/// Runtime policy for independent chain compression and decompression jobs.
///
/// The `parallel` Cargo feature must be enabled before a policy other than [`Self::Serial`] can be
/// used. This keeps thread creation under application control even when parallel support is
/// compiled into the crate.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum CodecParallelism {
    /// Run codec work on the caller thread.
    #[default]
    Serial,
    /// Select a bounded worker count from the host's available parallelism.
    Auto,
    /// Use a private codec pool with exactly this many worker threads.
    Threads(NonZeroUsize),
}

impl CodecParallelism {
    /// Returns `true` when codec work is explicitly restricted to the caller thread.
    #[must_use]
    pub const fn is_serial(self) -> bool {
        matches!(self, Self::Serial)
    }
}
