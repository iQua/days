//! Test-only phase marks for memory accounting (P18 diet, design ruling 1).
//!
//! The memory metric of a device run is, per phase (lowering, validation, planning, upload, the
//! run, readback), the peak live host heap plus the device bytes held. A counting global
//! allocator sees the heap but not where one phase ends, so the pipeline calls [`mark`] at the
//! end of each phase, naming it and the device bytes it holds. A test or probe that installed a
//! hook with [`set_phase_hook_for_testing`] then attributes the heap peak since the previous mark
//! to that phase. Each device allocation also marks `device_alloc` with its own byte size, after
//! its upload, so a probe can track the device bytes held at each step of an upload and bound the
//! joint peak of heap and device bytes tightly.
//!
//! The hook is per thread, as the CUDA run-step record is, so parallel tests never share it, and
//! it exists only under `planner-test-hooks`: in a production build [`mark`] is empty.

/// The hook: `(phase that just ended, device bytes held during it)`.
pub type PhaseHook = fn(&'static str, usize);

#[cfg(feature = "planner-test-hooks")]
std::thread_local! {
    static HOOK: std::cell::Cell<Option<PhaseHook>> = const { std::cell::Cell::new(None) };
}

/// Marks the end of one pipeline phase on this thread. Empty in production builds.
#[doc(hidden)]
#[inline(always)]
pub fn mark(phase: &'static str, device_bytes: usize) {
    #[cfg(feature = "planner-test-hooks")]
    if let Some(hook) = HOOK.get() {
        hook(phase, device_bytes);
    }
    let _ = (phase, device_bytes);
}

/// Test-only: installs this thread's phase hook (or removes it with `None`) and returns the
/// previous one.
#[doc(hidden)]
#[cfg(feature = "planner-test-hooks")]
pub fn set_phase_hook_for_testing(hook: Option<PhaseHook>) -> Option<PhaseHook> {
    HOOK.replace(hook)
}
