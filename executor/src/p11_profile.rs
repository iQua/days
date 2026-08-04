//! Compile-time optional P11 measurement instrumentation.
//!
//! Production builds do not compile this module. Timed production samples therefore pay none of
//! its clocks or counters; diagnostic runs use it only to attribute work and verify exact result
//! identity against the ordinary paths.

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Opt-in scalar horizon policy used only by the P11 measurement harness.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum P11HorizonPolicy {
    /// Production policy: one constant bound from the global frontier and minimum channel delay.
    #[default]
    Global,
    /// Per-LP bounds from shortest nonempty channel paths.
    ///
    /// The nonempty-path requirement makes `D(i, i)` the least positive cycle, or infinity when
    /// no cycle exists. A zero diagonal would set `B_i <= N_i` and prevent the half-open drain from
    /// processing LP `i`'s frontier event.
    TransitivePerLp,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct P11LpProfile {
    pub profiled_events: u64,
    pub consecutive_same_time_events: u64,
    pub fel_pop_count: u64,
    pub fel_insert_count: u64,
    pub outbox_events: u64,
    pub fel_pop_ns: u64,
    pub fel_insert_ns: u64,
    pub transition_body_ns: u64,
    pub resident_packet_ns: u64,
    pub resident_packet_lookups: u64,
    pub resident_packet_inserts: u64,
    pub resident_packet_generator_inserts: u64,
    pub resident_packet_removes: u64,
    pub outbox_staging_ns: u64,
    pub outbox_key_inversions: u64,
    pub outbox_target_inversions: u64,
    pub outbox_distinct_targets: u64,
}

impl P11LpProfile {
    pub fn saturating_add(self, other: Self) -> Self {
        Self {
            profiled_events: self.profiled_events.saturating_add(other.profiled_events),
            consecutive_same_time_events: self
                .consecutive_same_time_events
                .saturating_add(other.consecutive_same_time_events),
            fel_pop_count: self.fel_pop_count.saturating_add(other.fel_pop_count),
            fel_insert_count: self.fel_insert_count.saturating_add(other.fel_insert_count),
            outbox_events: self.outbox_events.saturating_add(other.outbox_events),
            fel_pop_ns: self.fel_pop_ns.saturating_add(other.fel_pop_ns),
            fel_insert_ns: self.fel_insert_ns.saturating_add(other.fel_insert_ns),
            transition_body_ns: self
                .transition_body_ns
                .saturating_add(other.transition_body_ns),
            resident_packet_ns: self
                .resident_packet_ns
                .saturating_add(other.resident_packet_ns),
            resident_packet_lookups: self
                .resident_packet_lookups
                .saturating_add(other.resident_packet_lookups),
            resident_packet_inserts: self
                .resident_packet_inserts
                .saturating_add(other.resident_packet_inserts),
            resident_packet_generator_inserts: self
                .resident_packet_generator_inserts
                .saturating_add(other.resident_packet_generator_inserts),
            resident_packet_removes: self
                .resident_packet_removes
                .saturating_add(other.resident_packet_removes),
            outbox_staging_ns: self
                .outbox_staging_ns
                .saturating_add(other.outbox_staging_ns),
            outbox_key_inversions: self
                .outbox_key_inversions
                .saturating_add(other.outbox_key_inversions),
            outbox_target_inversions: self
                .outbox_target_inversions
                .saturating_add(other.outbox_target_inversions),
            outbox_distinct_targets: self
                .outbox_distinct_targets
                .saturating_add(other.outbox_distinct_targets),
        }
    }

    pub fn timed_ns(self) -> u64 {
        self.fel_pop_ns
            .saturating_add(self.fel_insert_ns)
            .saturating_add(self.transition_body_ns)
            .saturating_add(self.outbox_staging_ns)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct P11RoundProfile {
    pub round_wall_ns: u64,
    pub horizon_ns: u64,
    pub horizon_policy: P11HorizonPolicy,
    /// Minimum and maximum member of the bound family used for this round.
    pub horizon_bound_min_ns: u128,
    pub horizon_bound_max_ns: u128,
    pub horizon_distinct_bounds: u64,
    pub exchange_merge_ns: u64,
    pub residual_ns: u64,
    pub exchange_targets: u64,
    pub exchange_fan_in_sum: u64,
    pub exchange_max_fan_in: u64,
    pub lp: P11LpProfile,
}

impl P11RoundProfile {
    pub(crate) fn close_residual(&mut self) {
        self.residual_ns = self.round_wall_ns.saturating_sub(
            self.horizon_ns
                .saturating_add(self.lp.timed_ns())
                .saturating_add(self.exchange_merge_ns),
        );
    }

    pub const fn profiled_events(self) -> u64 {
        self.lp.profiled_events
    }

    pub const fn consecutive_same_time_events(self) -> u64 {
        self.lp.consecutive_same_time_events
    }

    pub const fn transition_body_ns(self) -> u64 {
        self.lp.transition_body_ns
    }

    pub const fn outbox_key_inversions(self) -> u64 {
        self.lp.outbox_key_inversions
    }

    pub const fn outbox_target_inversions(self) -> u64 {
        self.lp.outbox_target_inversions
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct P11PacketStoreProfile {
    pub ns: u64,
    pub lookups: u64,
    pub inserts: u64,
    pub generator_inserts: u64,
    pub removes: u64,
}

impl P11PacketStoreProfile {
    pub(crate) fn saturating_sub(self, before: Self) -> Self {
        Self {
            ns: self.ns.saturating_sub(before.ns),
            lookups: self.lookups.saturating_sub(before.lookups),
            inserts: self.inserts.saturating_sub(before.inserts),
            generator_inserts: self
                .generator_inserts
                .saturating_sub(before.generator_inserts),
            removes: self.removes.saturating_sub(before.removes),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) enum P11PacketStoreOperation {
    Lookup,
    Insert { generator: bool },
    Remove,
}

#[derive(Default)]
pub(crate) struct P11PacketStoreProfiler {
    profile: Cell<P11PacketStoreProfile>,
}

impl P11PacketStoreProfiler {
    pub(crate) fn snapshot(&self) -> P11PacketStoreProfile {
        self.profile.get()
    }

    pub(crate) fn measure(&self, operation: P11PacketStoreOperation) -> P11PacketStoreGuard<'_> {
        let allocation_scope = match operation {
            P11PacketStoreOperation::Lookup => AllocationScope::None,
            P11PacketStoreOperation::Insert { generator: false }
            | P11PacketStoreOperation::Remove => AllocationScope::Resident,
            P11PacketStoreOperation::Insert { generator: true } => AllocationScope::Generator,
        };
        let previous_scope = ALLOCATION_SCOPE.with(|scope| scope.replace(allocation_scope));
        P11PacketStoreGuard {
            profiler: self,
            operation,
            started: Instant::now(),
            previous_scope,
        }
    }
}

pub(crate) struct P11PacketStoreGuard<'profile> {
    profiler: &'profile P11PacketStoreProfiler,
    operation: P11PacketStoreOperation,
    started: Instant,
    previous_scope: AllocationScope,
}

impl Drop for P11PacketStoreGuard<'_> {
    fn drop(&mut self) {
        let mut profile = self.profiler.profile.get();
        profile.ns = profile
            .ns
            .saturating_add(duration_ns(self.started.elapsed()));
        match self.operation {
            P11PacketStoreOperation::Lookup => {
                profile.lookups = profile.lookups.saturating_add(1);
            }
            P11PacketStoreOperation::Insert { generator } => {
                profile.inserts = profile.inserts.saturating_add(1);
                if generator {
                    profile.generator_inserts = profile.generator_inserts.saturating_add(1);
                }
            }
            P11PacketStoreOperation::Remove => {
                profile.removes = profile.removes.saturating_add(1);
            }
        }
        self.profiler.profile.set(profile);
        ALLOCATION_SCOPE.with(|scope| scope.set(self.previous_scope));
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct P11AllocationProfile {
    pub allocation_calls: u64,
    pub allocated_bytes: u64,
    pub deallocation_calls: u64,
    pub deallocated_bytes: u64,
    pub generator_allocation_calls: u64,
    pub generator_allocated_bytes: u64,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
enum AllocationScope {
    #[default]
    None,
    Resident,
    Generator,
}

thread_local! {
    static ALLOCATION_SCOPE: Cell<AllocationScope> = const { Cell::new(AllocationScope::None) };
}

static ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static GENERATOR_ALLOCATION_CALLS: AtomicU64 = AtomicU64::new(0);
static GENERATOR_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Called by the P11 benchmark binary's counting allocator.
pub fn record_scoped_allocation(bytes: usize) {
    ALLOCATION_SCOPE.with(|scope| match scope.get() {
        AllocationScope::None => {}
        AllocationScope::Resident => {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
        }
        AllocationScope::Generator => {
            ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
            GENERATOR_ALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            GENERATOR_ALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    });
}

/// Called by the P11 benchmark binary's counting allocator.
pub fn record_scoped_deallocation(bytes: usize) {
    ALLOCATION_SCOPE.with(|scope| {
        if scope.get() != AllocationScope::None {
            DEALLOCATION_CALLS.fetch_add(1, Ordering::Relaxed);
            DEALLOCATED_BYTES.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    });
}

pub fn reset_allocation_profile() {
    ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    ALLOCATED_BYTES.store(0, Ordering::Relaxed);
    DEALLOCATION_CALLS.store(0, Ordering::Relaxed);
    DEALLOCATED_BYTES.store(0, Ordering::Relaxed);
    GENERATOR_ALLOCATION_CALLS.store(0, Ordering::Relaxed);
    GENERATOR_ALLOCATED_BYTES.store(0, Ordering::Relaxed);
}

pub fn allocation_profile() -> P11AllocationProfile {
    P11AllocationProfile {
        allocation_calls: ALLOCATION_CALLS.load(Ordering::Relaxed),
        allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
        deallocation_calls: DEALLOCATION_CALLS.load(Ordering::Relaxed),
        deallocated_bytes: DEALLOCATED_BYTES.load(Ordering::Relaxed),
        generator_allocation_calls: GENERATOR_ALLOCATION_CALLS.load(Ordering::Relaxed),
        generator_allocated_bytes: GENERATOR_ALLOCATED_BYTES.load(Ordering::Relaxed),
    }
}

pub(crate) fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
