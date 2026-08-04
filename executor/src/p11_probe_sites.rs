//! Call-site-attributed counters for resident-packet-store probes.

#[cfg(feature = "p11-probe-sites")]
use std::cell::Cell;
#[cfg(feature = "p11-probe-sites")]
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(usize)]
pub enum P11ProbeSite {
    InstallPacketBoundary,
    CpuPinnedPreservedPacket,
    CpuOutboxStagingPacket,
    HostPacketArrivalEventPacket,
    HostPacketArrivalSetSourceTime,
    HostPacketArrivalInsertGenerated,
    HostCollectiveSendSetSourceTime,
    HostCollectiveSendInsertGenerated,
    ActivateCollectivesInsertFirst,
    ActivateCollectivesInsertNext,
    HostTcpInitialSendSetSourceTime,
    HostTcpInitialSendSeedSegment,
    HostPreloadedArrivalEventPacket,
    HostPreloadedArrivalSetSourceTime,
    HostTxReadyStartTransmission,
    HostTxReadyPacketSize,
    HostTxReadyRemoteTarget,
    HostTxCompleteEventPacket,
    HostTxCompleteFinishTransmission,
    SwitchRemoteArrivalEventPacket,
    SwitchRemoteArrivalEgressAt,
    SwitchRemoteArrivalIncomingLinkSelf,
    SwitchRemoteArrivalIncomingLinkRouteScan,
    SwitchRemoteArrivalQueueBytesScan,
    SwitchRemoteArrivalSetPacketMarked,
    SwitchRemoteArrivalMarkTerminal,
    SwitchPfcArrivalQueuePriorityScan,
    SwitchPfcArrivalMarkTerminal,
    HostRemoteArrivalEventPacket,
    HostRemoteArrivalMarkTerminal,
    HostDcqcnDataArrivalMarkTerminal,
    HostDcqcnDataArrivalInsertCnp,
    HostDcqcnCnpArrivalMarkTerminal,
    HostTcpDataArrivalMarkTerminal,
    HostTcpDataArrivalInsertAck,
    HostTcpAckArrivalMarkTerminal,
    HostTcpAckArrivalAcknowledge,
    HostPacingTimerEventPacket,
    HostPacingTimerSetSourceTime,
    HostPacingTimerMarkTerminal,
    HostPacingTimerInsertGenerated,
    HostDcqcnPacingSetSourceTime,
    HostDcqcnPacingMarkTerminal,
    HostDcqcnPacingInsertGenerated,
    HostDcqcnControlTimerMarkTerminal,
    SwitchTxReadyEgressAt,
    SwitchTxReadyEligibleScan,
    SwitchTxReadyEligibleIncomingLinkSelf,
    SwitchTxReadyEligibleIncomingLinkRouteScan,
    SwitchTxReadyStartTransmission,
    SwitchTxReadyPacketSize,
    SwitchTxReadyRemoteTarget,
    SwitchTxCompleteEgressAt,
    SwitchTxCompleteEventPacket,
    SwitchTxCompleteEligibleNextScan,
    SwitchTxCompleteFinishTransmission,
    SwitchSpInsertionPositionScan,
    EmitPfcFrameInsert,
    EnqueueSourcePacketEventPacket,
    EnqueueSourcePacketSourceTime,
    EnqueueSourcePacketQueueRpositionScan,
    EmitFeedbackDrivenInsert,
    PrepareTcpAttemptsRetransmitSegment,
    InstallTcpAttemptsSeedSegment,
    InstallTcpAttemptsInsertGenerated,
    ObservePacketEntry,
}

pub const P11_PROBE_SITE_COUNT: usize = 66;

/// Indexed by `site as usize`; snake_case, matching the variant.
pub const P11_PROBE_SITE_NAMES: [&str; P11_PROBE_SITE_COUNT] = [
    "install_packet_boundary",
    "cpu_pinned_preserved_packet",
    "cpu_outbox_staging_packet",
    "host_packet_arrival_event_packet",
    "host_packet_arrival_set_source_time",
    "host_packet_arrival_insert_generated",
    "host_collective_send_set_source_time",
    "host_collective_send_insert_generated",
    "activate_collectives_insert_first",
    "activate_collectives_insert_next",
    "host_tcp_initial_send_set_source_time",
    "host_tcp_initial_send_seed_segment",
    "host_preloaded_arrival_event_packet",
    "host_preloaded_arrival_set_source_time",
    "host_tx_ready_start_transmission",
    "host_tx_ready_packet_size",
    "host_tx_ready_remote_target",
    "host_tx_complete_event_packet",
    "host_tx_complete_finish_transmission",
    "switch_remote_arrival_event_packet",
    "switch_remote_arrival_egress_at",
    "switch_remote_arrival_incoming_link_self",
    "switch_remote_arrival_incoming_link_route_scan",
    "switch_remote_arrival_queue_bytes_scan",
    "switch_remote_arrival_set_packet_marked",
    "switch_remote_arrival_mark_terminal",
    "switch_pfc_arrival_queue_priority_scan",
    "switch_pfc_arrival_mark_terminal",
    "host_remote_arrival_event_packet",
    "host_remote_arrival_mark_terminal",
    "host_dcqcn_data_arrival_mark_terminal",
    "host_dcqcn_data_arrival_insert_cnp",
    "host_dcqcn_cnp_arrival_mark_terminal",
    "host_tcp_data_arrival_mark_terminal",
    "host_tcp_data_arrival_insert_ack",
    "host_tcp_ack_arrival_mark_terminal",
    "host_tcp_ack_arrival_acknowledge",
    "host_pacing_timer_event_packet",
    "host_pacing_timer_set_source_time",
    "host_pacing_timer_mark_terminal",
    "host_pacing_timer_insert_generated",
    "host_dcqcn_pacing_set_source_time",
    "host_dcqcn_pacing_mark_terminal",
    "host_dcqcn_pacing_insert_generated",
    "host_dcqcn_control_timer_mark_terminal",
    "switch_tx_ready_egress_at",
    "switch_tx_ready_eligible_scan",
    "switch_tx_ready_eligible_incoming_link_self",
    "switch_tx_ready_eligible_incoming_link_route_scan",
    "switch_tx_ready_start_transmission",
    "switch_tx_ready_packet_size",
    "switch_tx_ready_remote_target",
    "switch_tx_complete_egress_at",
    "switch_tx_complete_event_packet",
    "switch_tx_complete_eligible_next_scan",
    "switch_tx_complete_finish_transmission",
    "switch_sp_insertion_position_scan",
    "emit_pfc_frame_insert",
    "enqueue_source_packet_event_packet",
    "enqueue_source_packet_source_time",
    "enqueue_source_packet_queue_rposition_scan",
    "emit_feedback_driven_insert",
    "prepare_tcp_attempts_retransmit_segment",
    "install_tcp_attempts_seed_segment",
    "install_tcp_attempts_insert_generated",
    "observe_packet_entry",
];

/// Indexed by `site as usize`; one of `resident`, `observed`, or `tcp_ledger`.
pub const P11_PROBE_SITE_MAPS: [&str; P11_PROBE_SITE_COUNT] = [
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "tcp_ledger",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "tcp_ledger",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "resident",
    "tcp_ledger",
    "tcp_ledger",
    "resident",
    "observed",
];

#[cfg(feature = "p11-probe-sites")]
static PROBE_TOTALS: [AtomicU64; P11_PROBE_SITE_COUNT] =
    [const { AtomicU64::new(0) }; P11_PROBE_SITE_COUNT];
#[cfg(feature = "p11-probe-sites")]
static CALL_TOTALS: [AtomicU64; P11_PROBE_SITE_COUNT] =
    [const { AtomicU64::new(0) }; P11_PROBE_SITE_COUNT];

#[cfg(feature = "p11-probe-sites")]
pub(crate) struct P11ProbeSiteCounters {
    probes: Box<[Cell<u64>; P11_PROBE_SITE_COUNT]>,
    calls: Box<[Cell<u64>; P11_PROBE_SITE_COUNT]>,
}

#[cfg(feature = "p11-probe-sites")]
impl P11ProbeSiteCounters {
    #[inline(always)]
    pub(crate) fn probe(&self, site: P11ProbeSite) {
        let counter = &self.probes[site as usize];
        counter.set(counter.get().wrapping_add(1));
    }

    #[inline(always)]
    pub(crate) fn call(&self, site: P11ProbeSite) {
        let counter = &self.calls[site as usize];
        counter.set(counter.get().wrapping_add(1));
    }
}

#[cfg(feature = "p11-probe-sites")]
impl Default for P11ProbeSiteCounters {
    fn default() -> Self {
        Self {
            probes: Box::new(std::array::from_fn(|_| Cell::new(0))),
            calls: Box::new(std::array::from_fn(|_| Cell::new(0))),
        }
    }
}

#[cfg(feature = "p11-probe-sites")]
impl Drop for P11ProbeSiteCounters {
    fn drop(&mut self) {
        for index in 0..P11_PROBE_SITE_COUNT {
            PROBE_TOTALS[index].fetch_add(self.probes[index].get(), Ordering::Relaxed);
            CALL_TOTALS[index].fetch_add(self.calls[index].get(), Ordering::Relaxed);
            self.probes[index].set(0);
            self.calls[index].set(0);
        }
    }
}

#[cfg(not(feature = "p11-probe-sites"))]
#[derive(Default)]
pub(crate) struct P11ProbeSiteCounters;

#[cfg(not(feature = "p11-probe-sites"))]
impl P11ProbeSiteCounters {
    #[inline(always)]
    pub(crate) fn probe(&self, _site: P11ProbeSite) {}

    #[inline(always)]
    pub(crate) fn call(&self, _site: P11ProbeSite) {}
}

#[cfg(feature = "p11-probe-sites")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct P11ProbeSiteTotal {
    pub name: &'static str,
    pub map: &'static str,
    pub probes: u64,
    pub calls: u64,
}

#[cfg(feature = "p11-probe-sites")]
pub fn reset_p11_probe_sites() {
    for index in 0..P11_PROBE_SITE_COUNT {
        PROBE_TOTALS[index].store(0, Ordering::Relaxed);
        CALL_TOTALS[index].store(0, Ordering::Relaxed);
    }
}

#[cfg(feature = "p11-probe-sites")]
pub fn p11_probe_site_totals() -> Vec<P11ProbeSiteTotal> {
    (0..P11_PROBE_SITE_COUNT)
        .map(|index| P11ProbeSiteTotal {
            name: P11_PROBE_SITE_NAMES[index],
            map: P11_PROBE_SITE_MAPS[index],
            probes: PROBE_TOTALS[index].load(Ordering::Relaxed),
            calls: CALL_TOTALS[index].load(Ordering::Relaxed),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    const ALL_SITES: [P11ProbeSite; P11_PROBE_SITE_COUNT] = [
        P11ProbeSite::InstallPacketBoundary,
        P11ProbeSite::CpuPinnedPreservedPacket,
        P11ProbeSite::CpuOutboxStagingPacket,
        P11ProbeSite::HostPacketArrivalEventPacket,
        P11ProbeSite::HostPacketArrivalSetSourceTime,
        P11ProbeSite::HostPacketArrivalInsertGenerated,
        P11ProbeSite::HostCollectiveSendSetSourceTime,
        P11ProbeSite::HostCollectiveSendInsertGenerated,
        P11ProbeSite::ActivateCollectivesInsertFirst,
        P11ProbeSite::ActivateCollectivesInsertNext,
        P11ProbeSite::HostTcpInitialSendSetSourceTime,
        P11ProbeSite::HostTcpInitialSendSeedSegment,
        P11ProbeSite::HostPreloadedArrivalEventPacket,
        P11ProbeSite::HostPreloadedArrivalSetSourceTime,
        P11ProbeSite::HostTxReadyStartTransmission,
        P11ProbeSite::HostTxReadyPacketSize,
        P11ProbeSite::HostTxReadyRemoteTarget,
        P11ProbeSite::HostTxCompleteEventPacket,
        P11ProbeSite::HostTxCompleteFinishTransmission,
        P11ProbeSite::SwitchRemoteArrivalEventPacket,
        P11ProbeSite::SwitchRemoteArrivalEgressAt,
        P11ProbeSite::SwitchRemoteArrivalIncomingLinkSelf,
        P11ProbeSite::SwitchRemoteArrivalIncomingLinkRouteScan,
        P11ProbeSite::SwitchRemoteArrivalQueueBytesScan,
        P11ProbeSite::SwitchRemoteArrivalSetPacketMarked,
        P11ProbeSite::SwitchRemoteArrivalMarkTerminal,
        P11ProbeSite::SwitchPfcArrivalQueuePriorityScan,
        P11ProbeSite::SwitchPfcArrivalMarkTerminal,
        P11ProbeSite::HostRemoteArrivalEventPacket,
        P11ProbeSite::HostRemoteArrivalMarkTerminal,
        P11ProbeSite::HostDcqcnDataArrivalMarkTerminal,
        P11ProbeSite::HostDcqcnDataArrivalInsertCnp,
        P11ProbeSite::HostDcqcnCnpArrivalMarkTerminal,
        P11ProbeSite::HostTcpDataArrivalMarkTerminal,
        P11ProbeSite::HostTcpDataArrivalInsertAck,
        P11ProbeSite::HostTcpAckArrivalMarkTerminal,
        P11ProbeSite::HostTcpAckArrivalAcknowledge,
        P11ProbeSite::HostPacingTimerEventPacket,
        P11ProbeSite::HostPacingTimerSetSourceTime,
        P11ProbeSite::HostPacingTimerMarkTerminal,
        P11ProbeSite::HostPacingTimerInsertGenerated,
        P11ProbeSite::HostDcqcnPacingSetSourceTime,
        P11ProbeSite::HostDcqcnPacingMarkTerminal,
        P11ProbeSite::HostDcqcnPacingInsertGenerated,
        P11ProbeSite::HostDcqcnControlTimerMarkTerminal,
        P11ProbeSite::SwitchTxReadyEgressAt,
        P11ProbeSite::SwitchTxReadyEligibleScan,
        P11ProbeSite::SwitchTxReadyEligibleIncomingLinkSelf,
        P11ProbeSite::SwitchTxReadyEligibleIncomingLinkRouteScan,
        P11ProbeSite::SwitchTxReadyStartTransmission,
        P11ProbeSite::SwitchTxReadyPacketSize,
        P11ProbeSite::SwitchTxReadyRemoteTarget,
        P11ProbeSite::SwitchTxCompleteEgressAt,
        P11ProbeSite::SwitchTxCompleteEventPacket,
        P11ProbeSite::SwitchTxCompleteEligibleNextScan,
        P11ProbeSite::SwitchTxCompleteFinishTransmission,
        P11ProbeSite::SwitchSpInsertionPositionScan,
        P11ProbeSite::EmitPfcFrameInsert,
        P11ProbeSite::EnqueueSourcePacketEventPacket,
        P11ProbeSite::EnqueueSourcePacketSourceTime,
        P11ProbeSite::EnqueueSourcePacketQueueRpositionScan,
        P11ProbeSite::EmitFeedbackDrivenInsert,
        P11ProbeSite::PrepareTcpAttemptsRetransmitSegment,
        P11ProbeSite::InstallTcpAttemptsSeedSegment,
        P11ProbeSite::InstallTcpAttemptsInsertGenerated,
        P11ProbeSite::ObservePacketEntry,
    ];

    #[test]
    fn probe_site_metadata_matches_discriminants() {
        let mut names = HashSet::new();
        for (index, site) in ALL_SITES.into_iter().enumerate() {
            debug_assert_eq!(site as usize, index);
            assert!(names.insert(P11_PROBE_SITE_NAMES[index]));
        }
    }
}
