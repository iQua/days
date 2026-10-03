//! RoCE queue pairs: exact Go-back-N rules and their LeanGuard transition records.
//!
//! A queue pair is a reliable DCQCN flow (`FlowGeneratorKind::Roce`). Its Mellanox-form controller
//! takes its congestion feedback from the ECN echo of its ACKs and NACKs (P16 ruling D4) and emits
//! [`crate::DcqcnTransitionRecord`]s. This module holds the reliability layer: the pure rules the
//! Scalar and CPU transitions apply (pacer status, the restart grid point, the receiver's
//! decision), and the records of the pinned schema `days-gpu/plans/briefs/p15/qp-schema.md`.
//!
//! A PSN is the byte offset of a packet's first byte. Packet `psn` is
//! `min(mtu_bytes, total_bytes - psn)` bytes, every PSN the sender can name is a packet boundary,
//! and so a retransmission is a pure function of its PSN: no segment ledger exists.

use crate::{
    EventKey, FlowGeneratorState, FlowId, GeneratorStatus, NodeId, PayloadId, RoceGenerator,
    RoceReceiverState,
};

/// The size of packet `psn` of a queue pair.
pub(crate) const fn packet_size(roce: &RoceGenerator, psn: u64) -> u64 {
    let remaining = roce.pacer.total_bytes - psn;
    if roce.pacer.mtu_bytes < remaining {
        roce.pacer.mtu_bytes
    } else {
        remaining
    }
}

/// Credit quanta a packet of `size_bytes` costs (the DCQCN pacer's denominator is always one).
const fn packet_cost(size_bytes: u64) -> u128 {
    size_bytes as u128 * 8 * 1_000_000_000
}

/// The status of an armed pacer's pending tick: `Scheduled` when one more tick of credit at the
/// controller's current rate covers the next packet, else `Blocked`. A pending tick with nothing
/// left to send (an ACK moved `next_psn` to the end while it waited) will send nothing.
pub(crate) fn armed_status(roce: &RoceGenerator) -> GeneratorStatus {
    if roce.next_psn >= roce.pacer.total_bytes || window_bound(roce) {
        return GeneratorStatus::Blocked;
    }
    let tick =
        u128::from(roce.controller.current_rate_bps) * u128::from(roce.pacer.pacing_interval_ns);
    let covered = roce
        .pacer
        .credit_quanta
        .checked_add(tick)
        .is_some_and(|credit| credit >= packet_cost(packet_size(roce, roce.next_psn)));
    if covered {
        GeneratorStatus::Scheduled
    } else {
        GeneratorStatus::Blocked
    }
}

/// Why a pacing tick parked without crediting, when it did: its data class was paused at its host
/// (host-link PFC, Amendment 1, tested first) or its window was closed (ruling D7).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TickPark {
    ClassPaused,
    WindowBlocked,
}

/// P16 ruling D7: the queue pair's window at the controller's current rate, or `None` when the
/// window is off: `window_bytes`, or with a variable window `max(1, floor(window_bytes * R_C /
/// R_max))` (SimAI `GetWin`), computed exactly in `u128` (SimAI's `u64` product can wrap; this
/// one cannot). It never exceeds `window_bytes`, since the controller keeps `R_C <= R_max`.
pub(crate) fn window_bytes(roce: &RoceGenerator) -> Option<u64> {
    if roce.window_bytes == 0 {
        return None;
    }
    if !roce.variable_window {
        return Some(roce.window_bytes);
    }
    let scaled = u128::from(roce.window_bytes) * u128::from(roce.controller.current_rate_bps)
        / u128::from(roce.controller.config.maximum_rate_bps.max(1));
    Some(u64::try_from(scaled).unwrap_or(u64::MAX).max(1))
}

/// P16 ruling D7: the window binds: `next_psn - snd_una >= w` (SimAI `IsWinBound`).
pub(crate) fn window_bound(roce: &RoceGenerator) -> bool {
    window_bytes(roce).is_some_and(|window| roce.next_psn.saturating_sub(roce.snd_una) >= window)
}

/// The status a queue pair holds after a transition that leaves its pacer as it is: `Finished`
/// once every byte is acknowledged, the armed tick's prediction, or the parked status unchanged
/// (`Blocked` while waiting for feedback, `Stopped` beyond the stop time).
pub(crate) fn settled_status(
    roce: &RoceGenerator,
    parked_status: GeneratorStatus,
) -> GeneratorStatus {
    if roce.snd_una >= roce.pacer.total_bytes {
        GeneratorStatus::Finished
    } else if roce.pacer_armed {
        armed_status(roce)
    } else {
        parked_status
    }
}

/// The grid point at which a parked pacer restarts after a rewind at `now_ns`: the first point of
/// its pacing grid strictly after `now_ns` (design note D3), so a restart never shares the rewind's
/// instant and no credit accrues while parked. `None` on overflow.
pub(crate) fn restart_time_ns(roce: &RoceGenerator, now_ns: u64) -> Option<u64> {
    let first = roce.pacer.first_pacing_time_ns;
    let interval = roce.pacer.pacing_interval_ns;
    if now_ns < first {
        return Some(first);
    }
    let elapsed_ticks = (now_ns - first) / interval;
    elapsed_ticks
        .checked_add(1)?
        .checked_mul(interval)?
        .checked_add(first)
}

/// What a receiver answers one data arrival with (design note §5, step 9).
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoceReceiverAction {
    /// A cumulative ACK: the cadence was reached or the last byte arrived.
    Ack = 0,
    /// A cumulative ACK answering a packet below the frontier.
    DuplicateAck = 1,
    /// A NACK for the frontier: an out-of-order packet, admitted by the rate limit.
    Nack = 2,
    /// An out-of-order packet whose NACK the rate limit suppressed.
    NackSuppressed = 3,
    /// No feedback: an in-order packet below the ACK cadence, or a silently dropped duplicate.
    None = 4,
}

impl RoceReceiverAction {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Ack => "ack",
            Self::DuplicateAck => "duplicate_ack",
            Self::Nack => "nack",
            Self::NackSuppressed => "nack_suppressed",
            Self::None => "none",
        }
    }

    /// Whether the action sends a feedback packet (an ACK or a NACK).
    pub const fn sends_feedback(self) -> bool {
        matches!(self, Self::Ack | Self::DuplicateAck | Self::Nack)
    }
}

/// Applies the Go-back-N receiver to one data arrival of `size_bytes` at `psn`, at `now_ns`.
///
/// In order, the frontier advances and an ACK goes out every `ack_every_packets` packets and at
/// the last byte. Below the frontier, a duplicate is answered with an ACK when `duplicate_ack`
/// holds (ruling D7). Above it, the packet is dropped and a NACK for the frontier goes out unless
/// the rate limit suppresses it: a NACK repeats the last one's expected PSN only `nack_interval_ns`
/// or more after it. Every ACK or NACK carries the frontier and restarts the cadence.
pub(crate) fn receive(
    receiver: &mut RoceReceiverState,
    psn: u64,
    size_bytes: u64,
    now_ns: u64,
) -> RoceReceiverAction {
    let action = if psn == receiver.expected_psn {
        receiver.expected_psn += size_bytes;
        receiver.packets_since_ack += 1;
        if receiver.packets_since_ack >= receiver.ack_every_packets
            || receiver.expected_psn == receiver.total_bytes
        {
            RoceReceiverAction::Ack
        } else {
            RoceReceiverAction::None
        }
    } else if psn < receiver.expected_psn {
        if receiver.duplicate_ack {
            RoceReceiverAction::DuplicateAck
        } else {
            RoceReceiverAction::None
        }
    } else {
        // A repeat's earliest time beyond `u64::MAX` is never reached, as for the CNP interval.
        let admitted = receiver.last_nack.is_none_or(|mark| {
            mark.expected_psn != receiver.expected_psn
                || mark
                    .time_ns
                    .checked_add(receiver.nack_interval_ns)
                    .is_some_and(|earliest| now_ns >= earliest)
        });
        if admitted {
            receiver.last_nack = Some(crate::RoceNackMark {
                expected_psn: receiver.expected_psn,
                time_ns: now_ns,
            });
            RoceReceiverAction::Nack
        } else {
            RoceReceiverAction::NackSuppressed
        }
    };
    if action.sends_feedback() {
        receiver.packets_since_ack = 0;
    }
    action
}

/// The DCQCN notification point of an unreliable DCQCN flow's data arrival: a CE-marked packet
/// admitted by the CNP interval sends a CNP. (A queue pair echoes ECN on its ACKs instead.)
pub(crate) fn notification_point_sends_cnp(
    np: &mut crate::DcqcnReceiverState,
    now_ns: u64,
    congestion_experienced: bool,
) -> bool {
    let interval_open = np.last_cnp_time_ns.is_none_or(|last| {
        last.checked_add(np.cnp_interval_ns)
            .is_some_and(|earliest| now_ns >= earliest)
    });
    if !congestion_experienced || !interval_open {
        return false;
    }
    np.last_cnp_time_ns = Some(now_ns);
    true
}

/// A queue pair's pacer as the sender CSV names it.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RocePacerState {
    /// One pacing tick is pending.
    Armed = 0,
    /// No tick is pending: everything is sent, or the pair finished.
    Parked = 1,
    /// The next tick lies beyond the stop time.
    Stopped = 2,
}

impl RocePacerState {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Armed => "armed",
            Self::Parked => "parked",
            Self::Stopped => "stopped",
        }
    }
}

/// The sender state one record compares before and after a transition.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoceSenderView {
    pub next_psn: u64,
    pub snd_una: u64,
    /// The high-water mark: first transmissions only.
    pub bytes_emitted: u64,
    pub packets_emitted: u64,
    pub credit_quanta: u128,
    /// `None` while no retransmission timeout is armed.
    pub rto_deadline_ns: Option<u64>,
    pub pacer: RocePacerState,
    /// The pending tick's time while armed, or the beyond-stop tick while stopped.
    pub next_tick_ns: Option<u64>,
    pub status: GeneratorStatus,
}

impl RoceSenderView {
    pub(crate) fn of(generator: &FlowGeneratorState, roce: &RoceGenerator) -> Self {
        let pacer = if roce.pacer_armed {
            RocePacerState::Armed
        } else if generator.next_emission.status == GeneratorStatus::Stopped {
            RocePacerState::Stopped
        } else {
            RocePacerState::Parked
        };
        Self {
            next_psn: roce.next_psn,
            snd_una: roce.snd_una,
            bytes_emitted: generator.bytes_emitted,
            packets_emitted: generator.packets_emitted,
            credit_quanta: roce.pacer.credit_quanta,
            rto_deadline_ns: (roce.rto_ns != 0 && roce.snd_una < generator.bytes_emitted)
                .then_some(roce.rto_deadline_ns),
            pacer,
            next_tick_ns: (pacer != RocePacerState::Parked)
                .then_some(generator.next_emission.departure_time_ns),
            status: generator.next_emission.status,
        }
    }
}

/// The sender transitions of the schema's `kind` column.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoceSenderKind {
    Tick = 0,
    Ack = 1,
    Nack = 2,
    Timeout = 3,
    /// A host RESUME restarted the queue pair's pause-parked pacer (schema Amendment 2).
    Resume = 4,
}

impl RoceSenderKind {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Tick => "tick",
            Self::Ack => "ack",
            Self::Nack => "nack",
            Self::Timeout => "timeout",
            Self::Resume => "resume",
        }
    }
}

/// The data packet a pacing tick sent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoceEmission {
    pub psn: u64,
    pub bytes: u64,
    pub retransmission: bool,
    pub payload: PayloadId,
}

/// One transition of a queue pair's sender (`roce_sender_transitions_csv`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoceSenderRecord {
    pub key: EventKey,
    pub node: NodeId,
    pub flow: FlowId,
    pub kind: RoceSenderKind,
    /// A tick that found the queue pair's data class paused at its host (Amendment 1).
    pub class_paused: bool,
    /// A tick that found the queue pair's window closed (P16 Amendment 6, ruling D7).
    pub window_blocked: bool,
    /// The queue pair's data priority (Amendment 3).
    pub data_class: u8,
    pub mtu_bytes: u64,
    pub total_bytes: u64,
    pub pacing_interval_ns: u64,
    pub first_pacing_time_ns: u64,
    pub rto_ns: u64,
    /// The window configuration (Amendment 6): `window_bytes` (0: none), `variable_window`, and
    /// the controller's maximum rate, which scales a variable window.
    pub window_bytes: u64,
    pub variable_window: bool,
    pub maximum_rate_bps: u64,
    /// The controller rate a tick credited, for a tick that credited one.
    pub rate_bps: Option<u64>,
    /// The ACK or NACK value of an `ack` or `nack` transition.
    pub input_acknowledgment: Option<u64>,
    /// The ECN echo of an `ack` or `nack` transition's packet (P16 schema Amendment 6).
    pub input_ce_echo: Option<bool>,
    pub emitted: Option<RoceEmission>,
    pub before: RoceSenderView,
    pub after: RoceSenderView,
}

/// The receiver state one record compares before and after an arrival.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoceReceiverView {
    pub expected_psn: u64,
    pub packets_since_ack: u64,
    pub last_nack_psn: Option<u64>,
    pub last_nack_time_ns: Option<u64>,
}

impl RoceReceiverView {
    pub(crate) fn of(receiver: &RoceReceiverState) -> Self {
        Self {
            expected_psn: receiver.expected_psn,
            packets_since_ack: receiver.packets_since_ack,
            last_nack_psn: receiver.last_nack.map(|mark| mark.expected_psn),
            last_nack_time_ns: receiver.last_nack.map(|mark| mark.time_ns),
        }
    }
}

/// One data arrival at a queue pair's receiver (`roce_receiver_transitions_csv`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RoceReceiverRecord {
    pub key: EventKey,
    pub node: NodeId,
    pub flow: FlowId,
    pub total_bytes: u64,
    pub ack_every_packets: u64,
    pub nack_interval_ns: u64,
    pub duplicate_ack: bool,
    pub ack_size_bytes: u64,
    pub packet_psn: u64,
    pub packet_bytes: u64,
    pub packet_sent_time_ns: u64,
    pub packet_retransmission: bool,
    pub packet_ce: bool,
    pub action: RoceReceiverAction,
    /// The frontier an ACK or NACK carried.
    pub feedback_acknowledgment: Option<u64>,
    pub feedback_payload: Option<PayloadId>,
    /// The ECN echo an ACK or NACK carried: the CE mark of the packet that triggered it (ruling D5).
    pub feedback_ce_echo: Option<bool>,
    pub before: RoceReceiverView,
    pub after: RoceReceiverView,
}

/// One queue-pair reliability transition: exactly one per event that touches a pair's sender or
/// receiver reliability state. Sender and receiver events run on different LPs, so the event key
/// alone orders them.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoceTransitionRecord {
    Sender(RoceSenderRecord),
    Receiver(RoceReceiverRecord),
}

impl RoceTransitionRecord {
    pub const fn key(&self) -> EventKey {
        match self {
            Self::Sender(record) => record.key,
            Self::Receiver(record) => record.key,
        }
    }

    pub const fn flow(&self) -> FlowId {
        match self {
            Self::Sender(record) => record.flow,
            Self::Receiver(record) => record.flow,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RoceReceiverAction, armed_status, receive, window_bound, window_bytes};
    use crate::{FlowId, GeneratorStatus, RoceNackMark, RoceReceiverState};

    /// A queue pair with a window, at controller rate `current` of `maximum`.
    fn windowed(
        window: u64,
        variable_window: bool,
        current: u64,
        maximum: u64,
    ) -> crate::RoceGenerator {
        let config = crate::DcqcnControllerConfig {
            initial_rate_bps: maximum,
            minimum_rate_bps: 3,
            maximum_rate_bps: maximum,
            additive_rate_bps: 1,
            hyper_rate_bps: 1,
            g_q63: 1,
            alpha_interval_ns: 1,
            decrease_interval_ns: 1,
            increase_interval_ns: 1,
            fast_recovery_steps: 1,
            clamp_target_rate: false,
        };
        let mut controller = crate::DcqcnController::pristine(config);
        controller.current_rate_bps = current;
        crate::RoceGenerator {
            pacer: crate::RocePacer {
                first_pacing_time_ns: 0,
                pacing_interval_ns: 1,
                mtu_bytes: 1_000,
                total_bytes: 10_000,
                credit_quanta: 0,
            },
            controller,
            pacing_timer_payload: crate::PayloadId(0),
            next_psn: 0,
            snd_una: 0,
            rto_deadline_ns: 0,
            rto_ns: 0,
            pacer_armed: true,
            window_bytes: window,
            variable_window,
            window_parked: false,
        }
    }

    #[test]
    fn the_window_is_fixed_or_scaled_by_the_rate_floored_and_at_least_one() {
        assert_eq!(window_bytes(&windowed(0, false, 5, 10)), None);
        assert_eq!(window_bytes(&windowed(8_000, false, 5, 10)), Some(8_000));
        assert_eq!(window_bytes(&windowed(8_000, true, 5, 10)), Some(4_000));
        assert_eq!(window_bytes(&windowed(8_001, true, 1, 3)), Some(2_667));
        assert_eq!(window_bytes(&windowed(7, true, 1, 100)), Some(1));
        // 10^8 B at 4 x 10^11 b/s is a 4 x 10^19 product, past u64 (SimAI's u64 product wraps).
        let maximum = 400_000_000_000;
        assert_eq!(
            window_bytes(&windowed(100_000_000, true, maximum - 1, maximum)),
            Some(99_999_999)
        );
    }

    #[test]
    fn a_closed_window_predicts_blocked_whatever_the_credit() {
        let mut roce = windowed(2_000, false, 10, 10);
        roce.next_psn = 2_000;
        roce.pacer.credit_quanta = u128::MAX / 2;
        assert!(window_bound(&roce));
        assert_eq!(armed_status(&roce), GeneratorStatus::Blocked);
        roce.snd_una = 1_000;
        assert!(!window_bound(&roce));
        assert_eq!(armed_status(&roce), GeneratorStatus::Scheduled);
        roce.window_bytes = 0;
        roce.snd_una = 0;
        assert!(!window_bound(&roce), "no window never binds");
    }

    fn receiver(ack_every_packets: u64, duplicate_ack: bool) -> RoceReceiverState {
        RoceReceiverState {
            flow: FlowId(0),
            total_bytes: 2_500,
            expected_psn: 0,
            ack_every_packets,
            packets_since_ack: 0,
            ack_size_bytes: 64,
            nack_interval_ns: 100,
            last_nack: None,
            duplicate_ack,
        }
    }

    #[test]
    fn the_receiver_acks_on_cadence_and_at_the_last_byte() {
        let mut state = receiver(2, true);
        assert_eq!(receive(&mut state, 0, 1_000, 1), RoceReceiverAction::None);
        assert_eq!(
            receive(&mut state, 1_000, 1_000, 2),
            RoceReceiverAction::Ack
        );
        assert_eq!(state.packets_since_ack, 0);
        // The short last packet completes the frontier before the cadence.
        assert_eq!(receive(&mut state, 2_000, 500, 3), RoceReceiverAction::Ack);
        assert_eq!(state.expected_psn, 2_500);
    }

    #[test]
    fn duplicates_are_acked_or_dropped_silently_by_profile() {
        let mut acking = receiver(4, true);
        acking.expected_psn = 2_000;
        acking.packets_since_ack = 1;
        assert_eq!(
            receive(&mut acking, 1_000, 1_000, 5),
            RoceReceiverAction::DuplicateAck
        );
        assert_eq!(acking.packets_since_ack, 0);
        let mut silent = receiver(4, false);
        silent.expected_psn = 2_000;
        assert_eq!(receive(&mut silent, 0, 1_000, 5), RoceReceiverAction::None);
        assert_eq!(silent.expected_psn, 2_000);
    }

    /// A repeat NACK's earliest time beyond `u64::MAX` is never reached (design note §5.9, as the
    /// notification point reads the CNP interval): the repeat stays suppressed for the whole run.
    #[test]
    fn a_nack_interval_beyond_u64_never_expires() {
        let mut state = receiver(1, true);
        state.nack_interval_ns = u64::MAX - 5;
        assert_eq!(
            receive(&mut state, 1_000, 1_000, 10),
            RoceReceiverAction::Nack
        );
        assert_eq!(
            receive(&mut state, 2_000, 500, u64::MAX),
            RoceReceiverAction::NackSuppressed
        );
    }

    #[test]
    fn nacks_are_rate_limited_per_expected_psn() {
        let mut state = receiver(1, true);
        assert_eq!(
            receive(&mut state, 1_000, 1_000, 10),
            RoceReceiverAction::Nack
        );
        assert_eq!(
            state.last_nack,
            Some(RoceNackMark {
                expected_psn: 0,
                time_ns: 10
            })
        );
        // The same expected PSN within the interval is suppressed; at the interval's end it is
        // admitted again.
        assert_eq!(
            receive(&mut state, 2_000, 500, 109),
            RoceReceiverAction::NackSuppressed
        );
        assert_eq!(
            receive(&mut state, 2_000, 500, 110),
            RoceReceiverAction::Nack
        );
        // A new frontier is NACKed at once.
        assert_eq!(receive(&mut state, 0, 1_000, 111), RoceReceiverAction::Ack);
        assert_eq!(
            receive(&mut state, 2_000, 500, 112),
            RoceReceiverAction::Nack
        );
        assert_eq!(state.expected_psn, 1_000);
    }
}
