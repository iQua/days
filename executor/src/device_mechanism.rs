//! Device row codecs for the DCQCN reaction and notification points, shared by Metal and CUDA.
//!
//! Both device backends use the same 43-word generator row and the same 7-word per-flow receiver
//! row in the transport plane, so the host packing and readback of DCQCN state live here once.
//! The word layout is mirrored by `cuda_kernels.cu` and `metal_kernels.metal`.
//!
//! **Generator row (kind [`GENERATOR_KIND_DCQCN`]).** Words 12..19 keep the T25 rate layout. The
//! controller follows at [`G_DCQCN_MIN_RATE`]..=[`G_DCQCN_CONTROL_PAYLOAD`]. Immutable image data
//! that no transition reads (the initial rate and the CNP size) stays host-side: readback starts
//! from the image, so those fields are carried through unchanged.
//!
//! **Receiver row.** A DCQCN flow's notification-point state occupies that flow's receiver row in
//! the transport plane, the row a TCP flow uses for its cumulative-ACK receiver. Validation makes
//! every receiver either TCP or DCQCN and pins a DCQCN receiver to its flow's target, so the row
//! is otherwise unused and DCQCN adds no words to any plane. Words 4..6 remain zero because the
//! readback reads them as the TCP receive-range metadata.

use crate::{
    DcqcnGenerator, DcqcnIncreaseStage, DcqcnReceiverState, FlowGeneratorKind, PacketKind,
    SimulationImage,
};

/// Mechanism bit: some host holds a DCQCN notification point, so a receiver row can carry a
/// DCQCN marker. Without it every receiver-row marker is a TCP `1` or an unused `0` for the whole
/// run: only image receivers create the markers `2` and `3`, and no transition writes one.
pub(crate) const MECHANISM_DCQCN_RECEIVERS: u64 = 1;
/// Mechanism bit: the image holds DCQCN state of any kind (a reaction-point generator, a
/// notification point, or a resident CNP or control-timer packet). Without it no DCQCN packet
/// exists or can be created, so every DCQCN transition branch is dead for the whole run.
pub(crate) const MECHANISM_DCQCN: u64 = 2;
/// Mechanism bit: the image holds PFC state, the condition under which the planner allocates the
/// PFC region and its params offset is not `NONE` ([`crate::device_pfc::image_has_pfc`]), or a
/// resident PFC frame.
pub(crate) const MECHANISM_PFC: u64 = 4;

/// The two builds of the device round kernel (`days_round`), selected per run from the image.
///
/// Both builds come from one source: a CUDA template parameter and a Metal function constant.
/// A selection error can only ever cost time, never bytes: the mechanisms build runs every image,
/// and the plain build stops with a semantic error on any DCQCN or PFC state it meets.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoundKernel {
    /// DCQCN and PFC compiled out.
    Plain,
    /// DCQCN and PFC compiled in.
    Mechanisms,
}

impl RoundKernel {
    /// The mechanisms build if and only if the image holds any DCQCN or PFC state.
    pub fn for_image(image: &SimulationImage) -> Self {
        if mechanism_flags(image) == 0 {
            Self::Plain
        } else {
            Self::Mechanisms
        }
    }
}

/// The P14 mechanisms a device run can exercise, derived from the image alone. Host-side only:
/// it selects the round kernel and is never planned into a device plane.
pub(crate) fn mechanism_flags(image: &SimulationImage) -> u64 {
    let receivers = image
        .host_states
        .iter()
        .any(|state| !state.dcqcn_receivers.is_empty());
    let dcqcn = receivers
        || image
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .any(|generator| matches!(generator.kind, FlowGeneratorKind::Dcqcn(_)))
        || image.initial_packets.iter().any(|packet| {
            matches!(
                packet.kind,
                PacketKind::DcqcnCnp(_) | PacketKind::DcqcnControlTimer
            )
        });
    let pfc = crate::device_pfc::image_has_pfc(image)
        || image
            .initial_packets
            .iter()
            .any(|packet| matches!(packet.kind, PacketKind::Pfc(_)));
    (if receivers {
        MECHANISM_DCQCN_RECEIVERS
    } else {
        0
    }) | (if dcqcn { MECHANISM_DCQCN } else { 0 })
        | (if pfc { MECHANISM_PFC } else { 0 })
}

pub(crate) const GENERATOR_KIND_DCQCN: u64 = 3;

// Word layout shared by both device plans and kernels; `plan_check_word_indices_match_both_kernels`
// pins each against `cuda_kernels.cu` and `metal_kernels.metal`.
const PLAN_NONE: u64 = u64::MAX;
const G_VALID: usize = 0;
const G_OWNER: usize = 1;
const G_KIND: usize = 11;
const PLAN_GENERATOR_WORDS: usize = 43;
const PLAN_NODE_WORDS: usize = 11;
const N_SERVICE_VALID: usize = 4;
const PLAN_FLOW_WORDS: usize = 6;
const PLAN_QUEUE_META_WORDS: usize = 5;
const PLAN_ARENA_META_WORDS: usize = 4;
const PLAN_TCP_RECEIVER_WORDS: usize = 7;
const PK_KIND: usize = 10;
const PK_KIND_MASK: u64 = !(1_u64 << 63);
const PFC_PACKET: u64 = 4;
const DCQCN_CNP_PACKET: u64 = 5;
const DCQCN_CONTROL_TIMER_PACKET: u64 = 6;

/// The planes of one uploaded device plan that the plain-kernel check reads, in the word layout
/// both device backends share (`cuda_kernels.cu` and `metal_kernels.metal`). Each backend fills it
/// from its own plan, reading its own params indices.
pub(crate) struct UploadedPlan<'a> {
    /// `params[P_PFC_OFFSET]`: the PFC region's offset, or `NONE` without one.
    pub(crate) pfc_offset: u64,
    /// `params[P_TCP_RECEIVER_OFFSET]`: the per-flow receiver rows in `tcp_state`, or `NONE`.
    pub(crate) receiver_offset: u64,
    pub(crate) node_count: usize,
    pub(crate) flow_count: usize,
    pub(crate) node_state: &'a [u64],
    pub(crate) generators: &'a [u64],
    pub(crate) flows: &'a [u64],
    pub(crate) fel_meta: &'a [u64],
    pub(crate) fel_records: &'a [u64],
    pub(crate) queue_meta: &'a [u64],
    pub(crate) queue_records: &'a [u64],
    pub(crate) in_service: &'a [u64],
    pub(crate) stream_state: &'a [u64],
    pub(crate) stream_records: &'a [u64],
    /// Stream layout: channel streams, then one service stream per LP, then one generator stream
    /// per flow. `stream_count` is zero when streams are disabled.
    pub(crate) stream_count: usize,
    pub(crate) service_stream_base: usize,
    pub(crate) generator_stream_base: usize,
    pub(crate) tcp_state: &'a [u64],
}

/// Why the plain round kernel may not run an uploaded plan.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PlainKernelRefusal {
    /// The plan carries a PFC region.
    PfcRegion,
    /// A valid generator row is a DCQCN reaction point.
    DcqcnGenerator { flow: u64, owner: u64 },
    /// A flow's receiver row carries a DCQCN notification-point marker.
    DcqcnReceiver { flow: u64, target: u64 },
    /// A meta or row index points outside its plane, so the plan cannot be checked.
    MalformedPlan,
    /// A live record of a PFC, CNP or control-timer packet.
    MechanismPacket {
        arena: PlanArena,
        lp: Option<u64>,
        kind: u64,
    },
}

/// The plan arenas whose live records the check walks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PlanArena {
    FallbackHeap,
    ChannelStream,
    GeneratorStream,
    Queue,
    InService,
}

impl PlainKernelRefusal {
    /// The LP the refusal concerns, where there is one.
    pub(crate) fn node(self) -> Option<crate::NodeId> {
        match self {
            Self::PfcRegion | Self::MalformedPlan => None,
            Self::DcqcnGenerator { owner, .. } => Some(crate::NodeId(owner)),
            Self::DcqcnReceiver { target, .. } => Some(crate::NodeId(target)),
            Self::MechanismPacket { lp, .. } => lp.map(crate::NodeId),
        }
    }
}

/// Fails closed for the plain round kernel, on the host, over the plan about to be uploaded.
///
/// The plain `days_round` compiles every Lane B transition out and carries no device-side stop, so
/// the host refuses to launch it on any plan that could need one. It refuses when:
/// 1. the PFC region is present (`P_PFC_OFFSET != NONE`);
/// 2. a valid generator row has `G_KIND == GENERATOR_KIND_DCQCN`;
/// 3. a receiver region exists and a flow's receiver row carries a DCQCN marker (2 or 3; the check
///    mirrors the kernel's guard, `marker >= 2`);
/// 4. a live record in the fallback heap, a channel or generator stream, a queue, or `in_service`
///    has packet kind 4 (PFC), 5 (CNP) or 6 (DCQCN control timer).
///
/// **Why these four are sufficient for the whole run.** They are exactly the states under which a
/// `MECHANISMS`-guarded branch of the kernel is taken: `pfc_row` and `paused_mask` come only from
/// the PFC region; `dcqcn_pacing_timer` needs `G_KIND == 3`; `dcqcn_data_arrival` needs a receiver
/// marker of at least 2; `pfc_frame_arrival`, `dcqcn_cnp_arrival` and `dcqcn_control_timer` need
/// packet kinds 4, 5 and 6. None of these states can arise during a plain run: the only emitters of
/// kinds 4-6 are `emit_pfc_frame` and the DCQCN transitions, all compiled out; the markers 2 and 3
/// are written only by `dcqcn_data_arrival` (compiled out) or by the image; and no plain transition
/// writes `G_KIND` or the PFC offset. So their absence at upload holds for the whole run.
///
/// **Why it is independent of [`RoundKernel::for_image`].** Selection evaluates predicates on the
/// `SimulationImage` before planning. This check reads the encoded words the kernel will execute
/// on (the `G_KIND` tag, the receiver marker word, the PFC offset, packet-kind words after event
/// encoding and stream classification) and shares no code with selection. A selection defect, or a
/// planner or encoder defect (a mis-encoded tag, a region planned without image state, a checkpoint
/// restoring an in-flight CNP), is caught unless both paths fail the same way.
///
/// It walks only live records, by the arena metas, so its cost is linear in resident records plus
/// one pass over the generator and receiver rows, once per plan. The first refusal in this order
/// is returned: condition 1, then 2 by flow, then 3 by flow, then 4 by arena and slot.
pub(crate) fn plain_round_kernel_refusal(plan: &UploadedPlan<'_>) -> Option<PlainKernelRefusal> {
    // A plan whose metas point outside its planes cannot be checked; refuse it rather than accept.
    scan_uploaded_plan(plan).unwrap_or(Some(PlainKernelRefusal::MalformedPlan))
}

/// A meta or row index that points outside its plane.
struct OutsidePlane;

fn scan_uploaded_plan(plan: &UploadedPlan<'_>) -> Result<Option<PlainKernelRefusal>, OutsidePlane> {
    use crate::device_event_record::{
        EVENT_WORDS, StoredEventClass, decode_event_record, stored_event_words,
    };

    if plan.pfc_offset != PLAN_NONE {
        return Ok(Some(PlainKernelRefusal::PfcRegion));
    }
    for flow in 0..plan.flow_count {
        let row = words(
            plan.generators,
            flow * PLAN_GENERATOR_WORDS,
            PLAN_GENERATOR_WORDS,
        )?;
        if row[G_VALID] != 0 && row[G_KIND] == GENERATOR_KIND_DCQCN {
            return Ok(Some(PlainKernelRefusal::DcqcnGenerator {
                flow: flow as u64,
                owner: row[G_OWNER],
            }));
        }
    }
    if plan.receiver_offset != PLAN_NONE {
        for flow in 0..plan.flow_count {
            let row = add(
                plan.receiver_offset,
                (flow * PLAN_TCP_RECEIVER_WORDS) as u64,
            )?;
            let marker = words(plan.tcp_state, row as usize, 1)?[0];
            // The kernel's own guard: `dcqcn_data_arrival` is entered for any marker >= 2.
            if marker >= DCQCN_RECEIVER_NO_CNP {
                return Ok(Some(PlainKernelRefusal::DcqcnReceiver {
                    flow: flow as u64,
                    target: flow_word(plan, flow, 1)?,
                }));
            }
        }
    }

    let refuse = |arena, lp, packet_kind: u64| {
        let kind = packet_kind & PK_KIND_MASK;
        matches!(
            kind,
            PFC_PACKET | DCQCN_CNP_PACKET | DCQCN_CONTROL_TIMER_PACKET
        )
        .then_some(PlainKernelRefusal::MechanismPacket { arena, lp, kind })
    };
    // The fallback heap: each LP's records occupy `offset..offset + count`.
    for lp in 0..plan.node_count {
        let meta = words(
            plan.fel_meta,
            lp * PLAN_ARENA_META_WORDS,
            PLAN_ARENA_META_WORDS,
        )?;
        for slot in meta[0]..add(meta[0], meta[3])? {
            let record = words(plan.fel_records, index(slot, EVENT_WORDS)?, EVENT_WORDS)?;
            if let Some(refusal) = refuse(PlanArena::FallbackHeap, Some(lp as u64), record[PK_KIND])
            {
                return Ok(Some(refusal));
            }
        }
    }
    // Channel and generator streams: rings of encoded records at word offset `meta[0]`, decoded as
    // the readback decodes them. Service streams carry no packet kind: TX_COMPLETE hydrates from
    // `in_service`, checked below.
    for stream in 0..plan.stream_count {
        let (class, arena, flow) = if stream < plan.service_stream_base {
            (StoredEventClass::Channel, PlanArena::ChannelStream, None)
        } else if stream < plan.generator_stream_base {
            continue;
        } else {
            let flow = stream - plan.generator_stream_base;
            (
                StoredEventClass::Generator,
                PlanArena::GeneratorStream,
                Some(flow),
            )
        };
        let lp = flow.map(|flow| flow_word(plan, flow, 0)).transpose()?;
        let meta = words(
            plan.stream_state,
            stream * PLAN_ARENA_META_WORDS,
            PLAN_ARENA_META_WORDS,
        )?;
        let (source, capacity, head, count) = (meta[0], meta[1].max(1), meta[2], meta[3]);
        let record_words = stored_event_words(class);
        for live in 0..count {
            let physical = add(head, live)? % capacity;
            let start = add(source, index(physical, record_words)? as u64)?;
            let encoded = words(plan.stream_records, start as usize, record_words)?;
            let record = decode_event_record(
                encoded,
                class,
                lp.unwrap_or(0),
                flow.map(|flow| flow as u64),
                None,
            );
            if let Some(refusal) = refuse(arena, lp, record[PK_KIND]) {
                return Ok(Some(refusal));
            }
        }
    }
    // Queues: each LP's ring of full records.
    for lp in 0..plan.node_count {
        let meta = words(
            plan.queue_meta,
            lp * PLAN_QUEUE_META_WORDS,
            PLAN_QUEUE_META_WORDS,
        )?;
        let (offset, capacity, head, count) = (meta[0], meta[1].max(1), meta[2], meta[3]);
        for live in 0..count {
            let slot = add(offset, add(head, live)? % capacity)?;
            let record = words(plan.queue_records, index(slot, EVENT_WORDS)?, EVENT_WORDS)?;
            if let Some(refusal) = refuse(PlanArena::Queue, Some(lp as u64), record[PK_KIND]) {
                return Ok(Some(refusal));
            }
        }
    }
    // In-service rows, live while the LP's service is valid.
    for lp in 0..plan.node_count {
        if words(plan.node_state, lp * PLAN_NODE_WORDS + N_SERVICE_VALID, 1)?[0] == 0 {
            continue;
        }
        let record = words(plan.in_service, lp * EVENT_WORDS, EVENT_WORDS)?;
        if let Some(refusal) = refuse(PlanArena::InService, Some(lp as u64), record[PK_KIND]) {
            return Ok(Some(refusal));
        }
    }
    Ok(None)
}

/// `plane[start..start + len]`, or [`OutsidePlane`].
fn words(plane: &[u64], start: usize, len: usize) -> Result<&[u64], OutsidePlane> {
    start
        .checked_add(len)
        .and_then(|end| plane.get(start..end))
        .ok_or(OutsidePlane)
}

fn add(left: u64, right: u64) -> Result<u64, OutsidePlane> {
    left.checked_add(right).ok_or(OutsidePlane)
}

/// The word offset of record `slot` of `record_words` words.
fn index(slot: u64, record_words: usize) -> Result<usize, OutsidePlane> {
    usize::try_from(slot)
        .ok()
        .and_then(|slot| slot.checked_mul(record_words))
        .ok_or(OutsidePlane)
}

fn flow_word(plan: &UploadedPlan<'_>, flow: usize, word: usize) -> Result<u64, OutsidePlane> {
    Ok(words(plan.flows, flow * PLAN_FLOW_WORDS + word, 1)?[0])
}

const G_RATE_FIRST: usize = 12;
const G_RATE_INTERVAL: usize = 13;
const G_RATE_PACKET_SIZE: usize = 14;
const G_RATE_TOTAL: usize = 15;
const G_RATE_NUMERATOR: usize = 16;
const G_RATE_DENOMINATOR: usize = 17;
const G_RATE_CREDIT_LOW: usize = 18;
const G_RATE_CREDIT_HIGH: usize = 19;
pub(crate) const G_DCQCN_MIN_RATE: usize = 20;
const G_DCQCN_MAX_RATE: usize = 21;
const G_DCQCN_ADDITIVE_RATE: usize = 22;
const G_DCQCN_HYPER_RATE: usize = 23;
const G_DCQCN_G: usize = 24;
const G_DCQCN_DECREASE: usize = 25;
const G_DCQCN_CNP_INTERVAL: usize = 26;
const G_DCQCN_CONTROL_INTERVAL: usize = 27;
const G_DCQCN_BYTE_THRESHOLD: usize = 28;
const G_DCQCN_ALPHA: usize = 29;
const G_DCQCN_CURRENT_RATE: usize = 30;
const G_DCQCN_TARGET_RATE: usize = 31;
const G_DCQCN_CNP_SEEN: usize = 32;
const G_DCQCN_LAST_CNP_VALID: usize = 33;
const G_DCQCN_LAST_CNP: usize = 34;
const G_DCQCN_STAGE: usize = 35;
const G_DCQCN_STAGE_STEPS: usize = 36;
const G_DCQCN_BYTES_SINCE_INCREASE: usize = 37;
const G_DCQCN_NEXT_CONTROL: usize = 38;
pub(crate) const G_DCQCN_CONTROL_PAYLOAD: usize = 39;

/// Receiver-row marker: no CNP sent yet.
pub(crate) const DCQCN_RECEIVER_NO_CNP: u64 = 2;
/// Receiver-row marker: word 1 holds the last CNP time.
pub(crate) const DCQCN_RECEIVER_LAST_CNP: u64 = 3;
const DR_LAST_CNP: usize = 1;
const DR_CNP_INTERVAL: usize = 2;
const DR_CNP_SIZE: usize = 3;

/// Writes one DCQCN generator's kind word and kind-specific words into its 43-word row.
pub(crate) fn encode_dcqcn_generator(dcqcn: &DcqcnGenerator, row: &mut [u64]) {
    let rate = dcqcn.rate;
    let controller = dcqcn.controller;
    let config = controller.config;
    row[11] = GENERATOR_KIND_DCQCN;
    row[G_RATE_FIRST] = rate.first_pacing_time_ns;
    row[G_RATE_INTERVAL] = rate.pacing_interval_ns;
    row[G_RATE_PACKET_SIZE] = rate.packet_size_bytes;
    row[G_RATE_TOTAL] = rate.total_bytes;
    row[G_RATE_NUMERATOR] = rate.rate_numerator_bits_per_second;
    row[G_RATE_DENOMINATOR] = rate.rate_denominator;
    row[G_RATE_CREDIT_LOW] = rate.credit_quanta as u64;
    row[G_RATE_CREDIT_HIGH] = (rate.credit_quanta >> 64) as u64;
    row[G_DCQCN_MIN_RATE] = config.minimum_rate_bps;
    row[G_DCQCN_MAX_RATE] = config.maximum_rate_bps;
    row[G_DCQCN_ADDITIVE_RATE] = config.additive_rate_bps;
    row[G_DCQCN_HYPER_RATE] = config.hyper_rate_bps;
    row[G_DCQCN_G] = config.g_ppb;
    row[G_DCQCN_DECREASE] = config.decrease_ppb;
    row[G_DCQCN_CNP_INTERVAL] = config.cnp_interval_ns;
    row[G_DCQCN_CONTROL_INTERVAL] = config.control_interval_ns;
    row[G_DCQCN_BYTE_THRESHOLD] = config.increase_byte_threshold;
    row[G_DCQCN_ALPHA] = controller.alpha_ppb;
    row[G_DCQCN_CURRENT_RATE] = controller.current_rate_bps;
    row[G_DCQCN_TARGET_RATE] = controller.target_rate_bps;
    row[G_DCQCN_CNP_SEEN] = u64::from(controller.cnp_seen);
    row[G_DCQCN_LAST_CNP_VALID] = u64::from(controller.last_cnp_time_ns.is_some());
    row[G_DCQCN_LAST_CNP] = controller.last_cnp_time_ns.unwrap_or(0);
    row[G_DCQCN_STAGE] = controller.stage as u64;
    row[G_DCQCN_STAGE_STEPS] = u64::from(controller.stage_steps);
    row[G_DCQCN_BYTES_SINCE_INCREASE] = controller.bytes_since_increase;
    row[G_DCQCN_NEXT_CONTROL] = controller.next_control_time_ns;
    row[G_DCQCN_CONTROL_PAYLOAD] = dcqcn.control_timer_payload.0;
}

/// Restores the mutable DCQCN words of one generator row onto the image's generator.
///
/// Configuration, the control-timer token and the CNP size are image data no device transition
/// writes; they are checked rather than trusted, so a corrupted row surfaces as an error.
pub(crate) fn decode_dcqcn_generator(
    row: &[u64],
    dcqcn: &mut DcqcnGenerator,
) -> Result<(), &'static str> {
    let mut expected = [0_u64; 43];
    encode_dcqcn_generator(dcqcn, &mut expected);
    let immutable = [
        G_RATE_FIRST,
        G_RATE_INTERVAL,
        G_RATE_PACKET_SIZE,
        G_RATE_TOTAL,
        G_RATE_DENOMINATOR,
        G_DCQCN_MIN_RATE,
        G_DCQCN_MAX_RATE,
        G_DCQCN_ADDITIVE_RATE,
        G_DCQCN_HYPER_RATE,
        G_DCQCN_G,
        G_DCQCN_DECREASE,
        G_DCQCN_CNP_INTERVAL,
        G_DCQCN_CONTROL_INTERVAL,
        G_DCQCN_BYTE_THRESHOLD,
        G_DCQCN_CONTROL_PAYLOAD,
    ];
    if row[11] != GENERATOR_KIND_DCQCN || immutable.iter().any(|&word| row[word] != expected[word])
    {
        return Err("DCQCN generator row changed immutable configuration");
    }
    let stage = match row[G_DCQCN_STAGE] {
        0 => DcqcnIncreaseStage::FastRecovery,
        1 => DcqcnIncreaseStage::Additive,
        2 => DcqcnIncreaseStage::Hyper,
        _ => return Err("DCQCN generator row carries an unknown increase stage"),
    };
    let stage_steps = u8::try_from(row[G_DCQCN_STAGE_STEPS])
        .map_err(|_| "DCQCN generator row stage counter exceeds u8")?;
    let flag = |word: usize| match row[word] {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err("DCQCN generator row carries a non-boolean flag"),
    };
    dcqcn.rate.rate_numerator_bits_per_second = row[G_RATE_NUMERATOR];
    dcqcn.rate.credit_quanta =
        u128::from(row[G_RATE_CREDIT_LOW]) | (u128::from(row[G_RATE_CREDIT_HIGH]) << 64);
    let controller = &mut dcqcn.controller;
    controller.alpha_ppb = row[G_DCQCN_ALPHA];
    controller.current_rate_bps = row[G_DCQCN_CURRENT_RATE];
    controller.target_rate_bps = row[G_DCQCN_TARGET_RATE];
    controller.cnp_seen = flag(G_DCQCN_CNP_SEEN)?;
    controller.last_cnp_time_ns = flag(G_DCQCN_LAST_CNP_VALID)?.then_some(row[G_DCQCN_LAST_CNP]);
    controller.stage = stage;
    controller.stage_steps = stage_steps;
    controller.bytes_since_increase = row[G_DCQCN_BYTES_SINCE_INCREASE];
    controller.next_control_time_ns = row[G_DCQCN_NEXT_CONTROL];
    Ok(())
}

/// Writes one DCQCN notification point into its flow's 7-word receiver row.
pub(crate) fn encode_dcqcn_receiver(receiver: &DcqcnReceiverState, row: &mut [u64]) {
    row.fill(0);
    match receiver.last_cnp_time_ns {
        None => row[0] = DCQCN_RECEIVER_NO_CNP,
        Some(last) => {
            row[0] = DCQCN_RECEIVER_LAST_CNP;
            row[DR_LAST_CNP] = last;
        }
    }
    row[DR_CNP_INTERVAL] = receiver.cnp_interval_ns;
    row[DR_CNP_SIZE] = receiver.cnp_size_bytes;
}

/// Restores one DCQCN notification point from its flow's receiver row.
pub(crate) fn decode_dcqcn_receiver(
    row: &[u64],
    receiver: &mut DcqcnReceiverState,
) -> Result<(), &'static str> {
    if row[DR_CNP_INTERVAL] != receiver.cnp_interval_ns
        || row[DR_CNP_SIZE] != receiver.cnp_size_bytes
        || row[4..7].iter().any(|word| *word != 0)
    {
        return Err("DCQCN receiver row changed immutable configuration");
    }
    receiver.last_cnp_time_ns = match row[0] {
        DCQCN_RECEIVER_NO_CNP => None,
        DCQCN_RECEIVER_LAST_CNP => Some(row[DR_LAST_CNP]),
        _ => return Err("DCQCN receiver row carries an unknown marker"),
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DcqcnController, DcqcnControllerConfig, FlowId, PayloadId, RateGenerator};

    fn generator() -> DcqcnGenerator {
        let mut controller = DcqcnController::new(
            DcqcnControllerConfig {
                initial_rate_bps: 10,
                minimum_rate_bps: 1,
                maximum_rate_bps: 20,
                additive_rate_bps: 2,
                hyper_rate_bps: 3,
                g_ppb: 4,
                decrease_ppb: 5,
                cnp_interval_ns: 6,
                control_interval_ns: 7,
                increase_byte_threshold: 8,
            },
            9,
        )
        .expect("valid controller");
        controller.alpha_ppb = 11;
        controller.current_rate_bps = 12;
        controller.target_rate_bps = 13;
        controller.cnp_seen = true;
        controller.last_cnp_time_ns = Some(u64::MAX);
        controller.stage = DcqcnIncreaseStage::Additive;
        controller.stage_steps = 4;
        controller.bytes_since_increase = 14;
        DcqcnGenerator {
            rate: RateGenerator {
                first_pacing_time_ns: 15,
                pacing_interval_ns: 16,
                packet_size_bytes: 17,
                total_bytes: 18,
                rate_numerator_bits_per_second: 12,
                rate_denominator: 19,
                credit_quanta: (u128::from(u64::MAX) << 64) | 20,
            },
            controller,
            control_timer_payload: PayloadId(21),
            cnp_size_bytes: 22,
        }
    }

    fn host(generators: Vec<crate::FlowGeneratorState>) -> crate::HostState {
        crate::HostState {
            egress_link: crate::LinkId(0),
            queue: std::collections::VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
            generators,
            stages: Vec::new(),
            tcp_receivers: Vec::new(),
            dcqcn_receivers: Vec::new(),
            roce_receivers: None,
            pfc: None,
            next_origin_seq: 0,
            next_payload_seq: 0,
            sourced_packets: 0,
            departed_packets: 0,
            received_packets: 0,
        }
    }

    fn generator_state(kind: FlowGeneratorKind) -> crate::FlowGeneratorState {
        crate::FlowGeneratorState {
            flow: FlowId(0),
            packets_emitted: 0,
            bytes_emitted: 0,
            next_emission: crate::ScheduledEmission {
                status: crate::GeneratorStatus::Finished,
                departure_time_ns: 0,
                payload: PayloadId(0),
            },
            rng_state: 0,
            feedback: crate::GeneratorFeedbackState {
                arrivals: 0,
                outstanding_bytes: 0,
                unacknowledged_bytes: 0,
            },
            kind,
        }
    }

    fn image(host_states: Vec<crate::HostState>, packets: Vec<PacketKind>) -> SimulationImage {
        SimulationImage {
            stop_time_ns: 0,
            nodes: Vec::new(),
            host_states,
            switch_states: Vec::new(),
            flows: Vec::new(),
            initial_packets: packets
                .into_iter()
                .enumerate()
                .map(|(index, kind)| crate::PacketDescriptor {
                    id: PayloadId(index as u64),
                    flow: FlowId(0),
                    size_bytes: 0,
                    ecn_marked: false,
                    kind,
                })
                .collect(),
            links: Vec::new(),
            channels: Vec::new(),
            initial_events: Vec::new(),
            seed: 0,
        }
    }

    /// Each source of DCQCN state sets exactly its bits; an image without any plans zero.
    #[test]
    fn mechanism_flags_follow_the_image_dcqcn_state() {
        let rate = generator().rate;
        assert_eq!(
            mechanism_flags(&image(vec![host(Vec::new())], Vec::new())),
            0
        );
        assert_eq!(
            mechanism_flags(&image(
                vec![host(vec![generator_state(FlowGeneratorKind::Rate(rate))])],
                vec![PacketKind::Data, PacketKind::Feedback],
            )),
            0
        );

        let mut receiver_host = host(Vec::new());
        receiver_host.dcqcn_receivers.push(DcqcnReceiverState {
            flow: FlowId(0),
            cnp_interval_ns: 1,
            cnp_size_bytes: 64,
            last_cnp_time_ns: None,
        });
        assert_eq!(
            mechanism_flags(&image(vec![host(Vec::new()), receiver_host], Vec::new())),
            MECHANISM_DCQCN_RECEIVERS | MECHANISM_DCQCN
        );

        assert_eq!(
            mechanism_flags(&image(
                vec![host(vec![generator_state(FlowGeneratorKind::Dcqcn(
                    generator()
                ))])],
                Vec::new(),
            )),
            MECHANISM_DCQCN
        );
        for resident in [
            PacketKind::DcqcnCnp(crate::DcqcnCnpHeader {
                trigger_payload: PayloadId(0),
            }),
            PacketKind::DcqcnControlTimer,
        ] {
            assert_eq!(
                mechanism_flags(&image(vec![host(Vec::new())], vec![resident])),
                MECHANISM_DCQCN,
                "{resident:?}"
            );
        }
    }

    fn pfc_queue() -> crate::SwitchQueueState {
        crate::SwitchQueueState {
            egress_link: Some(crate::LinkId(0)),
            scheduler: crate::SchedulerKind::Fifo,
            queue_capacity_packets: 0,
            drop_mark: crate::DropMarkPolicy::TailDrop,
            pfc: Some(crate::PfcQueueState {
                paused_by_controller: Default::default(),
                ingresses: Vec::new(),
            }),
            queue: std::collections::VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
        }
    }

    /// One switch LP whose only queue is `queue`.
    fn switch_image(queue: crate::SwitchQueueState) -> SimulationImage {
        let mut image = image(Vec::new(), Vec::new());
        image.nodes.push(crate::NodeDescriptor {
            id: crate::NodeId(0),
            kind: crate::NodeKind::Switch,
            state_slot: 0,
        });
        image.switch_states.push(crate::SwitchState {
            physical_switch: 0,
            queues: vec![queue],
            next_origin_seq: 0,
            arrived_packets: 0,
            dropped_packets: 0,
            departed_packets: 0,
        });
        image
    }

    /// PFC state is exactly the planner's PFC-region condition; a resident PFC frame also counts.
    #[test]
    fn mechanism_flags_follow_the_image_pfc_state() {
        let mut plain = pfc_queue();
        plain.pfc = None;
        assert_eq!(mechanism_flags(&switch_image(plain.clone())), 0);
        assert!(!crate::device_pfc::image_has_pfc(&switch_image(plain)));

        let pfc = switch_image(pfc_queue());
        assert!(crate::device_pfc::image_has_pfc(&pfc));
        assert_eq!(mechanism_flags(&pfc), MECHANISM_PFC);

        let frame = image(
            vec![host(Vec::new())],
            vec![PacketKind::Pfc(crate::PfcHeader {
                controlled_link: crate::LinkId(0),
                priority: 3,
                pause: true,
            })],
        );
        assert_eq!(mechanism_flags(&frame), MECHANISM_PFC);
    }

    /// Every source of DCQCN or PFC state selects the mechanisms kernel; nothing else does.
    #[test]
    fn every_mechanism_source_selects_the_mechanisms_round_kernel() {
        let rate = generator().rate;
        let plain = [
            image(vec![host(Vec::new())], Vec::new()),
            image(
                vec![host(vec![generator_state(FlowGeneratorKind::Rate(rate))])],
                vec![PacketKind::Data, PacketKind::Feedback],
            ),
            switch_image(crate::SwitchQueueState {
                pfc: None,
                ..pfc_queue()
            }),
        ];
        for image in &plain {
            assert_eq!(RoundKernel::for_image(image), RoundKernel::Plain);
        }

        let mut receiver_host = host(Vec::new());
        receiver_host.dcqcn_receivers.push(DcqcnReceiverState {
            flow: FlowId(0),
            cnp_interval_ns: 1,
            cnp_size_bytes: 64,
            last_cnp_time_ns: Some(5),
        });
        let sources = [
            (
                "DCQCN generator",
                image(
                    vec![host(vec![generator_state(FlowGeneratorKind::Dcqcn(
                        generator(),
                    ))])],
                    Vec::new(),
                ),
            ),
            (
                "DCQCN receiver only",
                image(vec![host(Vec::new()), receiver_host], Vec::new()),
            ),
            (
                "resident CNP",
                image(
                    vec![host(Vec::new())],
                    vec![PacketKind::DcqcnCnp(crate::DcqcnCnpHeader {
                        trigger_payload: PayloadId(0),
                    })],
                ),
            ),
            (
                "resident control timer",
                image(vec![host(Vec::new())], vec![PacketKind::DcqcnControlTimer]),
            ),
            ("PFC queue state", switch_image(pfc_queue())),
            (
                "resident PFC frame",
                image(
                    vec![host(Vec::new())],
                    vec![PacketKind::Pfc(crate::PfcHeader {
                        controlled_link: crate::LinkId(0),
                        priority: 0,
                        pause: false,
                    })],
                ),
            ),
        ];
        for (name, image) in &sources {
            assert_eq!(
                RoundKernel::for_image(image),
                RoundKernel::Mechanisms,
                "{name}"
            );
        }
    }

    /// A two-LP, two-flow plan with every arena populated: live records of plain kinds, and free or
    /// dead slots holding PFC, CNP and control-timer kinds, which the check must not see.
    struct PlanFixture {
        pfc_offset: u64,
        receiver_offset: u64,
        node_state: Vec<u64>,
        generators: Vec<u64>,
        flows: Vec<u64>,
        fel_meta: Vec<u64>,
        fel_records: Vec<u64>,
        queue_meta: Vec<u64>,
        queue_records: Vec<u64>,
        in_service: Vec<u64>,
        stream_state: Vec<u64>,
        stream_records: Vec<u64>,
        tcp_state: Vec<u64>,
    }

    const T_NONE: u64 = u64::MAX;
    const T_EVENT: usize = crate::device_event_record::EVENT_WORDS;
    const T_ECN: u64 = 1 << 63;
    // Stream layout: channel stream 0; service streams 1 and 2; generator streams 3 and 4.
    const T_SERVICE_BASE: usize = 1;
    const T_GENERATOR_BASE: usize = 3;
    const T_STREAMS: usize = 5;
    const T_CHANNEL_WORDS: usize = crate::device_event_record::CHANNEL_EVENT_WORDS;
    const T_GENERATOR_EVENT_WORDS: usize = crate::device_event_record::GENERATOR_EVENT_WORDS;
    const T_GENERATOR_STREAM_OFFSET: usize = 2 * T_CHANNEL_WORDS;

    /// A 14-word event record of `event_kind` carrying packet kind `packet_kind`.
    fn record(event_kind: crate::EventKind, packet_kind: u64) -> [u64; T_EVENT] {
        let mut record = [0_u64; T_EVENT];
        record[5] = event_kind as u64;
        record[10] = packet_kind;
        record
    }

    fn stored(
        event_kind: crate::EventKind,
        packet_kind: u64,
        class: crate::device_event_record::StoredEventClass,
    ) -> Vec<u64> {
        let encoded =
            crate::device_event_record::encode_event_record(record(event_kind, packet_kind), class);
        encoded.words[..encoded.len].to_vec()
    }

    impl PlanFixture {
        fn clean() -> Self {
            use crate::EventKind::{PacketArrival, RemoteArrival, TxReady};
            use crate::device_event_record::StoredEventClass::{Channel, Generator};
            let mut generators = vec![0_u64; 2 * 43];
            // Flow 0: a valid TCP generator at LP 0. Flow 1: an invalid row tagged DCQCN.
            generators[0] = 1;
            generators[1] = 0;
            generators[11] = 1;
            generators[43 + 11] = GENERATOR_KIND_DCQCN;
            let mut flows = vec![0_u64; 2 * 6];
            flows[1] = 1;
            flows[6] = 1;
            flows[6 + 1] = 0;
            // LP 0's heap: one live record, then a dead CNP past its count.
            let fel_meta = vec![0, 4, 0, 1, 4, 4, 0, 0];
            let mut fel_records = vec![0_u64; 8 * T_EVENT];
            fel_records[..T_EVENT].copy_from_slice(&record(TxReady, 0));
            fel_records[T_EVENT..2 * T_EVENT].copy_from_slice(&record(RemoteArrival, 5));
            // LP 1's queue: a 3-slot ring with head 2 and count 2, so slots 2 and 0 are live and
            // slot 1 is free and holds a stale PFC frame.
            let queue_meta = vec![0, 0, 0, 0, 0, 0, 3, 2, 2, 0];
            let mut queue_records = vec![0_u64; 3 * T_EVENT];
            queue_records[T_EVENT + 10] = 4;
            // LP 0's in-service row holds a control-timer kind, but its service is not valid.
            let mut in_service = vec![0_u64; 2 * T_EVENT];
            in_service[10] = 6;
            // Channel stream 0: 2 slots, head 1, count 1: slot 1 live (data), slot 0 free (CNP).
            // Generator stream 3 (flow 0): 2 slots, head 0, count 1: slot 0 live, slot 1 free.
            let mut stream_state = vec![0_u64; T_STREAMS * 4];
            stream_state[..4].copy_from_slice(&[0, 2, 1, 1]);
            stream_state[3 * 4..4 * 4].copy_from_slice(&[
                T_GENERATOR_STREAM_OFFSET as u64,
                2,
                0,
                1,
            ]);
            let mut stream_records = Vec::new();
            stream_records.extend(stored(RemoteArrival, 5, Channel));
            stream_records.extend(stored(RemoteArrival, 0, Channel));
            stream_records.extend(stored(PacketArrival, 0, Generator));
            stream_records.extend(stored(PacketArrival, 6, Generator));
            assert_eq!(
                stream_records.len(),
                T_GENERATOR_STREAM_OFFSET + 2 * T_GENERATOR_EVENT_WORDS
            );
            // Receiver rows at offset 0: flow 0 a TCP receiver (marker 1), flow 1 unused.
            let mut tcp_state = vec![0_u64; 2 * 7];
            tcp_state[0] = 1;
            Self {
                pfc_offset: T_NONE,
                receiver_offset: 0,
                node_state: vec![0_u64; 2 * 11],
                generators,
                flows,
                fel_meta,
                fel_records,
                queue_meta,
                queue_records,
                in_service,
                stream_state,
                stream_records,
                tcp_state,
            }
        }

        fn refusal(&self) -> Option<PlainKernelRefusal> {
            plain_round_kernel_refusal(&UploadedPlan {
                pfc_offset: self.pfc_offset,
                receiver_offset: self.receiver_offset,
                node_count: 2,
                flow_count: 2,
                node_state: &self.node_state,
                generators: &self.generators,
                flows: &self.flows,
                fel_meta: &self.fel_meta,
                fel_records: &self.fel_records,
                queue_meta: &self.queue_meta,
                queue_records: &self.queue_records,
                in_service: &self.in_service,
                stream_state: &self.stream_state,
                stream_records: &self.stream_records,
                stream_count: T_STREAMS,
                service_stream_base: T_SERVICE_BASE,
                generator_stream_base: T_GENERATOR_BASE,
                tcp_state: &self.tcp_state,
            })
        }
    }

    /// Dead heap slots, free ring slots, an invalid in-service row, an invalid DCQCN-tagged
    /// generator row and a TCP receiver marker are not mechanism state.
    #[test]
    fn a_plain_plan_with_stale_mechanism_words_in_free_slots_is_accepted() {
        assert_eq!(PlanFixture::clean().refusal(), None);
    }

    #[test]
    fn a_pfc_region_refuses_the_plain_kernel() {
        let mut plan = PlanFixture::clean();
        plan.pfc_offset = 7;
        assert_eq!(plan.refusal(), Some(PlainKernelRefusal::PfcRegion));
        assert_eq!(PlainKernelRefusal::PfcRegion.node(), None);
    }

    #[test]
    fn a_valid_dcqcn_generator_row_refuses_the_plain_kernel() {
        let mut plan = PlanFixture::clean();
        plan.generators[43] = 1;
        plan.generators[43 + 1] = 1;
        let refusal = plan.refusal();
        assert_eq!(
            refusal,
            Some(PlainKernelRefusal::DcqcnGenerator { flow: 1, owner: 1 })
        );
        assert_eq!(refusal.unwrap().node(), Some(crate::NodeId(1)));
    }

    #[test]
    fn a_dcqcn_receiver_marker_refuses_the_plain_kernel_only_with_a_receiver_region() {
        for marker in [DCQCN_RECEIVER_NO_CNP, DCQCN_RECEIVER_LAST_CNP, 7] {
            let mut plan = PlanFixture::clean();
            plan.tcp_state[7] = marker;
            let refusal = plan.refusal();
            assert_eq!(
                refusal,
                Some(PlainKernelRefusal::DcqcnReceiver { flow: 1, target: 0 }),
                "marker {marker}"
            );
            assert_eq!(refusal.unwrap().node(), Some(crate::NodeId(0)));
            plan.receiver_offset = T_NONE;
            assert_eq!(plan.refusal(), None, "no receiver region, marker {marker}");
        }
    }

    /// A live record of each mechanism kind, with and without the ECN flag, in each arena.
    #[test]
    fn a_live_mechanism_packet_in_any_arena_refuses_the_plain_kernel() {
        use crate::EventKind::{PacketArrival, RemoteArrival};
        use crate::device_event_record::StoredEventClass::{Channel, Generator};
        for kind in [4_u64, 5, 6] {
            for flag in [0, T_ECN] {
                let packet_kind = kind | flag;
                let expect =
                    |arena, lp| Some(PlainKernelRefusal::MechanismPacket { arena, lp, kind });

                let mut plan = PlanFixture::clean();
                plan.fel_records[10] = packet_kind;
                assert_eq!(plan.refusal(), expect(PlanArena::FallbackHeap, Some(0)));

                let mut plan = PlanFixture::clean();
                plan.queue_records[10] = packet_kind;
                assert_eq!(plan.refusal(), expect(PlanArena::Queue, Some(1)));
                let mut plan = PlanFixture::clean();
                plan.queue_records[2 * T_EVENT + 10] = packet_kind;
                assert_eq!(plan.refusal(), expect(PlanArena::Queue, Some(1)));

                let mut plan = PlanFixture::clean();
                plan.node_state[4] = 1;
                plan.in_service[10] = packet_kind;
                assert_eq!(plan.refusal(), expect(PlanArena::InService, Some(0)));

                let mut plan = PlanFixture::clean();
                plan.stream_records[T_CHANNEL_WORDS..2 * T_CHANNEL_WORDS].copy_from_slice(&stored(
                    RemoteArrival,
                    packet_kind,
                    Channel,
                ));
                assert_eq!(plan.refusal(), expect(PlanArena::ChannelStream, None));

                let mut plan = PlanFixture::clean();
                plan.stream_records[T_GENERATOR_STREAM_OFFSET
                    ..T_GENERATOR_STREAM_OFFSET + T_GENERATOR_EVENT_WORDS]
                    .copy_from_slice(&stored(PacketArrival, packet_kind, Generator));
                // Generator stream 3 is flow 0, whose source is LP 0.
                assert_eq!(plan.refusal(), expect(PlanArena::GeneratorStream, Some(0)));
            }
        }
    }

    /// A meta that points outside its plane refuses, rather than being skipped.
    #[test]
    fn a_plan_whose_metas_point_outside_their_planes_is_refused() {
        // LP 1's heap starts at slot 4 of 8; a count of 9 runs past the plane.
        let mut plan = PlanFixture::clean();
        plan.fel_meta[4 + 3] = 9;
        assert_eq!(plan.refusal(), Some(PlainKernelRefusal::MalformedPlan));
        let mut plan = PlanFixture::clean();
        plan.stream_state[0] = 1_000;
        assert_eq!(plan.refusal(), Some(PlainKernelRefusal::MalformedPlan));
        let mut plan = PlanFixture::clean();
        plan.receiver_offset = 1_000;
        assert_eq!(plan.refusal(), Some(PlainKernelRefusal::MalformedPlan));
    }

    /// A plan without streams (streams disabled) still has its other arenas checked.
    #[test]
    fn a_streamless_plan_is_checked_without_streams() {
        let mut plan = PlanFixture::clean();
        plan.fel_records[10] = 5;
        let refusal = plain_round_kernel_refusal(&UploadedPlan {
            pfc_offset: plan.pfc_offset,
            receiver_offset: plan.receiver_offset,
            node_count: 2,
            flow_count: 2,
            node_state: &plan.node_state,
            generators: &plan.generators,
            flows: &plan.flows,
            fel_meta: &plan.fel_meta,
            fel_records: &plan.fel_records,
            queue_meta: &plan.queue_meta,
            queue_records: &plan.queue_records,
            in_service: &plan.in_service,
            stream_state: &[0],
            stream_records: &[0],
            stream_count: 0,
            service_stream_base: 0,
            generator_stream_base: 0,
            tcp_state: &plan.tcp_state,
        });
        assert_eq!(
            refusal,
            Some(PlainKernelRefusal::MechanismPacket {
                arena: PlanArena::FallbackHeap,
                lp: Some(0),
                kind: 5,
            })
        );
    }

    /// The word indices the check uses are the kernels' own.
    #[test]
    fn plan_check_word_indices_match_both_kernels() {
        for (backend, source, declare) in [
            ("CUDA", include_str!("cuda_kernels.cu"), "constexpr"),
            ("Metal", include_str!("metal_kernels.metal"), "constant"),
        ] {
            for (name, value) in [
                ("uint G_VALID", G_VALID),
                ("uint G_OWNER", G_OWNER),
                ("uint G_KIND", G_KIND),
                ("uint GENERATOR_WORDS", PLAN_GENERATOR_WORDS),
                ("uint NODE_WORDS", PLAN_NODE_WORDS),
                ("uint N_SERVICE_VALID", N_SERVICE_VALID),
                ("uint FLOW_WORDS", PLAN_FLOW_WORDS),
                ("uint QUEUE_META_WORDS", PLAN_QUEUE_META_WORDS),
                ("uint META_WORDS", PLAN_ARENA_META_WORDS),
                ("uint TCP_RECEIVER_WORDS", PLAN_TCP_RECEIVER_WORDS),
                ("uint PK_KIND", PK_KIND),
                ("ulong PFC_PACKET", PFC_PACKET as usize),
                ("ulong DCQCN_CNP_PACKET", DCQCN_CNP_PACKET as usize),
                (
                    "ulong DCQCN_CONTROL_TIMER_PACKET",
                    DCQCN_CONTROL_TIMER_PACKET as usize,
                ),
            ] {
                let line = format!("{declare} {name} = {value};");
                assert!(source.contains(&line), "{backend}: `{line}`");
            }
        }
    }

    #[test]
    fn dcqcn_generator_rows_round_trip_every_mutable_word() {
        let original = generator();
        let mut row = [0_u64; 43];
        encode_dcqcn_generator(&original, &mut row);
        let mut decoded = original;
        decoded.controller.alpha_ppb = 0;
        decoded.controller.last_cnp_time_ns = None;
        decoded.controller.stage = DcqcnIncreaseStage::Hyper;
        decoded.rate.credit_quanta = 0;
        decode_dcqcn_generator(&row, &mut decoded).expect("row must decode");
        assert_eq!(decoded, original);

        row[G_DCQCN_G] += 1;
        assert!(decode_dcqcn_generator(&row, &mut decoded).is_err());
        row[G_DCQCN_G] -= 1;
        row[G_DCQCN_STAGE] = 3;
        assert!(decode_dcqcn_generator(&row, &mut decoded).is_err());
    }

    #[test]
    fn dcqcn_receiver_rows_round_trip_and_keep_range_metadata_zero() {
        for last in [None, Some(0), Some(u64::MAX)] {
            let original = DcqcnReceiverState {
                flow: FlowId(3),
                cnp_interval_ns: 5,
                cnp_size_bytes: 64,
                last_cnp_time_ns: last,
            };
            let mut row = [7_u64; 7];
            encode_dcqcn_receiver(&original, &mut row);
            assert_eq!(&row[4..7], &[0, 0, 0]);
            let mut decoded = DcqcnReceiverState {
                last_cnp_time_ns: Some(1),
                ..original
            };
            decode_dcqcn_receiver(&row, &mut decoded).expect("row must decode");
            assert_eq!(decoded, original);
        }
    }
}
