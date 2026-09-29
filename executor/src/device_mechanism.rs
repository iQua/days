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
