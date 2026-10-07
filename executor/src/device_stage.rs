//! The device stage region (P16 G1, `days-gpu/evidence/P16/colldev-design.md` §1.3), shared by
//! the Metal and CUDA planners and readbacks. The word layout is mirrored by `cuda_kernels.cu` and
//! `metal_kernels.metal`.
//!
//! The region is a part of `tcp_state` just before the RoCE region, at `params[P_STAGE_OFFSET]`,
//! planned only when some host carries a stage (`MECHANISM_STAGES`). It holds one
//! [`STAGE_ROW_WORDS`]-word row per flow, indexed by `FlowId`, then the successor array.
//!
//! **Row.** Word [`SR_FLAGS`] carries the two mutable completion bits ([`SR_LOCAL_COMPLETE`],
//! [`SR_INBOUND_COMPLETE`]) and three immutable bits ([`SR_HAS_LOCAL`], [`SR_HAS_INBOUND`],
//! [`SR_IS_STAGE`]); [`SR_INBOUND_RECEIVED`] is the mutable inbound byte count;
//! [`SR_INBOUND_REQUIRED`] the immutable inbound requirement. [`SR_LOCAL_SUCCESSORS`] and
//! [`SR_INBOUND_SUCCESSORS`] are `offset << 32 | count` into the successor array (offsets relative
//! to the region start) for the stages whose local or inbound predecessor is this flow, in
//! ascending `FlowId`, which is generator-position order on their host. A flow that is not a stage
//! has a zero row; only stages are predecessors.
//!
//! **Not stored.** `activated` equals `prerequisites_complete` at every event boundary (the
//! validator pins it on every stage, `validate.rs`), so readback derives it. The predecessor flow
//! ids are implied by list membership and come from the image. A compute stage's duration is its
//! Constant generator's interval word (the validator pins the two equal).
//!
//! **Joins (P16 H1).** A stage with several local predecessors ([`SR_LOCAL_JOIN`]) counts them in
//! its flags word: bits 48..63 hold the requirement, bits 32..47 those complete. A stage with
//! several inbound predecessors ([`SR_INBOUND_JOIN`]) sums their in-order deliveries in
//! [`SR_INBOUND_RECEIVED`]. Its predecessor `P` cannot set the count to its frontier, so `P`'s
//! inbound list starts with one credit word, `P`'s frontier as last credited to its successors
//! ([`SR_INBOUND_CREDIT`] on `P`'s row; the list word's offset points past it); each delivery adds
//! the frontier's advance over it. A predecessor whose completion has been credited to its local
//! successors carries [`SR_LOCAL_CREDITED`], so later ACKs of a finished flow count nothing twice.
//! Neither credit is image state: both equal transport predicates at every boundary, so encoding
//! derives them and readback ignores them.
//!
//! **Ownership.** Every mutable word of stage `S` is written only by `S`'s source host: a local
//! predecessor completes at that host, and an inbound predecessor delivers to it. `P`'s
//! [`SR_LOCAL_CREDITED`] bit is written by `P`'s source and `P`'s credit word by `P`'s target, the
//! hosts where those events happen. Lists and immutable words are read-only after upload.

use crate::{CollectiveStage, HostState, SimulationImage, StageDependencies, StagePredecessors};

/// Words of one flow's row in the stage region. The host projection sizes the region from the
/// same constant (`device_sizing::stage_region_words`).
pub(crate) const STAGE_ROW_WORDS: usize = crate::device_sizing::STAGE_ROW_WORDS;
pub(crate) const SR_FLAGS: usize = 0;
pub(crate) const SR_INBOUND_RECEIVED: usize = 1;
pub(crate) const SR_INBOUND_REQUIRED: usize = 2;
pub(crate) const SR_LOCAL_SUCCESSORS: usize = 3;
pub(crate) const SR_INBOUND_SUCCESSORS: usize = 4;
/// Mutable: the local predecessor is complete.
pub(crate) const SR_LOCAL_COMPLETE: u64 = 1;
/// Mutable: the inbound predecessor delivered its required bytes.
pub(crate) const SR_INBOUND_COMPLETE: u64 = 2;
/// Immutable: the stage has a local predecessor.
pub(crate) const SR_HAS_LOCAL: u64 = 16;
/// Immutable: the stage has an inbound predecessor.
pub(crate) const SR_HAS_INBOUND: u64 = 32;
/// Immutable: the row's flow is a stage.
pub(crate) const SR_IS_STAGE: u64 = 64;
/// Immutable: the stage counts several local predecessors in bits 32..63 of its flags.
pub(crate) const SR_LOCAL_JOIN: u64 = 128;
/// Immutable: the stage sums several inbound predecessors' deliveries.
pub(crate) const SR_INBOUND_JOIN: u64 = 256;
/// Mutable, written by this flow's source: its completion was credited to its local successors.
pub(crate) const SR_LOCAL_CREDITED: u64 = 512;
/// Immutable: this flow's inbound successor list is preceded by its credit word.
pub(crate) const SR_INBOUND_CREDIT: u64 = 1024;
/// A join's local count fields: completed in bits 32..47, required in bits 48..63.
const SR_JOIN_COMPLETED_SHIFT: u32 = 32;
const SR_JOIN_REQUIRED_SHIFT: u32 = 48;
/// The largest local join a device row counts.
pub(crate) const SR_JOIN_MAX: u32 = 0xffff;
const SR_IMMUTABLE_BITS: u64 = SR_HAS_LOCAL
    | SR_HAS_INBOUND
    | SR_IS_STAGE
    | SR_LOCAL_JOIN
    | SR_INBOUND_JOIN
    | SR_INBOUND_CREDIT;
const SR_KNOWN_BITS: u64 =
    SR_LOCAL_COMPLETE | SR_INBOUND_COMPLETE | SR_LOCAL_CREDITED | SR_IMMUTABLE_BITS;

/// Whether any host carries a stage: exactly when the region is planned.
pub(crate) fn image_has_stages(image: &SimulationImage) -> bool {
    image
        .host_states
        .iter()
        .any(|state| !state.stages.is_empty())
}

/// Every `(position, stage)` of a host's stage table.
fn host_stages(state: &HostState) -> impl Iterator<Item = (usize, CollectiveStage)> + '_ {
    (0..state.stages.len()).filter_map(|position| state.stage(position).map(|s| (position, s)))
}

/// The row words of one stage.
fn encode_row(stage: CollectiveStage, row: &mut [u64]) {
    let dependencies = stage.dependencies;
    let mut flags = SR_IS_STAGE;
    if dependencies.local != StagePredecessors::None {
        flags |= SR_HAS_LOCAL;
    }
    if dependencies.inbound != StagePredecessors::None {
        flags |= SR_HAS_INBOUND;
    }
    if dependencies.local_complete() {
        flags |= SR_LOCAL_COMPLETE;
    }
    if dependencies.inbound_complete() {
        flags |= SR_INBOUND_COMPLETE;
    }
    if let StagePredecessors::Join { count, .. } = dependencies.local {
        flags |= SR_LOCAL_JOIN
            | (u64::from(dependencies.local_completed) << SR_JOIN_COMPLETED_SHIFT)
            | (u64::from(count) << SR_JOIN_REQUIRED_SHIFT);
    }
    if let StagePredecessors::Join { .. } = dependencies.inbound {
        flags |= SR_INBOUND_JOIN;
    }
    row[SR_FLAGS] = flags;
    row[SR_INBOUND_RECEIVED] = dependencies.inbound_bytes_received;
    row[SR_INBOUND_REQUIRED] = dependencies.inbound_predecessor_bytes;
}

/// The whole region of `image`: rows, then the successor array; empty without stages.
/// `has_stages` is [`image_has_stages`], decided once by the planner (P16 G2).
///
/// Fails when an offset or count does not fit the 32-bit halves of a list word, or a predecessor
/// names a flow outside the image (both rejected before upload, never truncated).
pub(crate) fn encode_stage_region(
    image: &SimulationImage,
    has_stages: bool,
) -> Result<Vec<u64>, &'static str> {
    debug_assert_eq!(has_stages, image_has_stages(image));
    if !has_stages {
        return Ok(Vec::new());
    }
    let flow_count = image.flows.len().max(1);
    let rows = flow_count
        .checked_mul(STAGE_ROW_WORDS)
        .ok_or("stage region rows overflow usize")?;
    let mut local = Vec::new();
    let mut inbound = Vec::new();
    let mut region = vec![0_u64; rows];
    for state in &image.host_states {
        for (position, stage) in host_stages(state) {
            let flow = state.generators[position].flow;
            let index = usize::try_from(flow.0).map_err(|_| "stage flow id exceeds usize")?;
            if index >= flow_count {
                return Err("stage flow id is outside the image");
            }
            encode_row(
                stage,
                &mut region[index * STAGE_ROW_WORDS..(index + 1) * STAGE_ROW_WORDS],
            );
            if let StagePredecessors::Join { count, .. } = stage.dependencies.local {
                if count > SR_JOIN_MAX {
                    return Err("a stage join exceeds the device's local count");
                }
            }
            let joined = matches!(stage.dependencies.inbound, StagePredecessors::Join { .. });
            for predecessor in stage.dependencies.local.iter(&image.stage_joins) {
                local.push((predecessor, flow, false));
            }
            for predecessor in stage.dependencies.inbound.iter(&image.stage_joins) {
                inbound.push((predecessor, flow, joined));
            }
            // A predecessor's local credit: its completion was counted (it finished).
            if state.generators[position].next_emission.status == crate::GeneratorStatus::Finished {
                region[index * STAGE_ROW_WORDS + SR_FLAGS] |= SR_LOCAL_CREDITED;
            }
        }
    }
    // Pairs are unique (a predecessor names a successor once), so the unstable sort is
    // deterministic; each predecessor's successors come out ascending.
    local.sort_unstable();
    inbound.sort_unstable();
    for (pairs, word) in [
        (&local, SR_LOCAL_SUCCESSORS),
        (&inbound, SR_INBOUND_SUCCESSORS),
    ] {
        let mut start = 0;
        while start < pairs.len() {
            let predecessor = pairs[start].0;
            let end = start + pairs[start..].partition_point(|(key, ..)| *key == predecessor);
            let index =
                usize::try_from(predecessor.0).map_err(|_| "predecessor flow id exceeds usize")?;
            if index >= flow_count {
                return Err("stage predecessor is outside the image");
            }
            // A predecessor of an inbound join keeps its credit word just before its list.
            if word == SR_INBOUND_SUCCESSORS && pairs[start..end].iter().any(|pair| pair.2) {
                region[index * STAGE_ROW_WORDS + SR_FLAGS] |= SR_INBOUND_CREDIT;
                region.push(
                    inbound_frontier(image, predecessor)
                        .ok_or("an inbound predecessor of a join has no receiver at its target")?,
                );
            }
            let offset =
                u32::try_from(region.len()).map_err(|_| "stage list offset exceeds u32")?;
            let count = u32::try_from(end - start).map_err(|_| "stage list count exceeds u32")?;
            region[index * STAGE_ROW_WORDS + word] = (u64::from(offset) << 32) | u64::from(count);
            region.extend(
                pairs[start..end]
                    .iter()
                    .map(|(_, successor, _)| successor.0),
            );
            start = end;
        }
    }
    Ok(region)
}

/// The in-order frontier of `flow` at its target host: TCP's next expected sequence, or a RoCE
/// queue pair's expected PSN.
fn inbound_frontier(image: &SimulationImage, flow: crate::FlowId) -> Option<u64> {
    let descriptor = image.flows.get(usize::try_from(flow.0).ok()?)?;
    let node = image
        .nodes
        .iter()
        .find(|node| node.id == descriptor.target && node.kind == crate::NodeKind::Host)?;
    let state = image.host_states.get(node.state_slot as usize)?;
    if let Some(receivers) = state.roce_receivers.as_deref() {
        if let Ok(position) = receivers.binary_search_by_key(&flow, |receiver| receiver.flow) {
            return Some(receivers[position].expected_psn);
        }
    }
    state
        .tcp_receivers
        .binary_search_by_key(&flow, |receiver| receiver.flow)
        .ok()
        .map(|position| state.tcp_receivers[position].next_expected_sequence)
}

/// Appends the stage region to `tcp_state` and returns its start, or `None` (nothing appended)
/// when no host carries a stage (`has_stages`, decided once by the planner).
pub(crate) fn append_stage_region(
    image: &SimulationImage,
    has_stages: bool,
    tcp_state: &mut Vec<u64>,
) -> Result<Option<usize>, &'static str> {
    let region = encode_stage_region(image, has_stages)?;
    if region.is_empty() {
        return Ok(None);
    }
    let start = tcp_state.len();
    tcp_state.extend_from_slice(&region);
    Ok(Some(start))
}

/// Words of the stage rows alone (the part readback decodes), zero without stages.
pub(crate) fn stage_row_words(image: &SimulationImage) -> usize {
    if image_has_stages(image) {
        image.flows.len().max(1) * STAGE_ROW_WORDS
    } else {
        0
    }
}

/// Restores every stage's dependency state from the rows read back from the region start.
///
/// `image` is the input image and `host_states` its clone being rebuilt. Each row's immutable
/// words are checked against the input (a corrupt row is an error, not trusted state); the
/// mutable state must have moved only forward from the input (completion bits are never cleared,
/// inbound bytes never decrease or exceed the requirement, and an inbound completion bit means
/// exactly that the requirement is met). `activated` is derived as `prerequisites_complete`, the
/// value the validator pins on every stage at every boundary.
pub(crate) fn decode_stage_rows(
    rows: &[u64],
    image: &SimulationImage,
    host_states: &mut [HostState],
) -> Result<(), &'static str> {
    // The predecessors of inbound joins, whose rows carry the immutable credit bit that
    // `encode_stage_region` sets; none (and no allocation) in an image without joins.
    let mut credited = Vec::new();
    if !image.stage_joins.is_empty() {
        for state in &image.host_states {
            for (_, stage) in host_stages(state) {
                if matches!(stage.dependencies.inbound, StagePredecessors::Join { .. }) {
                    credited.extend(stage.dependencies.inbound.iter(&image.stage_joins));
                }
            }
        }
        credited.sort_unstable();
        credited.dedup();
    }
    for (input, state) in image.host_states.iter().zip(host_states.iter_mut()) {
        for (position, stage) in host_stages(input) {
            let flow = input.generators[position].flow;
            let index = usize::try_from(flow.0).map_err(|_| "stage flow id exceeds usize")?;
            let row = rows
                .get(index * STAGE_ROW_WORDS..(index + 1) * STAGE_ROW_WORDS)
                .ok_or("stage region is shorter than its rows")?;
            let mut expected = [0_u64; STAGE_ROW_WORDS];
            encode_row(stage, &mut expected);
            if credited.binary_search(&flow).is_ok() {
                expected[SR_FLAGS] |= SR_INBOUND_CREDIT;
            }
            let flags = row[SR_FLAGS];
            let low = flags & 0xffff_ffff;
            let expected_low = expected[SR_FLAGS] & 0xffff_ffff;
            let required = flags >> SR_JOIN_REQUIRED_SHIFT;
            let completed = (flags >> SR_JOIN_COMPLETED_SHIFT) & u64::from(SR_JOIN_MAX);
            if low & !SR_KNOWN_BITS != 0
                || low & SR_IMMUTABLE_BITS != expected_low & SR_IMMUTABLE_BITS
                || required != expected[SR_FLAGS] >> SR_JOIN_REQUIRED_SHIFT
                || row[SR_INBOUND_REQUIRED] != expected[SR_INBOUND_REQUIRED]
            {
                return Err("stage row changed immutable words");
            }
            let before = stage.dependencies;
            let local_complete = flags & SR_LOCAL_COMPLETE != 0;
            let inbound_complete = flags & SR_INBOUND_COMPLETE != 0;
            let received = row[SR_INBOUND_RECEIVED];
            // A join counts its completed predecessors; any other stage has its completion bit.
            let local_completed = if flags & SR_LOCAL_JOIN != 0 {
                u32::try_from(completed).map_err(|_| "stage join count exceeds u32")?
            } else if local_complete {
                before.local.count()
            } else {
                0
            };
            if local_completed < before.local_completed
                || local_completed > before.local.count()
                || local_complete != (local_completed == before.local.count())
                || received < before.inbound_bytes_received
                || received > before.inbound_predecessor_bytes
                || inbound_complete != (received == before.inbound_predecessor_bytes)
            {
                return Err("stage row moved its dependency state backwards or inconsistently");
            }
            let dependencies = StageDependencies {
                local_completed,
                inbound_bytes_received: received,
                ..before
            };
            let record = state.stages[position]
                .as_mut()
                .ok_or("stage table changed shape")?;
            record.dependencies = dependencies;
            record.activated = dependencies.prerequisites_complete();
        }
    }
    Ok(())
}

/// Whether a generator's stage was unreleased in the input image and is released in the decoded
/// state (design note G6: only then may its queue pair's grid anchor change). `None` (not a stage)
/// is never released during the run.
pub(crate) fn released_during_run(
    input: Option<CollectiveStage>,
    decoded: Option<CollectiveStage>,
) -> bool {
    input.is_some_and(|stage| !stage.activated) && decoded.is_some_and(|stage| stage.activated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ComputeStage, ConstantGenerator, FlowGeneratorKind, FlowGeneratorState, FlowId,
        GeneratorFeedbackState, GeneratorStatus, GeneratorTermination, LinkId, PayloadId,
        ScheduledEmission, StageRole,
    };

    fn generator(flow: u64) -> FlowGeneratorState {
        FlowGeneratorState {
            flow: FlowId(flow),
            packets_emitted: 0,
            bytes_emitted: 0,
            next_emission: ScheduledEmission {
                status: GeneratorStatus::Blocked,
                departure_time_ns: 0,
                payload: PayloadId(0),
            },
            rng_state: 0,
            feedback: GeneratorFeedbackState {
                arrivals: 0,
                outstanding_bytes: 0,
                unacknowledged_bytes: 0,
            },
            kind: FlowGeneratorKind::Constant(ConstantGenerator {
                first_departure_ns: 0,
                interval_ns: 1,
                packet_size_bytes: 0,
                termination: GeneratorTermination::Bytes(0),
            }),
        }
    }

    fn stage(local: Option<u64>, inbound: Option<(u64, u64)>, done: bool) -> CollectiveStage {
        CollectiveStage {
            role: StageRole::Compute(ComputeStage {
                compute_id: 0,
                group_size: 2,
                rank: 0,
                duration_ns: 1,
            }),
            dependencies: StageDependencies {
                local: local.map_or(StagePredecessors::None, |flow| {
                    StagePredecessors::One(FlowId(flow))
                }),
                inbound: inbound.map_or(StagePredecessors::None, |(flow, _)| {
                    StagePredecessors::One(FlowId(flow))
                }),
                inbound_predecessor_bytes: inbound.map_or(0, |(_, bytes)| bytes),
                inbound_bytes_received: if done {
                    inbound.map_or(0, |(_, bytes)| bytes)
                } else {
                    0
                },
                local_completed: u32::from(local.is_some() && done),
            },
            activated: done,
        }
    }

    fn host(
        generators: Vec<FlowGeneratorState>,
        stages: Vec<Option<CollectiveStage>>,
    ) -> HostState {
        HostState {
            egress_link: LinkId(0),
            queue: std::collections::VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
            generators,
            stages,
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

    /// Host A owns flows 0 (root), 1 and 4 (both after 0; 4 also waits on flow 3 from host B) and
    /// 5 (no stage). Host B owns flows 2 (root) and 3 (after 2).
    fn image() -> SimulationImage {
        let a = host(
            vec![generator(0), generator(1), generator(4), generator(5)],
            vec![
                Some(stage(None, None, true)),
                Some(stage(Some(0), None, false)),
                Some(stage(Some(0), Some((3, 700)), false)),
                None,
            ],
        );
        let b = host(
            vec![generator(2), generator(3)],
            vec![
                Some(stage(None, None, true)),
                Some(stage(Some(2), None, false)),
            ],
        );
        SimulationImage {
            stop_time_ns: 0,
            nodes: Vec::new(),
            host_states: vec![a, b, host(vec![generator(6)], Vec::new())],
            switch_states: Vec::new(),
            flows: (0..7)
                .map(|id| crate::FlowDescriptor {
                    id: FlowId(id),
                    source: crate::NodeId(0),
                    target: crate::NodeId(1),
                    priority: 0,
                    feedback_priority: 0,
                    route: Vec::new(),
                    reverse_route: Vec::new(),
                })
                .collect(),
            initial_packets: Vec::new(),
            links: Vec::new(),
            channels: Vec::new(),
            initial_events: Vec::new(),
            seed: 0,
            stage_joins: Vec::new(),
            seeded_all_to_alls: Vec::new(),
        }
    }

    fn list(region: &[u64], flow: usize, word: usize) -> Vec<u64> {
        let packed = region[flow * STAGE_ROW_WORDS + word];
        let (offset, count) = ((packed >> 32) as usize, (packed & 0xffff_ffff) as usize);
        region[offset..offset + count].to_vec()
    }

    #[test]
    fn the_region_lays_out_rows_and_ascending_successor_lists() {
        let image = image();
        let region = encode_stage_region(&image, true).unwrap();
        assert_eq!(region.len(), 7 * STAGE_ROW_WORDS + 4);
        // The host projection's size of the region (P16 G2).
        assert_eq!(
            crate::device_sizing::stage_region_words(&image),
            Ok(region.len())
        );
        assert_eq!(list(&region, 0, SR_LOCAL_SUCCESSORS), vec![1, 4]);
        assert_eq!(list(&region, 2, SR_LOCAL_SUCCESSORS), vec![3]);
        assert_eq!(list(&region, 3, SR_INBOUND_SUCCESSORS), vec![4]);
        assert_eq!(region[STAGE_ROW_WORDS + SR_LOCAL_SUCCESSORS], 0);
        assert_eq!(stage_row_words(&image), 7 * STAGE_ROW_WORDS);
        assert_eq!(
            region[4 * STAGE_ROW_WORDS + SR_FLAGS],
            SR_IS_STAGE | SR_HAS_LOCAL | SR_HAS_INBOUND
        );
        assert_eq!(region[4 * STAGE_ROW_WORDS + SR_INBOUND_REQUIRED], 700);
        assert_eq!(
            region[0],
            SR_IS_STAGE | SR_LOCAL_COMPLETE | SR_INBOUND_COMPLETE
        );
        assert_eq!(&region[5 * STAGE_ROW_WORDS..6 * STAGE_ROW_WORDS], &[0; 5]);
        let mut stageless = image.clone();
        for state in &mut stageless.host_states {
            state.stages.clear();
        }
        assert_eq!(encode_stage_region(&stageless, false), Ok(Vec::new()));
        assert_eq!(stage_row_words(&stageless), 0);
        assert_eq!(crate::device_sizing::stage_region_words(&stageless), Ok(0));
        let mut words = vec![9_u64];
        assert_eq!(append_stage_region(&stageless, false, &mut words), Ok(None));
        assert_eq!(words, vec![9]);
        assert_eq!(append_stage_region(&image, true, &mut words), Ok(Some(1)));
        assert_eq!(&words[1..], &region[..]);
    }

    #[test]
    fn decoding_restores_forward_progress_and_derives_activation() {
        let image = image();
        let mut rows = encode_stage_region(&image, true).unwrap()[..7 * STAGE_ROW_WORDS].to_vec();
        let mut decoded = image.host_states.clone();
        decode_stage_rows(&rows, &image, &mut decoded).unwrap();
        assert_eq!(
            decoded, image.host_states,
            "an unchanged region decodes to the input"
        );

        // Flow 4: local complete, 300 of 700 inbound bytes: not released.
        rows[4 * STAGE_ROW_WORDS + SR_FLAGS] |= SR_LOCAL_COMPLETE;
        rows[4 * STAGE_ROW_WORDS + SR_INBOUND_RECEIVED] = 300;
        decode_stage_rows(&rows, &image, &mut decoded).unwrap();
        let flow4 = decoded[0].stage(2).unwrap();
        assert!(flow4.dependencies.local_complete() && !flow4.activated);
        assert_eq!(flow4.dependencies.inbound_bytes_received, 300);
        // All 700: released.
        rows[4 * STAGE_ROW_WORDS + SR_FLAGS] |= SR_INBOUND_COMPLETE;
        rows[4 * STAGE_ROW_WORDS + SR_INBOUND_RECEIVED] = 700;
        decode_stage_rows(&rows, &image, &mut decoded).unwrap();
        assert!(decoded[0].stage(2).unwrap().activated);
        assert!(released_during_run(
            image.host_states[0].stage(2),
            decoded[0].stage(2)
        ));
        assert!(!released_during_run(
            image.host_states[0].stage(0),
            decoded[0].stage(0)
        ));
        assert!(!released_during_run(None, None));
    }

    /// `image()` with flow 3 (host B) also joining flows 0 and 1 from host A, 500 B each, which
    /// host B receives over TCP: flows 0 and 1 carry the credit word of an inbound join's
    /// predecessor (P16 H1).
    fn join_image() -> SimulationImage {
        let mut image = image();
        image.nodes = vec![
            crate::NodeDescriptor {
                id: crate::NodeId(0),
                kind: crate::NodeKind::Host,
                state_slot: 0,
            },
            crate::NodeDescriptor {
                id: crate::NodeId(1),
                kind: crate::NodeKind::Host,
                state_slot: 1,
            },
        ];
        image.stage_joins = vec![FlowId(0), FlowId(1)];
        let b = &mut image.host_states[1];
        b.tcp_receivers = vec![
            crate::TcpReceiverState::new(FlowId(0), 40),
            crate::TcpReceiverState::new(FlowId(1), 40),
        ];
        let join = b.stages[1].as_mut().unwrap();
        join.dependencies.inbound = StagePredecessors::Join { first: 0, count: 2 };
        join.dependencies.inbound_predecessor_bytes = 1_000;
        image
    }

    #[test]
    fn a_region_with_an_inbound_join_decodes_to_its_input() {
        let image = join_image();
        let region = encode_stage_region(&image, true).unwrap();
        assert_ne!(region[SR_FLAGS] & SR_INBOUND_CREDIT, 0);
        assert_ne!(region[STAGE_ROW_WORDS + SR_FLAGS] & SR_INBOUND_CREDIT, 0);
        let rows = &region[..7 * STAGE_ROW_WORDS];
        let mut decoded = image.host_states.clone();
        decode_stage_rows(rows, &image, &mut decoded).unwrap();
        assert_eq!(decoded, image.host_states);
        // The credit bit is immutable: a row may neither drop nor gain it.
        let mut dropped = rows.to_vec();
        dropped[SR_FLAGS] &= !SR_INBOUND_CREDIT;
        assert!(decode_stage_rows(&dropped, &image, &mut image.host_states.clone()).is_err());
        let mut gained = rows.to_vec();
        gained[2 * STAGE_ROW_WORDS + SR_FLAGS] |= SR_INBOUND_CREDIT;
        assert!(decode_stage_rows(&gained, &image, &mut image.host_states.clone()).is_err());
    }

    #[test]
    fn corrupt_or_backward_rows_are_rejected() {
        let image = image();
        let rows = encode_stage_region(&image, true).unwrap()[..7 * STAGE_ROW_WORDS].to_vec();
        let corrupt = |mutate: &dyn Fn(&mut Vec<u64>)| {
            let mut rows = rows.clone();
            mutate(&mut rows);
            decode_stage_rows(&rows, &image, &mut image.host_states.clone())
        };
        let flow4 = 4 * STAGE_ROW_WORDS;
        assert!(corrupt(&|rows| rows[flow4 + SR_FLAGS] |= 1 << 10).is_err());
        assert!(corrupt(&|rows| rows[flow4 + SR_FLAGS] &= !SR_HAS_INBOUND).is_err());
        assert!(corrupt(&|rows| rows[flow4 + SR_INBOUND_REQUIRED] = 699).is_err());
        assert!(corrupt(&|rows| rows[flow4 + SR_INBOUND_RECEIVED] = 701).is_err());
        assert!(corrupt(&|rows| rows[flow4 + SR_FLAGS] |= SR_INBOUND_COMPLETE).is_err());
        assert!(corrupt(&|rows| rows[SR_FLAGS] &= !SR_LOCAL_COMPLETE).is_err());
        assert!(corrupt(&|rows| rows[STAGE_ROW_WORDS + SR_INBOUND_RECEIVED] = 1).is_err());
        assert!(decode_stage_rows(&rows[..10], &image, &mut image.host_states.clone()).is_err());
    }
}
