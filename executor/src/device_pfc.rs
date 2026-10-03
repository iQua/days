//! Device PFC plane codec shared by Metal and CUDA (P14 Lane B T4).
//!
//! PFC state rides at the end of the scheduler plane, in a region that exists only when the image
//! carries PFC state. Without it the region is absent, its params word holds [`NONE`], and no PFC
//! word is allocated.
//!
//! Region layout, as absolute word offsets from the region start `R`:
//!
//! - `R + node`: the node's PFC queue row offset (absolute in the plane), or [`NONE`].
//! - `R + node_count + flow`: the flow's PFC class word, `priority | ((feedback_priority ^
//!   priority) << 8)`. Bits 0..8 are the data class; a CNP, RoCE ACK or RoCE NACK takes
//!   `(word ^ (word >> 8)) & 0xff`, its feedback class (P15). The word equals `priority` whenever
//!   the two classes agree, so images without a separate feedback class keep every word.
//! - One row per switch queue carrying `PfcQueueState`:
//!   - `+0` controller count `C`
//!   - `+1` ingress-monitor count `I`
//!   - `+2` bitset words per priority `W` (image-wide)
//!   - `+3` absolute offset of the pause bitsets
//!   - `+4` absolute offset of the ingress records
//!   - `+5 ..+5+C` controller node ids, ascending
//!   - the pause bitsets: priority-major, `8 * W` words. Bit `b` of priority `p` is set while
//!     controller `b` asserts pause. A priority is paused while any bit of its words is set.
//!   - `I` ingress records of [`PFC_INGRESS_WORDS`] words, in image order
//!
//! - One row per host with egress pause state (`HostPfcState`, P15 host-link PFC), in the same
//!   layout with no ingress records (`+1` is zero; hosts hold no monitors), and `+4` the absolute
//!   offset of the host's queue-pair list `[Q, flow_0 .. flow_{Q-1}]`, in generator-position
//!   (`FlowId`) order. A RESUME restarts pause-parked pairs by scanning this list; the parked set
//!   itself is not stored (ruling D3): readback recomputes it with the validator's
//!   `expected_pause_parked`.
//!
//! Scalar keeps `paused_by_controller: [BTreeSet<NodeId>; 8]`. A queue's possible controllers are
//! static image data: the downstream LPs with an ingress monitor on the queue's egress link, which
//! validation requires to be the only pause holders. The bitset is therefore an exact, fixed-width
//! encoding of every reachable set.

#![cfg_attr(
    not(any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))),
    allow(dead_code)
)]

use std::collections::BTreeSet;

use crate::{
    FlowGeneratorKind, HostState, LinkId, NodeId, NodeKind, PfcQueueState, SimulationImage,
    SwitchQueueState,
};

pub(crate) const NONE: u64 = u64::MAX;

#[cfg(all(feature = "cuda", feature = "planner-test-hooks"))]
std::thread_local! {
    /// Test-only: whole-fabric PFC-state scans on this thread ([`image_has_pfc`] and
    /// [`pfc_control_lane_producers`]). Per thread, so parallel tests never share it. Its only
    /// reader is the CUDA planner hook, so it exists only in CUDA builds with planner test hooks.
    static PFC_STATE_SCANS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Test-only: counts one whole-fabric PFC-state scan on this thread.
fn count_pfc_state_scan() {
    #[cfg(all(feature = "cuda", feature = "planner-test-hooks"))]
    PFC_STATE_SCANS.with(|scans| scans.set(scans.get() + 1));
}

/// Test-only: returns this thread's PFC-state scan count and resets it to zero.
#[cfg(all(feature = "cuda", feature = "planner-test-hooks"))]
pub(crate) fn take_pfc_state_scans_for_testing() -> usize {
    PFC_STATE_SCANS.with(|scans| scans.replace(0))
}
pub(crate) const PFC_ROW_HEADER_WORDS: usize = 5;
pub(crate) const PFC_INGRESS_WORDS: usize = 43;
const INGRESS_LINK: usize = 0;
const INGRESS_TARGET: usize = 1;
const INGRESS_DELAY: usize = 2;
const INGRESS_CAPACITY: usize = 3;
const INGRESS_XOFF: usize = 11;
const INGRESS_XON: usize = 19;
const INGRESS_OCCUPANCY: usize = 27;
const INGRESS_ASSERTED: usize = 35;

/// The controllers that may hold pause on an egress link: every switch LP whose PFC ingress
/// monitors control that link, ascending by node id, plus any residual holder in `paused`.
fn link_controllers(
    image: &SimulationImage,
    egress: Option<LinkId>,
    paused: &[BTreeSet<NodeId>; 8],
) -> Vec<NodeId> {
    let mut controllers = BTreeSet::new();
    if let Some(egress) = egress {
        for node in image
            .nodes
            .iter()
            .filter(|node| node.kind == NodeKind::Switch)
        {
            let monitors = image.switch_states[node.state_slot as usize]
                .queues
                .iter()
                .filter_map(|queue| queue.pfc.as_ref())
                .flat_map(|pfc| &pfc.ingresses);
            if monitors
                .into_iter()
                .any(|ingress| ingress.controlled_link == egress)
            {
                controllers.insert(node.id);
            }
        }
    }
    // Validation admits pause state only for declared controllers; keeping any residue here makes
    // the encoding total rather than silently dropping a holder.
    controllers.extend(paused.iter().flatten().copied());
    controllers.into_iter().collect()
}

/// One row of the PFC region: a switch LP's first queue, or a host with egress pause state.
#[derive(Clone, Copy)]
enum PfcRow<'a> {
    Switch(&'a SwitchQueueState),
    Host(&'a HostState),
}

impl PfcRow<'_> {
    fn paused(&self) -> &[BTreeSet<NodeId>; 8] {
        match self {
            Self::Switch(queue) => &queue.pfc.as_ref().expect("a PFC row").paused_by_controller,
            Self::Host(state) => {
                &state
                    .pfc
                    .as_deref()
                    .expect("a PFC row")
                    .paused_by_controller
            }
        }
    }

    fn controllers(&self, image: &SimulationImage) -> Vec<NodeId> {
        let egress = match self {
            Self::Switch(queue) => queue.egress_link,
            Self::Host(state) => Some(state.egress_link),
        };
        link_controllers(image, egress, self.paused())
    }

    fn ingress_count(&self) -> usize {
        match self {
            Self::Switch(queue) => queue.pfc.as_ref().map_or(0, |pfc| pfc.ingresses.len()),
            Self::Host(_) => 0,
        }
    }
}

/// The flows of a host's queue-pair generators, in generator-position (`FlowId`) order.
fn host_queue_pairs(state: &HostState) -> impl Iterator<Item = u64> + '_ {
    state
        .generators
        .iter()
        .filter(|generator| matches!(generator.kind, FlowGeneratorKind::Roce(_)))
        .map(|generator| generator.flow.0)
}

/// Every PFC row in node order: each switch LP's first queue with PFC state (the only queue a
/// lowered LP owns and the only one the device kernels model) and each host with egress pause
/// state. One pass over the nodes.
fn pfc_rows(image: &SimulationImage) -> impl Iterator<Item = (usize, PfcRow<'_>)> {
    image.nodes.iter().filter_map(|node| {
        let lp = node.id.0 as usize;
        match node.kind {
            NodeKind::Switch => image.switch_states[node.state_slot as usize]
                .queues
                .first()
                .filter(|queue| queue.pfc.is_some())
                .map(|queue| (lp, PfcRow::Switch(queue))),
            NodeKind::Host => image
                .host_states
                .get(node.state_slot as usize)
                .filter(|state| state.pfc.is_some())
                .map(|state| (lp, PfcRow::Host(state))),
        }
    })
}

/// Whether the image carries any PFC state (switch queues or host egress), which is exactly when
/// the region exists. One pass over the nodes, decided per node.
pub(crate) fn image_has_pfc(image: &SimulationImage) -> bool {
    count_pfc_state_scan();
    pfc_rows(image).next().is_some()
}

/// `(producer, target)` LP pairs of every PFC reverse control lane.
///
/// Control frames travel on these lanes, never on a flow route, so the exchange's inbound-producer
/// sets derived from routes must include them. Empty without PFC state.
pub(crate) fn pfc_control_lane_producers(image: &SimulationImage) -> Vec<(u64, usize)> {
    count_pfc_state_scan();
    image
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .filter_map(|queue| queue.pfc.as_ref())
        .flat_map(|pfc| &pfc.ingresses)
        .filter_map(|ingress| image.channels.get(ingress.control_channel_index as usize))
        .map(|channel| (channel.source.0, channel.target.0 as usize))
        .collect()
}

/// Upper bound on PFC control frames, which are transitions the round bound must cover.
///
/// Every frame is an XOFF emitted by an admission or an XON emitted by a dequeue at an ingress
/// monitor, and every admission or dequeue belongs to one packet at one route hop. So each counted
/// packet contributes at most two frames per hop of its longer route. Zero without PFC state.
pub(crate) fn pfc_frame_transition_bound(image: &SimulationImage, counts: &[usize]) -> usize {
    if !image_has_pfc(image) {
        return 0;
    }
    image
        .flows
        .iter()
        .zip(counts)
        .fold(0_usize, |bound, (flow, count)| {
            bound.saturating_add(
                count
                    .saturating_mul(2)
                    .saturating_mul(flow.route.len().max(flow.reverse_route.len())),
            )
        })
}

/// The PFC class word of a flow: the data class, with the feedback class's difference in bits
/// 8..16 (zero whenever the classes agree).
pub(crate) fn flow_class_word(flow: &crate::FlowDescriptor) -> u64 {
    u64::from(flow.priority) | (u64::from(flow.feedback_priority ^ flow.priority) << 8)
}

/// Words the PFC region occupies: zero exactly when the image carries no PFC state.
pub(crate) fn pfc_region_words(image: &SimulationImage) -> usize {
    if !image_has_pfc(image) {
        return 0;
    }
    let rows = pfc_rows(image)
        .map(|(_, row)| (row, row.controllers(image).len()))
        .collect::<Vec<_>>();
    let bitset_words = rows
        .iter()
        .map(|(_, controllers)| controllers.div_ceil(64))
        .max()
        .unwrap_or(0)
        .max(1);
    rows.iter().fold(
        image.nodes.len().saturating_add(image.flows.len()),
        |total, (row, controllers)| {
            let tail = match row {
                PfcRow::Switch(_) => row.ingress_count().saturating_mul(PFC_INGRESS_WORDS),
                PfcRow::Host(state) => 1 + host_queue_pairs(state).count(),
            };
            total
                .saturating_add(PFC_ROW_HEADER_WORDS)
                .saturating_add(*controllers)
                .saturating_add(8 * bitset_words)
                .saturating_add(tail)
        },
    )
}

/// Appends the PFC region to the scheduler plane and returns its start, or `None` (no words
/// appended) when the image carries no PFC state.
pub(crate) fn append_pfc_region(
    image: &SimulationImage,
    words: &mut Vec<u64>,
) -> Result<Option<usize>, String> {
    if !image_has_pfc(image) {
        return Ok(None);
    }
    let node_count = image.nodes.len();
    let flow_count = image.flows.len();
    let rows = pfc_rows(image)
        .map(|(lp, row)| (lp, row, row.controllers(image)))
        .collect::<Vec<_>>();
    let bitset_words = rows
        .iter()
        .map(|(_, _, controllers)| controllers.len().div_ceil(64))
        .max()
        .unwrap_or(0)
        .max(1);

    let region = words.len();
    words.resize(region + node_count + flow_count, 0);
    words[region..region + node_count].fill(NONE);
    for (index, flow) in image.flows.iter().enumerate() {
        words[region + node_count + index] = flow_class_word(flow);
    }
    for (lp, row, controllers) in rows {
        let start = words.len();
        let bitsets = start + PFC_ROW_HEADER_WORDS + controllers.len();
        let tail = bitsets + 8 * bitset_words;
        let tail_words = match row {
            PfcRow::Switch(_) => row
                .ingress_count()
                .checked_mul(PFC_INGRESS_WORDS)
                .ok_or("PFC ingress region overflows usize")?,
            PfcRow::Host(state) => 1 + host_queue_pairs(state).count(),
        };
        let end = tail
            .checked_add(tail_words)
            .ok_or("PFC region overflows usize")?;
        words.resize(end, 0);
        words[region + lp] = start as u64;
        words[start] = controllers.len() as u64;
        words[start + 1] = row.ingress_count() as u64;
        words[start + 2] = bitset_words as u64;
        words[start + 3] = bitsets as u64;
        words[start + 4] = tail as u64;
        for (index, controller) in controllers.iter().enumerate() {
            words[start + PFC_ROW_HEADER_WORDS + index] = controller.0;
        }
        for (priority, holders) in row.paused().iter().enumerate() {
            for holder in holders {
                let bit = controllers
                    .binary_search(holder)
                    .expect("the controllers include every pause holder");
                words[bitsets + priority * bitset_words + bit / 64] |= 1_u64 << (bit % 64);
            }
        }
        match row {
            PfcRow::Switch(queue) => {
                let pfc = queue.pfc.as_ref().expect("a PFC row");
                for (index, ingress) in pfc.ingresses.iter().enumerate() {
                    let base = tail + index * PFC_INGRESS_WORDS;
                    let channel = image
                        .channels
                        .get(ingress.control_channel_index as usize)
                        .ok_or("PFC ingress references an unknown control channel")?;
                    words[base + INGRESS_LINK] = ingress.controlled_link.0;
                    words[base + INGRESS_TARGET] = channel.target.0;
                    words[base + INGRESS_DELAY] = channel.min_delay_ns;
                    for priority in 0..8 {
                        words[base + INGRESS_CAPACITY + priority] =
                            ingress.buffer_capacity_bytes[priority];
                        words[base + INGRESS_XOFF + priority] =
                            ingress.xoff_threshold_bytes[priority];
                        words[base + INGRESS_XON + priority] =
                            ingress.xon_threshold_bytes[priority];
                        words[base + INGRESS_OCCUPANCY + priority] =
                            ingress.occupancy_bytes[priority];
                        words[base + INGRESS_ASSERTED + priority] =
                            u64::from(ingress.pause_asserted[priority]);
                    }
                }
            }
            PfcRow::Host(state) => {
                let mut count = 0;
                for flow in host_queue_pairs(state) {
                    count += 1;
                    words[tail + count] = flow;
                }
                words[tail] = count as u64;
            }
        }
    }
    debug_assert_eq!(words.len() - region, pfc_region_words(image));
    Ok(Some(region))
}

/// Restores a host's egress pause sets from its PFC row (P15 host-link PFC).
///
/// The pause bitsets are device-written; the header and the queue-pair list are image data, checked
/// rather than trusted. `pause_parked` is left as it is: it is a function of the restored pause
/// sets and the queue pairs' states, which the caller recomputes once the generators are decoded
/// (ruling D3).
pub(crate) fn restore_host_pfc(
    words: &[u64],
    region: usize,
    lp: usize,
    state: &mut HostState,
) -> Result<(), String> {
    let row_word = words[region + lp];
    let Some(pfc) = state.pfc.as_deref_mut() else {
        return if row_word == NONE {
            Ok(())
        } else {
            Err(format!(
                "PFC row published for host LP {lp} without pause state"
            ))
        };
    };
    let row = usize::try_from(row_word).map_err(|_| "PFC row offset overflows usize")?;
    let controllers = usize::try_from(words[row]).map_err(|_| "PFC controller count")?;
    let bitset_words = usize::try_from(words[row + 2]).map_err(|_| "PFC bitset width")?;
    let bitsets = usize::try_from(words[row + 3]).map_err(|_| "PFC bitset offset")?;
    let list = usize::try_from(words[row + 4]).map_err(|_| "PFC queue-pair list offset")?;
    let pairs = state
        .generators
        .iter()
        .filter(|generator| matches!(generator.kind, FlowGeneratorKind::Roce(_)))
        .map(|generator| generator.flow.0)
        .collect::<Vec<_>>();
    if words[row + 1] != 0
        || words[list] != pairs.len() as u64
        || words[list + 1..list + 1 + pairs.len()] != pairs[..]
    {
        return Err(format!("PFC row for host LP {lp} changed immutable state"));
    }
    pfc.paused_by_controller = std::array::from_fn(|priority| {
        (0..controllers)
            .filter(|bit| {
                words[bitsets + priority * bitset_words + bit / 64] & (1_u64 << (bit % 64)) != 0
            })
            .map(|bit| NodeId(words[row + PFC_ROW_HEADER_WORDS + bit]))
            .collect()
    });
    Ok(())
}

/// Restores one LP queue's PFC state from the region.
///
/// Pause sets and per-priority occupancy/assertion are device-written. Every other word is image
/// data no transition writes, and is checked rather than trusted.
pub(crate) fn restore_pfc_queue(
    words: &[u64],
    region: usize,
    lp: usize,
    queue: &mut SwitchQueueState,
) -> Result<(), String> {
    let row_word = words[region + lp];
    let Some(pfc) = queue.pfc.as_mut() else {
        return if row_word == NONE {
            Ok(())
        } else {
            Err(format!("PFC row published for LP {lp} without PFC state"))
        };
    };
    let row = usize::try_from(row_word).map_err(|_| "PFC row offset overflows usize")?;
    let controllers = usize::try_from(words[row]).map_err(|_| "PFC controller count")?;
    let bitset_words = usize::try_from(words[row + 2]).map_err(|_| "PFC bitset width")?;
    let bitsets = usize::try_from(words[row + 3]).map_err(|_| "PFC bitset offset")?;
    let ingresses = usize::try_from(words[row + 4]).map_err(|_| "PFC ingress offset")?;
    if words[row + 1] != pfc.ingresses.len() as u64 {
        return Err(format!("PFC row for LP {lp} changed its ingress count"));
    }
    let mut restored = PfcQueueState {
        paused_by_controller: std::array::from_fn(|priority| {
            (0..controllers)
                .filter(|bit| {
                    words[bitsets + priority * bitset_words + bit / 64] & (1_u64 << (bit % 64)) != 0
                })
                .map(|bit| NodeId(words[row + PFC_ROW_HEADER_WORDS + bit]))
                .collect()
        }),
        ingresses: pfc.ingresses.clone(),
    };
    for (index, ingress) in restored.ingresses.iter_mut().enumerate() {
        let base = ingresses + index * PFC_INGRESS_WORDS;
        if words[base + INGRESS_LINK] != ingress.controlled_link.0
            || (0..8).any(|priority| {
                words[base + INGRESS_CAPACITY + priority] != ingress.buffer_capacity_bytes[priority]
                    || words[base + INGRESS_XOFF + priority]
                        != ingress.xoff_threshold_bytes[priority]
                    || words[base + INGRESS_XON + priority] != ingress.xon_threshold_bytes[priority]
            })
        {
            return Err(format!(
                "PFC ingress {index} of LP {lp} changed immutable state"
            ));
        }
        for priority in 0..8 {
            ingress.occupancy_bytes[priority] = words[base + INGRESS_OCCUPANCY + priority];
            ingress.pause_asserted[priority] = match words[base + INGRESS_ASSERTED + priority] {
                0 => false,
                1 => true,
                _ => return Err(format!("PFC ingress {index} of LP {lp} has a non-boolean")),
            };
        }
    }
    *pfc = restored;
    Ok(())
}
