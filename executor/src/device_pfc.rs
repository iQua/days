//! Device PFC plane codec shared by Metal and CUDA (P14 Lane B T4).
//!
//! PFC state rides at the end of the scheduler plane, in a region that exists only when the image
//! carries PFC state. Without it the region is absent, its params word holds [`NONE`], and no PFC
//! word is allocated.
//!
//! Region layout, as absolute word offsets from the region start `R`:
//!
//! - `R + node`: the node's PFC queue row offset (absolute in the plane), or [`NONE`].
//! - `R + node_count + flow`: the flow's 802.1Q priority.
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
//! Scalar keeps `paused_by_controller: [BTreeSet<NodeId>; 8]`. A queue's possible controllers are
//! static image data: the downstream LPs with an ingress monitor on the queue's egress link, which
//! validation requires to be the only pause holders. The bitset is therefore an exact, fixed-width
//! encoding of every reachable set.

#![cfg_attr(
    not(any(feature = "cuda", all(feature = "metal", target_vendor = "apple"))),
    allow(dead_code)
)]

use std::collections::BTreeSet;

use crate::{NodeId, NodeKind, PfcQueueState, SimulationImage, SwitchQueueState};

pub(crate) const NONE: u64 = u64::MAX;
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

/// The controllers that may hold pause on the queue whose egress is `egress`: every switch LP whose
/// PFC ingress monitors control that link, ascending by node id.
fn queue_controllers(image: &SimulationImage, queue: &SwitchQueueState) -> Vec<NodeId> {
    let mut controllers = BTreeSet::new();
    if let Some(egress) = queue.egress_link {
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
    if let Some(pfc) = &queue.pfc {
        controllers.extend(pfc.paused_by_controller.iter().flatten().copied());
    }
    controllers.into_iter().collect()
}

/// The first queue of every switch LP, which is the only queue a lowered LP owns and the only one
/// the device kernels model.
fn lp_queues(image: &SimulationImage) -> impl Iterator<Item = (usize, &SwitchQueueState)> {
    image.nodes.iter().filter_map(|node| {
        (node.kind == NodeKind::Switch)
            .then(|| image.switch_states[node.state_slot as usize].queues.first())
            .flatten()
            .map(|queue| (node.id.0 as usize, queue))
    })
}

/// Whether the image carries any PFC state, which is exactly when the region exists.
pub(crate) fn image_has_pfc(image: &SimulationImage) -> bool {
    lp_queues(image).any(|(_, queue)| queue.pfc.is_some())
}

/// `(producer, target)` LP pairs of every PFC reverse control lane.
///
/// Control frames travel on these lanes, never on a flow route, so the exchange's inbound-producer
/// sets derived from routes must include them. Empty without PFC state.
pub(crate) fn pfc_control_lane_producers(image: &SimulationImage) -> Vec<(u64, usize)> {
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

/// Words the PFC region occupies: zero exactly when the image carries no PFC state.
pub(crate) fn pfc_region_words(image: &SimulationImage) -> usize {
    if !image_has_pfc(image) {
        return 0;
    }
    let queues = lp_queues(image)
        .filter(|(_, queue)| queue.pfc.is_some())
        .map(|(_, queue)| (queue, queue_controllers(image, queue).len()))
        .collect::<Vec<_>>();
    let bitset_words = queues
        .iter()
        .map(|(_, controllers)| controllers.div_ceil(64))
        .max()
        .unwrap_or(0)
        .max(1);
    queues.iter().fold(
        image.nodes.len().saturating_add(image.flows.len()),
        |total, (queue, controllers)| {
            let ingresses = queue.pfc.as_ref().map_or(0, |pfc| pfc.ingresses.len());
            total
                .saturating_add(PFC_ROW_HEADER_WORDS)
                .saturating_add(*controllers)
                .saturating_add(8 * bitset_words)
                .saturating_add(ingresses.saturating_mul(PFC_INGRESS_WORDS))
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
    let queues = lp_queues(image)
        .filter(|(_, queue)| queue.pfc.is_some())
        .map(|(lp, queue)| (lp, queue, queue_controllers(image, queue)))
        .collect::<Vec<_>>();
    let bitset_words = queues
        .iter()
        .map(|(_, _, controllers)| controllers.len().div_ceil(64))
        .max()
        .unwrap_or(0)
        .max(1);

    let region = words.len();
    words.resize(region + node_count + flow_count, 0);
    words[region..region + node_count].fill(NONE);
    for (index, flow) in image.flows.iter().enumerate() {
        words[region + node_count + index] = u64::from(flow.priority);
    }
    for (lp, queue, controllers) in queues {
        let pfc = queue.pfc.as_ref().expect("filtered to PFC queues");
        let row = words.len();
        let bitsets = row + PFC_ROW_HEADER_WORDS + controllers.len();
        let ingresses = bitsets + 8 * bitset_words;
        let end = ingresses
            .checked_add(
                pfc.ingresses
                    .len()
                    .checked_mul(PFC_INGRESS_WORDS)
                    .ok_or("PFC ingress region overflows usize")?,
            )
            .ok_or("PFC region overflows usize")?;
        words.resize(end, 0);
        words[region + lp] = row as u64;
        words[row] = controllers.len() as u64;
        words[row + 1] = pfc.ingresses.len() as u64;
        words[row + 2] = bitset_words as u64;
        words[row + 3] = bitsets as u64;
        words[row + 4] = ingresses as u64;
        for (index, controller) in controllers.iter().enumerate() {
            words[row + PFC_ROW_HEADER_WORDS + index] = controller.0;
        }
        for (priority, holders) in pfc.paused_by_controller.iter().enumerate() {
            for holder in holders {
                let bit = controllers
                    .binary_search(holder)
                    .expect("queue_controllers includes every pause holder");
                words[bitsets + priority * bitset_words + bit / 64] |= 1_u64 << (bit % 64);
            }
        }
        for (index, ingress) in pfc.ingresses.iter().enumerate() {
            let base = ingresses + index * PFC_INGRESS_WORDS;
            let channel = image
                .channels
                .get(ingress.control_channel_index as usize)
                .ok_or("PFC ingress references an unknown control channel")?;
            words[base + INGRESS_LINK] = ingress.controlled_link.0;
            words[base + INGRESS_TARGET] = channel.target.0;
            words[base + INGRESS_DELAY] = channel.min_delay_ns;
            for priority in 0..8 {
                words[base + INGRESS_CAPACITY + priority] = ingress.buffer_capacity_bytes[priority];
                words[base + INGRESS_XOFF + priority] = ingress.xoff_threshold_bytes[priority];
                words[base + INGRESS_XON + priority] = ingress.xon_threshold_bytes[priority];
                words[base + INGRESS_OCCUPANCY + priority] = ingress.occupancy_bytes[priority];
                words[base + INGRESS_ASSERTED + priority] =
                    u64::from(ingress.pause_asserted[priority]);
            }
        }
    }
    debug_assert_eq!(words.len() - region, pfc_region_words(image));
    Ok(Some(region))
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
