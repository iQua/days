//! Shared host codec for the bounded device scheduler plane.

#![cfg_attr(
    not(any(
        feature = "cuda",
        all(feature = "metal-spike", target_vendor = "apple")
    )),
    allow(dead_code)
)]

use num_bigint::BigUint;

use crate::{ExactRational, NodeKind, SchedulerKind, SimulationImage, SwitchQueueState};

pub(crate) const RATIONAL_LIMBS: usize = 5;
pub(crate) const RATIONAL_WORDS: usize = RATIONAL_LIMBS * 2;

pub(crate) const SCHEDULER_NODE_WORDS: usize = 25;
pub(crate) const SCHEDULER_KIND: usize = 0;
pub(crate) const SCHEDULER_CLASS_COUNT: usize = 1;
pub(crate) const SCHEDULER_CLASS_OFFSET: usize = 2;
pub(crate) const SCHEDULER_QUEUE_TAG_OFFSET: usize = 3;
pub(crate) const SCHEDULER_LAST_UPDATED: usize = 4;
pub(crate) const SCHEDULER_VIRTUAL_TIME: usize = 5;
pub(crate) const SCHEDULER_IN_SERVICE_TAG: usize = 15;

pub(crate) const SCHEDULER_CLASS_WORDS: usize = 12;
pub(crate) const SCHEDULER_CLASS_VALUE: usize = 0;
pub(crate) const SCHEDULER_CLASS_ACTIVE: usize = 1;
pub(crate) const SCHEDULER_CLASS_FINISH: usize = 2;

pub(crate) const NONE: u64 = u64::MAX;

pub(crate) fn device_scheduler_word_count(
    image: &SimulationImage,
    queue_capacities: &[usize],
) -> Result<usize, String> {
    let mut words = image
        .nodes
        .len()
        .checked_mul(SCHEDULER_NODE_WORDS)
        .ok_or_else(|| "device scheduler metadata size overflows usize".to_owned())?;
    for node in image
        .nodes
        .iter()
        .filter(|node| node.kind == NodeKind::Switch)
    {
        let Some(queue) = image.switch_states[node.state_slot as usize].queues.first() else {
            continue;
        };
        let class_count = match &queue.scheduler {
            SchedulerKind::Fifo => 0,
            SchedulerKind::StaticPriority { priorities } => priorities.len(),
            SchedulerKind::WeightedFairQueue(state) => state.weights.len(),
        };
        words = words
            .checked_add(
                class_count
                    .checked_mul(SCHEDULER_CLASS_WORDS)
                    .ok_or_else(|| {
                        "device scheduler class-state size overflows usize".to_owned()
                    })?,
            )
            .ok_or_else(|| "device scheduler class arena size overflows usize".to_owned())?;
        if matches!(queue.scheduler, SchedulerKind::WeightedFairQueue(_)) {
            let slot = usize::try_from(node.id.0)
                .map_err(|_| "device scheduler node index overflows usize".to_owned())?;
            words = words
                .checked_add(
                    queue_capacities[slot]
                        .checked_mul(RATIONAL_WORDS)
                        .ok_or_else(|| {
                            "device scheduler queue-tag size overflows usize".to_owned()
                        })?,
                )
                .ok_or_else(|| "device scheduler queue-tag arena overflows usize".to_owned())?;
        }
    }
    Ok(words.max(1))
}

pub(crate) fn prepare_device_schedulers(
    image: &SimulationImage,
    queue_meta: &[u64],
) -> Result<Vec<u64>, String> {
    const META_WORDS: usize = 4;

    let fixed_words = image
        .nodes
        .len()
        .checked_mul(SCHEDULER_NODE_WORDS)
        .ok_or_else(|| "device scheduler metadata size overflows usize".to_owned())?;
    let mut words = vec![0_u64; fixed_words];
    for node in &image.nodes {
        let slot = usize::try_from(node.id.0)
            .map_err(|_| "device scheduler node index overflows usize".to_owned())?;
        let node_base = slot * SCHEDULER_NODE_WORDS;
        words[node_base + SCHEDULER_CLASS_OFFSET] = NONE;
        words[node_base + SCHEDULER_QUEUE_TAG_OFFSET] = NONE;
        write_zero(&mut words, node_base + SCHEDULER_VIRTUAL_TIME);
        write_zero(&mut words, node_base + SCHEDULER_IN_SERVICE_TAG);
        if node.kind != NodeKind::Switch {
            continue;
        }

        let Some(queue) = image.switch_states[node.state_slot as usize].queues.first() else {
            continue;
        };
        words[node_base + SCHEDULER_KIND] = u64::from(queue.scheduler.code());
        match &queue.scheduler {
            SchedulerKind::Fifo => {}
            SchedulerKind::StaticPriority { priorities } => {
                words[node_base + SCHEDULER_CLASS_COUNT] = priorities.len() as u64;
                words[node_base + SCHEDULER_CLASS_OFFSET] = words.len() as u64;
                for priority in priorities {
                    let class_base = append_class(&mut words)?;
                    words[class_base + SCHEDULER_CLASS_VALUE] = *priority;
                }
            }
            SchedulerKind::WeightedFairQueue(state) => {
                words[node_base + SCHEDULER_CLASS_COUNT] = state.weights.len() as u64;
                words[node_base + SCHEDULER_CLASS_OFFSET] = words.len() as u64;
                words[node_base + SCHEDULER_LAST_UPDATED] = state.last_updated_ns;
                write_rational(
                    &mut words,
                    node_base + SCHEDULER_VIRTUAL_TIME,
                    &state.virtual_time,
                )?;
                for (class, weight) in state.weights.iter().copied().enumerate() {
                    let class_base = append_class(&mut words)?;
                    words[class_base + SCHEDULER_CLASS_VALUE] = weight;
                    words[class_base + SCHEDULER_CLASS_ACTIVE] = state.active_packets[class];
                    write_rational(
                        &mut words,
                        class_base + SCHEDULER_CLASS_FINISH,
                        &state.finish_times[class],
                    )?;
                }

                let queue_base = slot * META_WORDS;
                let capacity = usize::try_from(queue_meta[queue_base + 1])
                    .map_err(|_| "device scheduler queue capacity overflows usize".to_owned())?;
                let head = usize::try_from(queue_meta[queue_base + 2])
                    .map_err(|_| "device scheduler queue head overflows usize".to_owned())?;
                let tag_offset = words.len();
                words[node_base + SCHEDULER_QUEUE_TAG_OFFSET] = tag_offset as u64;
                let tag_words = capacity
                    .checked_mul(RATIONAL_WORDS)
                    .ok_or_else(|| "device scheduler queue-tag size overflows usize".to_owned())?;
                words.resize(
                    words.len().checked_add(tag_words).ok_or_else(|| {
                        "device scheduler queue-tag arena size overflows usize".to_owned()
                    })?,
                    0,
                );
                for physical in 0..capacity {
                    write_zero(&mut words, tag_offset + physical * RATIONAL_WORDS);
                }
                for (logical, payload) in queue.queue.iter().enumerate() {
                    let physical = (head + logical) % capacity.max(1);
                    let finish = state.packet_finish_times.get(payload).ok_or_else(|| {
                        format!("WFQ packet {payload:?} has no checkpoint finish tag")
                    })?;
                    write_rational(&mut words, tag_offset + physical * RATIONAL_WORDS, finish)?;
                }
                if let Some(payload) = queue.in_service {
                    let finish = state.packet_finish_times.get(&payload).ok_or_else(|| {
                        format!("WFQ in-service packet {payload:?} has no checkpoint finish tag")
                    })?;
                    write_rational(&mut words, node_base + SCHEDULER_IN_SERVICE_TAG, finish)?;
                }
            }
        }
    }
    Ok(words)
}

fn append_class(words: &mut Vec<u64>) -> Result<usize, String> {
    let base = words.len();
    words.resize(
        base.checked_add(SCHEDULER_CLASS_WORDS)
            .ok_or_else(|| "device scheduler class-state size overflows usize".to_owned())?,
        0,
    );
    write_zero(words, base + SCHEDULER_CLASS_FINISH);
    Ok(base)
}

fn write_zero(words: &mut [u64], offset: usize) {
    words[offset..offset + RATIONAL_WORDS].fill(0);
    words[offset + RATIONAL_LIMBS] = 1;
}

pub(crate) fn write_rational(
    words: &mut [u64],
    offset: usize,
    value: &ExactRational,
) -> Result<(), String> {
    words[offset..offset + RATIONAL_WORDS].fill(0);
    let numerator = value.numer().to_u64_digits();
    let denominator = value.denom().to_u64_digits();
    if numerator.len() > RATIONAL_LIMBS || denominator.len() > RATIONAL_LIMBS {
        return Err("device WFQ rational exceeds the validated 320-bit limit".to_owned());
    }
    words[offset..offset + numerator.len()].copy_from_slice(&numerator);
    words[offset + RATIONAL_LIMBS..offset + RATIONAL_LIMBS + denominator.len()]
        .copy_from_slice(&denominator);
    Ok(())
}

pub(crate) fn read_rational(words: &[u64], offset: usize) -> Result<ExactRational, String> {
    let numerator = BigUint::new(
        words[offset..offset + RATIONAL_LIMBS]
            .iter()
            .flat_map(|word| [*word as u32, (*word >> 32) as u32])
            .collect(),
    );
    let denominator = BigUint::new(
        words[offset + RATIONAL_LIMBS..offset + RATIONAL_WORDS]
            .iter()
            .flat_map(|word| [*word as u32, (*word >> 32) as u32])
            .collect(),
    );
    if denominator == BigUint::from(0_u8) {
        return Err("device WFQ rational has a zero denominator".to_owned());
    }
    Ok(ExactRational::new_raw(numerator, denominator))
}

pub(crate) fn restore_device_scheduler(
    node: usize,
    queue_meta: &[u64],
    words: &[u64],
    queue: &mut SwitchQueueState,
) -> Result<(), String> {
    const META_WORDS: usize = 4;

    let node_base = node
        .checked_mul(SCHEDULER_NODE_WORDS)
        .ok_or_else(|| "device scheduler node offset overflows usize".to_owned())?;
    let encoded_kind = *words
        .get(node_base + SCHEDULER_KIND)
        .ok_or_else(|| "device scheduler node metadata is truncated".to_owned())?;
    if encoded_kind != u64::from(queue.scheduler.code()) {
        return Err(format!(
            "device scheduler kind {encoded_kind} disagrees with checkpoint kind {}",
            queue.scheduler.code()
        ));
    }
    let SchedulerKind::WeightedFairQueue(state) = &mut queue.scheduler else {
        return Ok(());
    };

    let class_count = usize::try_from(words[node_base + SCHEDULER_CLASS_COUNT])
        .map_err(|_| "device WFQ class count overflows usize".to_owned())?;
    if class_count != state.weights.len() {
        return Err("device WFQ class count changed during execution".to_owned());
    }
    let class_offset = usize::try_from(words[node_base + SCHEDULER_CLASS_OFFSET])
        .map_err(|_| "device WFQ class offset overflows usize".to_owned())?;
    state.virtual_time = read_rational(words, node_base + SCHEDULER_VIRTUAL_TIME)?;
    state.last_updated_ns = words[node_base + SCHEDULER_LAST_UPDATED];
    for class in 0..class_count {
        let class_base = class_offset
            .checked_add(class * SCHEDULER_CLASS_WORDS)
            .ok_or_else(|| "device WFQ class-state offset overflows usize".to_owned())?;
        if words[class_base + SCHEDULER_CLASS_VALUE] != state.weights[class] {
            return Err(format!("device WFQ weight for class {class} changed"));
        }
        state.active_packets[class] = words[class_base + SCHEDULER_CLASS_ACTIVE];
        state.finish_times[class] = read_rational(words, class_base + SCHEDULER_CLASS_FINISH)?;
    }

    state.packet_finish_times.clear();
    let queue_base = node * META_WORDS;
    let capacity = usize::try_from(queue_meta[queue_base + 1])
        .map_err(|_| "device WFQ queue capacity overflows usize".to_owned())?;
    let head = usize::try_from(queue_meta[queue_base + 2])
        .map_err(|_| "device WFQ queue head overflows usize".to_owned())?;
    let tag_offset = usize::try_from(words[node_base + SCHEDULER_QUEUE_TAG_OFFSET])
        .map_err(|_| "device WFQ tag offset overflows usize".to_owned())?;
    for (logical, payload) in queue.queue.iter().copied().enumerate() {
        let physical = (head + logical) % capacity.max(1);
        let finish = read_rational(
            words,
            tag_offset
                .checked_add(physical * RATIONAL_WORDS)
                .ok_or_else(|| "device WFQ waiting-tag offset overflows usize".to_owned())?,
        )?;
        state.packet_finish_times.insert(payload, finish);
    }
    if let Some(payload) = queue.in_service {
        let finish = read_rational(words, node_base + SCHEDULER_IN_SERVICE_TAG)?;
        state.packet_finish_times.insert(payload, finish);
    }
    Ok(())
}
