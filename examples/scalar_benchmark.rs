use std::{env, error::Error, time::Instant};

use days::scenario::compile_config;
use days_executor::{
    ArrivalDisposition, Backend, RunResult, SimulationImage, run_scalar, validate,
};

fn processed_event_count(image: &SimulationImage, result: &RunResult) -> u64 {
    let initial_origin_seq = image
        .host_states
        .iter()
        .map(|state| state.next_origin_seq)
        .chain(
            image
                .switch_states
                .iter()
                .map(|state| state.next_origin_seq),
        )
        .sum();
    let final_origin_seq = result
        .host_states
        .iter()
        .map(|state| state.next_origin_seq)
        .chain(
            result
                .switch_states
                .iter()
                .map(|state| state.next_origin_seq),
        )
        .sum();
    processed_event_count_from_parts(
        image.initial_events.len() as u64,
        initial_origin_seq,
        final_origin_seq,
        result.pending_events.len() as u64,
    )
}

fn processed_event_count_from_parts(
    initial_events: u64,
    initial_origin_seq: u64,
    final_origin_seq: u64,
    pending_events: u64,
) -> u64 {
    let generated_events = final_origin_seq - initial_origin_seq;
    initial_events + generated_events - pending_events
}

fn packet_size(image: &SimulationImage, payload: days_executor::PayloadId) -> u64 {
    image.packets[payload.0 as usize].size_bytes
}

fn main() -> Result<(), Box<dyn Error>> {
    let path = env::args().nth(1).ok_or("usage: scalar_benchmark CONFIG")?;
    let image = compile_config(&path)?;
    validate(&image, Backend::Scalar)?;

    let timer = Instant::now();
    let result = run_scalar(&image, None)?;
    let elapsed = timer.elapsed();

    let sourced_packets = result
        .host_states
        .iter()
        .map(|state| state.sourced_packets)
        .sum::<u64>();
    let sourced_bytes = image
        .initial_events
        .iter()
        .filter(|event| event.key.time_ns <= image.stop_time_ns)
        .map(|event| packet_size(&image, event.payload))
        .sum::<u64>();
    let received_packets = result
        .arrivals
        .iter()
        .filter(|arrival| arrival.disposition == ArrivalDisposition::Delivered)
        .count() as u64;
    let received_bytes = result
        .arrivals
        .iter()
        .filter(|arrival| arrival.disposition == ArrivalDisposition::Delivered)
        .map(|arrival| packet_size(&image, arrival.payload))
        .sum::<u64>();
    let dropped_packets = result
        .arrivals
        .iter()
        .filter(|arrival| arrival.disposition == ArrivalDisposition::Dropped)
        .count() as u64;
    let dropped_bytes = result
        .arrivals
        .iter()
        .filter(|arrival| arrival.disposition == ArrivalDisposition::Dropped)
        .map(|arrival| packet_size(&image, arrival.payload))
        .sum::<u64>();
    println!(
        "config={path} stop_time_ns={} next_pending_ns={:?} pending_events={} \
         certified_lookahead_ns={:?} events={} \
         sourced_packets={sourced_packets} sourced_bytes={sourced_bytes} \
         received_packets={received_packets} received_bytes={received_bytes} \
         dropped_packets={dropped_packets} dropped_bytes={dropped_bytes} wall_ns={}",
        image.stop_time_ns,
        result.pending_events.first().map(|event| event.key.time_ns),
        result.pending_events.len(),
        image
            .channels
            .iter()
            .map(|channel| channel.min_delay_ns)
            .min(),
        processed_event_count(&image, &result),
        elapsed.as_nanos()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subtracts_pending_events_from_all_created_events() {
        assert_eq!(processed_event_count_from_parts(12, 12, 147, 0), 147);
    }

    #[test]
    fn counts_processed_boundary_events_while_children_remain_pending() {
        assert_eq!(processed_event_count_from_parts(8, 8, 32, 16), 16);
    }
}
