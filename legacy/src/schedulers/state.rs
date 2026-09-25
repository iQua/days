//! Shared queue state accounting for scheduler capacity checks.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::schedulers::drop::CapacityUnit;

#[derive(Debug)]
pub struct QueueState {
    capacity: usize,
    capacity_unit: CapacityUnit,
    queued_bytes: AtomicUsize,
    queued_packets: AtomicUsize,
}

impl QueueState {
    pub fn new(capacity: usize, capacity_unit: CapacityUnit) -> Arc<QueueState> {
        Arc::new(QueueState {
            capacity,
            capacity_unit,
            queued_bytes: AtomicUsize::new(0),
            queued_packets: AtomicUsize::new(0),
        })
    }

    pub fn record_enqueue(&self, packet_size: usize) {
        self.queued_bytes.fetch_add(packet_size, Ordering::Relaxed);
        self.queued_packets.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dequeue(&self, packet_size: usize) {
        self.queued_bytes.fetch_sub(packet_size, Ordering::Relaxed);
        self.queued_packets.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn can_accept(&self, packet_size: usize) -> bool {
        if self.capacity == 0 {
            return true;
        }
        match self.capacity_unit {
            CapacityUnit::Bytes => {
                self.queued_bytes.load(Ordering::Relaxed) + packet_size <= self.capacity
            }
            CapacityUnit::Packets => self.queued_packets.load(Ordering::Relaxed) < self.capacity,
        }
    }

    pub fn queued_bytes(&self) -> usize {
        self.queued_bytes.load(Ordering::Relaxed)
    }

    pub fn queued_packets(&self) -> usize {
        self.queued_packets.load(Ordering::Relaxed)
    }
}
