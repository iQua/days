//! Closed model choices supported by the v1 simulation image.

/// Scheduling discipline selected for a node-owned queue.
///
/// TailDrop is the only v1 admission policy, so it does not need a second open-ended selector.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SchedulerKind {
    Fifo = 0,
}
