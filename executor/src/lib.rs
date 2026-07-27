//! Platform-neutral data contracts for the Days executor.
//!
//! This crate intentionally contains no execution backend. Its fixed-width records and exact
//! integer-time helpers form the common input contract for later CPU and GPU executors.

pub mod event;
pub mod image;
pub mod model;
pub mod time;

pub use event::{Event, EventKey, EventKind, LinkId, NodeId, PayloadId};
pub use image::{
    HostState, LinkDescriptor, NodeDescriptor, RemoteChannel, SimulationImage, SwitchState,
    default_propagation_ns,
};
pub use model::{NodeKind, SchedulerKind, TransitionHandler, resolve_transition};
pub use time::{TimeError, link_arrival_time_ns, serialization_time_ns};
