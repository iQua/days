//! Exact execution contracts and the scalar oracle for the Days executor.
//!
//! Fixed-width event records and exact integer-time helpers form the common input contract for
//! later CPU and GPU executors. The scalar backend defines their executable reference behavior.

pub mod cpu;
pub mod event;
pub mod image;
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub mod metal_spike;
pub mod model;
pub mod safe_horizon;
pub mod scalar;
pub mod time;
pub mod validate;

pub use cpu::{
    ChunkGranularity, CpuConfig, CpuFaultInjection, CpuFaultKind, CpuRoundMetrics, CpuRun,
    LpExecutionTiming, LpWorkEstimate, StaticPartitionPolicy, WorkClass, WorkPartition,
    WorkerRoundTiming, run_cpu, run_cpu_with_observations,
};
pub use event::{Event, EventKey, EventKind, FlowId, LinkId, NodeId, PayloadId, event_phase};
pub use image::{
    ConstantGenerator, FlowDescriptor, FlowGeneratorKind, FlowGeneratorState,
    GeneratorFeedbackAction, GeneratorFeedbackState, GeneratorStatus, GeneratorTermination,
    HostState, LinkDescriptor, NodeDescriptor, PacketDescriptor, PacketKind, RemoteChannel,
    ScheduledEmission, SimulationImage, SwitchQueueState, SwitchState, default_propagation_ns,
};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub use metal_spike::{RealReplayTrace, ReplayStep, ReplayTraceCapture};
pub use model::{NodeKind, SchedulerKind, TransitionHandler, resolve_transition};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub use safe_horizon::run_scalar_rounds_with_replay_trace;
pub use safe_horizon::{
    LpRoundWork, RoundMetrics, ScalarRoundRun, run_scalar_rounds,
    run_scalar_rounds_with_observations,
};
pub use scalar::{
    ArrivalDisposition, ExecutionError, ObservationMode, PacketArrivalObservation, PacketDeparture,
    RunResult, RunSummary, run_scalar, run_scalar_with_observations,
};
pub use time::{TimeError, link_arrival_time_ns, serialization_time_ns};
pub use validate::{Backend, ValidationError, validate};
