//! Exact execution contracts and the scalar oracle for the Days executor.
//!
//! Fixed-width event records and exact integer-time helpers form the common input contract for
//! later CPU and GPU executors. The scalar backend defines their executable reference behavior.

mod aqm_trace;
pub mod cpu;
#[cfg(feature = "cuda")]
pub mod cuda;
mod dcqcn;
mod device_scheduler;
pub mod device_sizing;
pub mod event;
pub mod image;
mod mechanism_trace;
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub mod metal;
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub mod metal_spike;
pub mod model;
pub mod safe_horizon;
pub mod scalar;
pub mod tcp;
mod tcp_ledger;
pub mod tcp_trace;
pub mod time;
pub mod validate;

pub use aqm_trace::{AqmTraceError, aqm_transitions_csv};
pub use cpu::{
    ChunkGranularity, CpuConfig, CpuFaultInjection, CpuFaultKind, CpuRoundMetrics, CpuRun,
    LpExecutionTiming, LpWorkEstimate, StaticPartitionPolicy, WorkClass, WorkPartition,
    WorkerRoundTiming, run_cpu, run_cpu_with_observations,
};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub use cpu::{WindowedCpuRun, run_cpu_with_metrics_window};
#[cfg(feature = "cuda")]
pub use cuda::{
    CudaArena, CudaConfig, CudaError, CudaExecutor, CudaInitializationTimings, CudaMemoryLayout,
    CudaRun, run_cuda, run_cuda_with_observations,
};
pub use dcqcn::{
    DCQCN_FRACTION_SCALE, DCQCN_STAGE_STEPS, DcqcnArithmeticError, DcqcnController,
    DcqcnControllerConfig, DcqcnIncreaseStage, DcqcnTransitionKind, DcqcnTransitionRecord,
};
pub use device_sizing::{
    DeviceEventArenaSizing, DevicePlaneSizing, DeviceSizingError, DeviceSizingReport,
    size_default_device_plan,
};
pub use event::{
    Event, EventFelClass, EventKey, EventKind, FlowId, LinkId, NodeId, PayloadId, event_fel_class,
    event_phase,
};
pub use image::{
    CollectiveAlgorithm, CollectiveChannelPolicy, CollectiveChunkPolicy, CollectiveGenerator,
    CollectivePhase, ConstantGenerator, DcqcnCnpHeader, DcqcnGenerator, DcqcnReceiverState,
    EcnCodepoint, FlowDescriptor, FlowGeneratorKind, FlowGeneratorState, GeneratorFeedbackAction,
    GeneratorFeedbackState, GeneratorStatus, GeneratorTermination, HostState, LinkDescriptor,
    NodeDescriptor, PacketDescriptor, PacketKind, PfcHeader, PfcIngressState, PfcQueueState,
    RateGenerator, RemoteChannel, ScheduledEmission, SimulationImage, SwitchQueueState,
    SwitchState, TcpAckHeader, TcpDataHeader, TcpGenerator, TcpReceiveRange, TcpReceiverState,
    TcpTimerState, default_propagation_ns,
};
pub use mechanism_trace::{
    CollectiveActivationCause, CollectiveProgressRecord, DrrTransitionRecord, MechanismTraceError,
    MechanismTransitionRecord, PfcControlAction, PfcControlTransitionRecord, PfcOccupancyAction,
    PfcThresholdTransitionRecord, RateReplayConfig, RateReplayState, RateTransitionRecord,
    SchedulerPacket, WrrTransitionRecord, collective_transitions_csv, dcqcn_transitions_csv,
    drr_transitions_csv, pfc_transitions_csv, rate_transitions_csv, wrr_transitions_csv,
};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub use metal::{
    MetalArena, MetalConfig, MetalDrainDecomposition, MetalError, MetalExecutor,
    MetalFelControlRun, MetalFelProbeRun, MetalInitializationTimings, MetalMemoryLayout,
    MetalMergeFanIn, MetalMergeFanInRun, MetalPhaseProfile, MetalPhaseTimings, MetalRun, run_metal,
    run_metal_with_observations,
};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub use metal_spike::{RealReplayTrace, ReplayStep, ReplayTraceCapture};
pub use model::{
    DropMarkPolicy, DrrSchedulerState, EcnThresholdPolicy, ExactRational, NodeKind, QueueDepthUnit,
    RedPolicyState, SchedulerKind, TransitionHandler, WfqSchedulerState, WrrSchedulerState,
    resolve_transition,
};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub use safe_horizon::run_scalar_rounds_with_replay_trace;
pub use safe_horizon::{
    LpRoundWork, RoundMetrics, ScalarRoundRun, run_scalar_rounds,
    run_scalar_rounds_with_observations,
};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
pub use safe_horizon::{
    RoundMetricsWindow, RoundRunTotals, WindowedRunTotals, WindowedScalarRoundRun,
    run_scalar_rounds_with_windowed_replay_trace,
};
pub use scalar::{
    AqmTransitionAction, AqmTransitionRecord, ArrivalDisposition, DiagnosticPlanes, ExecutionError,
    ObservationMode, PacketArrivalObservation, PacketDeparture, RunResult, RunSummary,
    TcpTransitionInput, TcpTransitionRecord, run_scalar, run_scalar_with_observations,
};
pub use tcp::{CUBIC_WINDOW_SCALE, TcpCongestionControl, TcpPhase};
pub use tcp_trace::{TcpTraceError, tcp_transitions_csv};
pub use time::{TimeError, link_arrival_time_ns, serialization_time_ns};
pub use validate::{
    Backend, RateSourceLookahead, ValidationError, rate_source_lookahead, validate,
};
