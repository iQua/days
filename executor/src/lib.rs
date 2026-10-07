//! Exact execution contracts and the scalar oracle for the Days executor.
//!
//! Fixed-width event records and exact integer-time helpers form the common input contract for
//! later CPU and GPU executors. The scalar backend defines their executable reference behavior.

mod aqm_trace;
pub mod cpu;
#[cfg(feature = "cuda")]
pub mod cuda;
mod dcqcn;
mod device_capacity;
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal", target_vendor = "apple")
))]
mod device_compaction;
mod device_event_record;
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal", target_vendor = "apple")
))]
mod device_mechanism;
mod device_pfc;
mod device_scheduler;
pub mod device_sizing;
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal", target_vendor = "apple")
))]
mod device_stage;
pub mod event;
pub mod image;
mod mechanism_trace;
#[cfg(all(feature = "metal", target_vendor = "apple"))]
pub mod metal;
pub mod model;
#[cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]
mod planner_capacity;
mod roce;
pub mod safe_horizon;
pub mod scalar;
mod stage_index;
mod stage_sizing;
pub mod tcp;
mod tcp_ledger;
mod tcp_ledger_ring;
pub mod tcp_trace;
pub mod time;
pub mod validate;

pub use aqm_trace::{AqmTraceError, aqm_transitions_csv};
pub use cpu::{
    ChunkGranularity, CpuConfig, CpuFaultInjection, CpuFaultKind, CpuRoundMetrics, CpuRun,
    LpExecutionTiming, LpWorkEstimate, StaticPartitionPolicy, WorkClass, WorkPartition,
    WorkerRoundTiming, run_cpu, run_cpu_with_observations,
};
#[cfg(all(feature = "cuda", feature = "planner-test-hooks"))]
#[doc(hidden)]
pub use cuda::assert_cuda_planner_bit_equal_for_testing;
#[cfg(feature = "cuda")]
pub use cuda::{
    CudaArena, CudaConfig, CudaError, CudaExecutor, CudaInitializationTimings, CudaMemoryLayout,
    CudaRun, cuda_device_count, run_cuda, run_cuda_with_observations,
};
#[cfg(all(feature = "cuda", feature = "planner-test-hooks"))]
#[doc(hidden)]
pub use cuda::{
    mechanism_plane_words_cuda_for_testing, pfc_state_scans_cuda_plan_for_testing,
    size_cuda_plan_for_testing,
};
pub use dcqcn::{
    DCQCN_ALPHA_ONE, DcqcnAdvance, DcqcnArithmeticError, DcqcnController, DcqcnControllerConfig,
    DcqcnTransitionKind, DcqcnTransitionRecord,
};
pub use device_capacity::{
    CapacityRetryRecord, CapacityWarmStart, ChannelStreamCapacityLevel, DeviceCapacityCaps,
    DeviceCapacityFloors,
};
#[cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]
pub use device_mechanism::RoundKernel;
pub use device_sizing::{
    DeviceEventArenaSizing, DevicePlaneSizing, DeviceSizingError, DeviceSizingReport,
    MechanismPlaneWords, size_default_device_plan,
};
pub use event::{
    Event, EventFelClass, EventKey, EventKind, FlowId, LinkId, NodeId, PayloadId, event_fel_class,
    event_phase,
};
pub use image::{
    CollectiveAlgorithm, CollectiveChannelPolicy, CollectiveChunkPolicy, CollectivePhase,
    CollectiveStage, CollectiveStageIdentity, ComputeStage, ConstantGenerator, DcqcnCnpHeader,
    DcqcnGenerator, DcqcnReceiverState, EcnCodepoint, FlowDescriptor, FlowGeneratorKind,
    FlowGeneratorState, GeneratorFeedbackAction, GeneratorFeedbackState, GeneratorStatus,
    GeneratorTermination, HostPfcState, HostState, LinkDescriptor, NodeDescriptor,
    PacketDescriptor, PacketKind, PfcHeader, PfcIngressState, PfcQueueState, RateGenerator,
    RemoteChannel, RoceAckHeader, RoceDataHeader, RoceGenerator, RoceNackMark, RocePacer,
    RoceReceiverState, ScheduledEmission, SimulationImage, StageDependencies, StagePredecessors,
    StageRole, SwitchQueueState, SwitchState, TcpAckHeader, TcpDataHeader, TcpGenerator,
    TcpReceiveRange, TcpReceiverState, TcpTimerState, default_propagation_ns,
};
pub use mechanism_trace::{
    CollectiveActivationCause, CollectiveProgressRecord, CollectiveStageKind, CollectiveTraceError,
    DrrTransitionRecord, MechanismTraceError, MechanismTransitionRecord, PfcControlAction,
    PfcControlTransitionRecord, PfcOccupancyAction, PfcThresholdTransitionRecord, RateReplayConfig,
    RateReplayState, RateTransitionRecord, SchedulerPacket, WrrTransitionRecord,
    collective_transitions_csv, dcqcn_cnp_arrivals_csv, dcqcn_transitions_csv, drr_transitions_csv,
    pfc_transitions_csv, rate_transitions_csv, roce_receiver_transitions_csv,
    roce_sender_transitions_csv, wrr_transitions_csv,
};
#[cfg(all(
    feature = "metal",
    feature = "planner-test-hooks",
    target_vendor = "apple"
))]
#[doc(hidden)]
pub use metal::assert_metal_planner_bit_equal_for_testing;
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
#[doc(hidden)]
pub use metal::{
    ArenaOccupancyHighWater, DominantArenaHighWater, take_dominant_arena_high_water_for_testing,
};
#[cfg(all(feature = "metal", target_vendor = "apple"))]
pub use metal::{
    MetalArena, MetalConfig, MetalError, MetalExecutor, MetalInitializationTimings,
    MetalMemoryLayout, MetalRun, run_metal, run_metal_with_observations,
};
#[cfg(all(
    feature = "metal",
    feature = "planner-test-hooks",
    target_vendor = "apple"
))]
#[doc(hidden)]
pub use metal::{mechanism_plane_words_metal_for_testing, size_metal_plan_for_testing};
pub use model::{
    DropMarkPolicy, DrrSchedulerState, EcnThresholdPolicy, ExactRational, NodeKind, QueueDepthUnit,
    RedPolicyState, SchedulerKind, TransitionHandler, WfqSchedulerState, WrrSchedulerState,
    resolve_transition,
};
pub use roce::{
    RoceEmission, RocePacerState, RoceReceiverAction, RoceReceiverRecord, RoceReceiverView,
    RoceSenderKind, RoceSenderRecord, RoceSenderView, RoceTransitionRecord,
};
pub use safe_horizon::{
    LpRoundWork, RoundMetrics, ScalarRoundRun, run_scalar_rounds,
    run_scalar_rounds_with_observations,
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
    Backend, RateSourceLookahead, ValidationError, pfc_line_rate_bytes,
    pfc_required_headroom_bytes, rate_source_lookahead, validate,
};
#[cfg(feature = "planner-test-hooks")]
#[doc(hidden)]
pub use validate::{
    assert_validate_flow_index_equivalent_for_testing,
    assert_validate_generator_index_equivalent_for_testing,
    validate_flow_index_builds_stage_lookups_for_testing,
};
