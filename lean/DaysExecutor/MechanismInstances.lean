import DaysExecutor.SchedulerInstances

namespace DaysExecutor

/-!
Supported-abstraction obligations for the P10c mechanism vocabulary.

These statements intentionally cover only facts represented by the frozen executor model: canonical
event placement, role support, packet mark/control typing, and conservative conflict coverage. The
concrete DRR/WRR counters, RED average/counter, rate credit, and PFC pause-eligibility state are not
fields of `OrderedQueueStateFamily`; no concrete transition-refinement claim is made here.
-/

/-- Marking changes only the ECN bit in the immutable packet observation. -/
def PacketDescriptor.markEcn (packet : PacketDescriptor) : PacketDescriptor :=
  { packet with ecnMarked := true }

@[simp] theorem markEcn_is_marked (packet : PacketDescriptor) :
    packet.markEcn.ecnMarked = true := rfl

/-- ECN marking has no packet-size, identity, flow, or payload-kind timing effect. -/
theorem markEcn_preserves_timing_fields (packet : PacketDescriptor) :
    packet.markEcn.id = packet.id ∧
      packet.markEcn.flow = packet.flow ∧
      packet.markEcn.sizeBytes = packet.sizeBytes ∧
      packet.markEcn.kind = packet.kind := by
  exact ⟨rfl, rfl, rfl, rfl⟩

/-- Typed PFC reverse-channel descriptor; PFC is a packet payload, not an event kind. -/
def pfcPacketDescriptor
    (payload : PayloadId)
    (flow : FlowId)
    (sizeBytes : Nat)
    (header : PfcHeader) : PacketDescriptor :=
  { id := payload
    flow
    sizeBytes
    ecnMarked := false
    kind := .pfc header }

@[simp] theorem pfcPacketDescriptor_kind payload flow sizeBytes header :
    (pfcPacketDescriptor payload flow sizeBytes header).kind = .pfc header := rfl

/-- Every declared packet channel remains an ordinary `RemoteArrival`, including PFC control. -/
theorem declared_channel_uses_remoteArrival
    {image : SimulationImage State}
    (hchannels : ChannelsReferenceDirectedLinks image)
    {channel : RemoteChannel}
    (hchannel : channel ∈ image.channels) :
    channel.eventKind = .remoteArrival := by
  rcases hchannels channel hchannel with ⟨_, _, _, _, hkind⟩
  exact hkind

/-- The rate-source timer is the phase-1, host-only fallback-heap client required by M3. -/
theorem pacingTimer_structural_obligations :
    eventPhase .pacingTimer = 1 ∧
      eventFelClass .pacingTimer = .fallbackHeap ∧
      roleSupports .host .pacingTimer ∧
      ¬ roleSupports .switch .pacingTimer := by
  exact ⟨rfl, rfl, trivial, id⟩

/-- DRR arrival, service start, and completion stay in one conservative queue conflict class. -/
theorem drr_queueMutationConflictCoverage :
    fifoQueueConflictClass .remoteArrival = fifoQueueConflictClass .txReady ∧
      fifoQueueConflictClass .txReady = fifoQueueConflictClass .txComplete := by
  exact ⟨rfl, rfl⟩

/-- WRR has the same conservative queue-conflict coverage as every M2 discipline. -/
theorem wrr_queueMutationConflictCoverage :
    fifoQueueConflictClass .remoteArrival = fifoQueueConflictClass .txReady ∧
      fifoQueueConflictClass .txReady = fifoQueueConflictClass .txComplete := by
  exact ⟨rfl, rfl⟩

/-- RED/ECN enqueue mutation conflicts with service selection at the same queue LP. -/
theorem dropMark_enqueue_conflicts_with_serviceStart :
    fifoQueueConflictClass .remoteArrival = fifoQueueConflictClass .txReady := rfl

/-- A PFC pause/resume arrival conflicts with service eligibility at the gated egress LP. -/
theorem pfc_pauseArrival_conflicts_with_serviceStart :
    fifoQueueConflictClass .remoteArrival = fifoQueueConflictClass .txReady := rfl

end DaysExecutor
