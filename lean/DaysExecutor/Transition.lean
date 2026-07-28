import DaysExecutor.Image

namespace DaysExecutor

/--
Constant-space result counters mirroring every field of `executor/src/scalar.rs:49-62`
(`RunSummary`).
-/
structure RunSummary where
  sourcedPackets : Nat
  sourcedBytes : Nat
  departedPackets : Nat
  departedBytes : Nat
  admittedPackets : Nat
  admittedBytes : Nat
  receivedPackets : Nat
  receivedBytes : Nat
  droppedPackets : Nat
  droppedBytes : Nat
  feedbackPackets : Nat
  feedbackBytes : Nat
  deriving DecidableEq, Repr

/-- Zero accumulated output before the first transition. -/
def RunSummary.zero : RunSummary where
  sourcedPackets := 0
  sourcedBytes := 0
  departedPackets := 0
  departedBytes := 0
  admittedPackets := 0
  admittedBytes := 0
  receivedPackets := 0
  receivedBytes := 0
  droppedPackets := 0
  droppedBytes := 0
  feedbackPackets := 0
  feedbackBytes := 0

/-- Checked-success-path counter accumulation corresponding to CPU result assembly. -/
def RunSummary.add (left right : RunSummary) : RunSummary where
  sourcedPackets := left.sourcedPackets + right.sourcedPackets
  sourcedBytes := left.sourcedBytes + right.sourcedBytes
  departedPackets := left.departedPackets + right.departedPackets
  departedBytes := left.departedBytes + right.departedBytes
  admittedPackets := left.admittedPackets + right.admittedPackets
  admittedBytes := left.admittedBytes + right.admittedBytes
  receivedPackets := left.receivedPackets + right.receivedPackets
  receivedBytes := left.receivedBytes + right.receivedBytes
  droppedPackets := left.droppedPackets + right.droppedPackets
  droppedBytes := left.droppedBytes + right.droppedBytes
  feedbackPackets := left.feedbackPackets + right.feedbackPackets
  feedbackBytes := left.feedbackBytes + right.feedbackBytes

/-- Remote-arrival outcome mirroring `executor/src/scalar.rs:14-21`. -/
inductive ArrivalDisposition where
  | admitted
  | dropped
  | delivered
  | feedback
  deriving DecidableEq, Repr, Ord

/-- Completed transmission record mirroring `executor/src/scalar.rs:23-29`. -/
structure PacketDeparture where
  payload : PayloadId
  timeNs : Nat
  deriving DecidableEq, Repr

/-- Processed remote-arrival record mirroring `executor/src/scalar.rs:31-37`. -/
structure PacketArrivalObservation where
  payload : PayloadId
  timeNs : Nat
  disposition : ArrivalDisposition
  deriving DecidableEq, Repr

/-- Internal canonical key retained while normalizing a departure result. -/
structure RecordedDeparture where
  eventKey : EventKey
  departure : PacketDeparture
  deriving DecidableEq, Repr

/-- Internal canonical key retained while normalizing an arrival result. -/
structure RecordedArrival where
  eventKey : EventKey
  arrival : PacketArrivalObservation
  deriving DecidableEq, Repr

/--
Proof metadata for one state-dependent, committed service selection corresponding to a `TxReady`
handler at `executor/src/scalar.rs:863-921,1091-1166`. Completeness is stated separately, so an
implementation cannot omit a selection from this trace.
-/
structure ServiceDecision where
  node : NodeId
  decisionKey : EventKey
  packet : PayloadId
  committedNonPreemptively : Bool
  deriving DecidableEq, Repr

/--
Abstract result of one role-correct run-to-completion handler, including child, descriptor,
summary, and observation effects accumulated by `TransitionState.dispatch` and finish assembly at
`executor/src/scalar.rs:575-663`.
-/
structure TransitionResult (State : StateFamily) (kind : NodeKind) where
  nextState : RoleState State kind
  children : List Event
  packetInstalls : List PacketDescriptor
  packetRemovals : List PayloadId
  summaryDelta : RunSummary
  observedPackets : List PacketDescriptor
  departures : List RecordedDeparture
  arrivals : List RecordedArrival
  decisions : List ServiceDecision

/--
Per-LP abstract transition relation. The `node` argument deliberately permits different LPs and
roles to use different code while retaining Rust's `(NodeKind, EventKind)` dispatch shape at
`executor/src/scalar.rs:638-663`.
-/
abbrev TransitionRelation (State : StateFamily) :=
  (node : NodeDescriptor) →
  (event : Event) →
  RoleState State node.kind →
  TransitionResult State node.kind →
  Prop

/--
Closed role/event support table mirroring `executor/src/model.rs:41-60`
(`resolve_transition`).
-/
def roleSupports : NodeKind → EventKind → Prop
  | .host, _ => True
  | .switch, .packetArrival => False
  | .switch, .txReady | .switch, .txComplete | .switch, .remoteArrival => True

/--
Determinism of each LP's abstract handler, matching the single-result dispatch in
`executor/src/scalar.rs:638-663`.
-/
def TransitionDeterministic
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state left right,
    transition node event state left →
    transition node event state right →
    left = right

/--
Role and target correctness of dispatched handlers, matching the checks before
`executor/src/scalar.rs:652-662`.
-/
def TransitionRoleCorrect
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    event.target = node.id ∧ roleSupports node.kind event.kind

/--
Every generated child targets a declared LP whose closed role supports the child's event kind,
mirroring the target-role dispatch guard at `executor/src/scalar.rs:638-663`.
-/
def GeneratedEventsRoleCorrect
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    ∀ child ∈ result.children,
      ∃ target ∈ image.nodes,
        child.target = target.id ∧ roleSupports target.kind child.kind

/--
Every generated child strictly advances its complete parent key, mirroring
`executor/src/scalar.rs:1300-1313`.
-/
def ChildrenAdvanceParent
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    ∀ child ∈ result.children, event.key < child.key

/--
Every child's key origin is the emitting LP, mirroring per-owner origin allocation at
`executor/src/scalar.rs:1236-1297`.
-/
def ChildrenUseOwnerOrigin
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    ∀ child ∈ result.children, child.key.originNode = node.id

/--
Every generated child uses the canonical phase for its closed event kind, mirroring construction at
`executor/src/scalar.rs:1251-1263,1283-1295`.
-/
def ChildrenUseCanonicalPhase
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    ∀ child ∈ result.children, child.key.phase = eventPhase child.kind

/--
Children of one deterministic transition have unique keys, matching Rust's duplicate-key
diagnostic at `executor/src/safe_horizon.rs:422-431`.
-/
def TransitionChildrenHaveUniqueKeys
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    UniqueEventKeys result.children

/--
Every possible cross-LP emission uses a declared channel, mirroring the validator coverage check at
`executor/src/validate.rs:1072-1079`.
-/
def RemoteEmissionCoverage
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    ∀ child ∈ result.children,
      child.target ≠ node.id →
      ∃ channel ∈ image.channels,
        channel.source = node.id ∧
        channel.target = child.target ∧
        channel.eventKind = child.kind

/--
Declared channel lower bounds are no larger than both the supported directed-link delay and the
actual remote child advance, mirroring `executor/src/validate.rs:1053-1068` and link timing at
`executor/src/scalar.rs:893-920,1139-1165`.
-/
def CertifiedBoundSoundness
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    ∀ child ∈ result.children,
      child.target ≠ node.id →
      ∀ channel ∈ image.channels,
        channel.source = node.id →
        channel.target = child.target →
        channel.eventKind = child.kind →
        ∃ link ∈ image.links,
          channel.link = link.id ∧
          link.source = node.id ∧
          channel.minDelayNs ≤ linkDelayNs link (image.payloadBytes child.payload) ∧
          event.key.timeNs + channel.minDelayNs ≤ child.key.timeNs

/--
Every descriptor installed or retained for full observations is the immutable oracle value for its
payload. This matches Rust's conflicting-descriptor rejection at
`executor/src/scalar.rs:545-565`.
-/
def TransitionDescriptorEffectsCoherent
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    (∀ descriptor ∈ result.packetInstalls,
      descriptor = image.packetDescriptor descriptor.id) ∧
    (∀ descriptor ∈ result.observedPackets,
      descriptor = image.packetDescriptor descriptor.id)

/--
All global transition-safety assumptions used by the safe-horizon statements, collected without
introducing a runtime certificate; reachable enabledness is stated separately after execution
reachability is defined. These mirror validation plus dispatch at
`executor/src/validate.rs:1010-1080` and `executor/src/scalar.rs:638-663,1300-1313`.
-/
def TransitionAxioms
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  TransitionDeterministic transition ∧
    TransitionRoleCorrect transition ∧
    GeneratedEventsRoleCorrect image transition ∧
    ChildrenAdvanceParent transition ∧
    ChildrenUseOwnerOrigin transition ∧
    ChildrenUseCanonicalPhase transition ∧
    TransitionChildrenHaveUniqueKeys transition ∧
    RemoteEmissionCoverage image transition ∧
    CertifiedBoundSoundness image transition ∧
    TransitionDescriptorEffectsCoherent image transition

/--
State-dependent choices occur only at the actual `TxReady`, choose at most one packet, and commit
that transmission non-preemptively, mirroring
`executor/src/scalar.rs:863-921,1091-1166`.
-/
def ActualServiceStartDiscipline
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    result.decisions.length ≤ 1 ∧
      ∀ decision ∈ result.decisions,
        event.kind = .txReady ∧
        decision.node = node.id ∧
        decision.decisionKey = event.key ∧
        decision.committedNonPreemptively = true

/-- A transition newly commits one packet in the role state's intrinsic service ledger. -/
def SelectionIntroduced
    (before after : RoleState State kind)
    (packet : PayloadId) : Prop :=
  packet ∉ before.committedService ∧ packet ∈ after.committedService

/--
Every newly committed service packet has exactly the decision record carrying it, and every
decision record denotes such a newly committed packet. This closes optional metadata for the
observable ledger; `TxReadySelectionPrivateIrrelevant` separately excludes an earlier reservation
hidden only in private state.
-/
def ServiceDecisionTraceComplete
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    ∀ packet,
      SelectionIntroduced state result.nextState packet ↔
        ∃ decision ∈ result.decisions, decision.packet = packet

/--
Two transition results agree on every service-selection observable. Private bookkeeping may differ,
but the ordered queue, committed ledger, and recorded decision do not.
-/
def SameServiceSelectionResult
    (left right : TransitionResult State kind) : Prop :=
  left.nextState.serviceQueue = right.nextState.serviceQueue ∧
    left.nextState.committedService = right.nextState.committedService ∧
    left.decisions = right.decisions

/-- Service-selection result agreement is executable for finite result data. -/
instance
    (left right : TransitionResult State kind) :
    Decidable (SameServiceSelectionResult left right) := by
  unfold SameServiceSelectionResult
  infer_instance

/--
A `TxReady` service choice is a function only of the ordered queue and committed-service ledger
visible at that instant. The alternate transition must exist for every replacement private state,
so a relation cannot make this premise vacuous with a private/ledger consistency guard.

An accepted FIFO instance is satisfiable: arrivals mutate `serviceQueue`, and `TxReady` removes its
head and commits it without consulting private counters or other bookkeeping.
-/
def TxReadySelectionPrivateIrrelevant
    (transition : TransitionRelation State) : Prop :=
  ∀ node event state result,
    transition node event state result →
    event.kind = .txReady →
    ∀ privateState,
      ∃ alternateResult,
        transition node event
          { privateState
            serviceQueue := state.serviceQueue
            committedService := state.committedService }
          alternateResult ∧
        SameServiceSelectionResult result alternateResult

/-- Complete service-start contract used by F2–F4. -/
def CompleteActualServiceStartDiscipline
    (transition : TransitionRelation State) : Prop :=
  ActualServiceStartDiscipline transition ∧
    ServiceDecisionTraceComplete transition ∧
    TxReadySelectionPrivateIrrelevant transition

/--
Every initial event has a supported handler for its target role, mirroring validation before
`executor/src/scalar.rs:638-650`.
-/
def InitialEventsRoleCorrect
    (image : SimulationImage State) : Prop :=
  ∀ event ∈ image.initialEvents,
    ∃ node ∈ image.nodes,
      event.target = node.id ∧ roleSupports node.kind event.kind

/--
Conservative queue-conflict class for the accepted FIFO/TailDrop handlers.

Rust's arrival, readiness, and completion handlers all mutate the same finite-capacity queue
(`executor/src/scalar.rs:975-1016,1091-1233`), so formal "same-type" ordering must mean a shared
queue-conflict class rather than literal equality of `EventKind`. This intentionally restricts
same-node reordering while still permitting same-stage batching across LPs.
-/
def fifoQueueConflictClass (_kind : EventKind) : Nat :=
  0

end DaysExecutor
