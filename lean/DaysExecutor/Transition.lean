import DaysExecutor.Image

namespace DaysExecutor

/--
Normalized observable record corresponding to the key-sorted scalar observations at
`executor/src/scalar.rs:575-601`.
-/
structure Observation where
  eventKey : EventKey
  ordinal : Nat
  tag : Nat
  value : Nat
  deriving DecidableEq, Repr, Ord

/--
One state-dependent, committed service selection corresponding to a `TxReady` handler at
`executor/src/scalar.rs:863-921,1091-1166`.
-/
structure ServiceDecision where
  node : NodeId
  decisionKey : EventKey
  packet : PayloadId
  committedNonPreemptively : Bool
  deriving DecidableEq, Repr

/--
Abstract result of one role-correct run-to-completion handler, mirroring the child and observation
effects of `TransitionState.dispatch` at `executor/src/scalar.rs:638-663`.
-/
structure TransitionResult (State : StateFamily) (kind : NodeKind) where
  nextState : State kind
  children : List Event
  observations : List Observation
  decisions : List ServiceDecision

/--
Per-LP abstract transition relation. The `node` argument deliberately permits different LPs and
roles to use different code while retaining Rust's `(NodeKind, EventKind)` dispatch shape at
`executor/src/scalar.rs:638-663`.
-/
abbrev TransitionRelation (State : StateFamily) :=
  (node : NodeDescriptor) →
  (event : Event) →
  State node.kind →
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
Progress/enabledness for every accepted role/event pair, matching the closed Rust dispatch table
at `executor/src/model.rs:45-60`.
-/
def TransitionEnabled
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  ∀ node ∈ image.nodes, ∀ event,
    event.target = node.id →
    roleSupports node.kind event.kind →
    ∀ state, ∃ result, transition node event state result

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
All transition assumptions used by the safe-horizon statements, collected without introducing a
runtime certificate; these mirror validation plus dispatch at
`executor/src/validate.rs:1010-1080` and `executor/src/scalar.rs:638-663,1300-1313`.
-/
def TransitionAxioms
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  TransitionDeterministic transition ∧
    TransitionEnabled image transition ∧
    TransitionRoleCorrect transition ∧
    GeneratedEventsRoleCorrect image transition ∧
    ChildrenAdvanceParent transition ∧
    ChildrenUseOwnerOrigin transition ∧
    ChildrenUseCanonicalPhase transition ∧
    TransitionChildrenHaveUniqueKeys transition ∧
    RemoteEmissionCoverage image transition ∧
    CertifiedBoundSoundness image transition

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
Accepted heterogeneous image plus its abstract closed transition semantics, corresponding to the
pre-execution validator boundary at `executor/src/validate.rs:196-245,1010-1080`.
-/
def AcceptedModel
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  StaticImageWellFormed image ∧
    InitialEventsRoleCorrect image ∧
    TransitionAxioms image transition

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
