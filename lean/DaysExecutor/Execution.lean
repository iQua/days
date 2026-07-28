import DaysExecutor.Transition

namespace DaysExecutor

/--
Semantic machine configuration shared by serial and round execution, corresponding to the complete
state and pending-event result assembled at `executor/src/scalar.rs:575-602` and
`executor/src/cpu.rs:3372-3433`.

`nextOriginSeq`, `allocatedKeys`, and `emissions` are proof-only ghosts. The remaining fields
project every Rust `RunResult` component, including descriptor data that crosses LPs.
-/
structure MachineState (State : StateFamily) where
  localState : (node : NodeDescriptor) → RoleState State node.kind
  packetStore : NodeDescriptor → List PacketDescriptor
  pending : List Event
  summary : RunSummary
  observedPackets : List PacketDescriptor
  departures : List RecordedDeparture
  arrivals : List RecordedArrival
  nextOriginSeq : NodeId → Nat
  allocatedKeys : List EventKey
  emissions : List (Event × Event)

/--
Canonical insertion into an `EventKey`-ordered future-event list, modeling Rust's `BTreeMap`
insertion at `executor/src/scalar.rs:352-355`.
-/
def insertEvent (event : Event) : List Event → List Event
  | [] => [event]
  | head :: tail =>
      if event.key ≤ head.key then
        event :: head :: tail
      else
        head :: insertEvent event tail

/--
Canonical insertion of deterministic child emissions, modeling the scalar child loop at
`executor/src/scalar.rs:351-356`.
-/
def insertEvents (children pending : List Event) : List Event :=
  children.foldl (fun queue child => insertEvent child queue) pending

/--
Canonical pending-event normalization used for initial images and comparison, corresponding to the
ordered scalar queue built at `executor/src/scalar.rs:1714-1723`.
-/
def canonicalizeEvents (events : List Event) : List Event :=
  insertEvents events []

/--
Generated child keys are fresh relative to the remaining future-event list, matching the duplicate
diagnostic at `executor/src/scalar.rs:352-355`.
-/
def FreshEventKeys (children existing : List Event) : Prop :=
  ∀ child ∈ children, ∀ pending ∈ existing, child.key ≠ pending.key

/--
Canonical descriptor ordering by payload ID, matching the `BTreeMap`-normalized result assembly at
`executor/src/scalar.rs:577-590` and `executor/src/cpu.rs:3383-3426`.
-/
def descriptorLE (left right : PacketDescriptor) : Prop :=
  left.id ≤ right.id

/-- Decidability for payload-keyed descriptor normalization. -/
instance (left right : PacketDescriptor) : Decidable (descriptorLE left right) := by
  unfold descriptorLE
  infer_instance

/--
Install one immutable descriptor in payload order. A descriptor already installed for its payload
is retained; `DescriptorStoreCoherent` rules out a conflicting value.
-/
def installDescriptor (descriptor : PacketDescriptor) : List PacketDescriptor → List PacketDescriptor
  | [] => [descriptor]
  | head :: tail =>
      if descriptor.id = head.id then
        head :: tail
      else if descriptorLE descriptor head then
        descriptor :: head :: tail
      else
        head :: installDescriptor descriptor tail

/-- A local descriptor store contains unique payload IDs and exactly the oracle values. -/
def DescriptorStoreCoherent
    (image : SimulationImage State)
    (store : List PacketDescriptor) : Prop :=
  (store.map PacketDescriptor.id).Nodup ∧
    ∀ descriptor ∈ store,
      descriptor = image.packetDescriptor descriptor.id

/--
Remove one payload descriptor from a local resident store.
-/
def removeDescriptor (payload : PayloadId) (store : List PacketDescriptor) : List PacketDescriptor :=
  store.filter fun descriptor => descriptor.id ≠ payload

/-- Canonical global payload-ID projection used by `RunResult.resident_packets`. -/
def canonicalizeDescriptors (descriptors : List PacketDescriptor) : List PacketDescriptor :=
  descriptors.foldl
    (fun current descriptor => installDescriptor descriptor current)
    []

/-- Apply one transition's resident-packet effects in their deterministic result-list order. -/
def applyPacketEffects
    (result : TransitionResult State kind)
    (store : List PacketDescriptor) : List PacketDescriptor :=
  result.packetInstalls.foldl
    (fun current descriptor => installDescriptor descriptor current)
    (result.packetRemovals.foldl
      (fun current payload => removeDescriptor payload current)
      store)

/--
Every emitted child's actual descriptor remains available at the emitting LP after transition
packet effects, matching `packet_descriptor(child.payload)` at
`executor/src/cpu.rs:627-667`.
-/
def ChildDescriptorsAvailable
    (image : SimulationImage State)
    (store : List PacketDescriptor)
    (children : List Event) : Prop :=
  ∀ child ∈ children,
    image.packetDescriptor child.payload ∈ store

/--
Install the immutable descriptors of children addressed to one LP. In the scalar executor all
handlers share one packet map (`executor/src/scalar.rs:362-372,545-565`), so its per-LP projection
materializes this availability when a child enters the future-event list. The exchanged executor
does the corresponding target installation from descriptor-carrying envelopes at
`executor/src/cpu.rs:664-667,4485-4500`.
-/
def installChildDescriptorsFor
    (image : SimulationImage State)
    (target : NodeId)
    (children : List Event)
    (store : List PacketDescriptor) : List PacketDescriptor :=
  children.foldl
    (fun current child =>
      if child.target = target then
        installDescriptor (image.packetDescriptor child.payload) current
      else
        current)
    store

/-- Insert a keyed departure in canonical event-key order. -/
def insertDeparture (record : RecordedDeparture) : List RecordedDeparture → List RecordedDeparture
  | [] => [record]
  | head :: tail =>
      if record.eventKey ≤ head.eventKey then record :: head :: tail
      else head :: insertDeparture record tail

/-- Insert a keyed arrival in canonical event-key order. -/
def insertArrival (record : RecordedArrival) : List RecordedArrival → List RecordedArrival
  | [] => [record]
  | head :: tail =>
      if record.eventKey ≤ head.eventKey then record :: head :: tail
      else head :: insertArrival record tail

/-- Canonically merge transition output into the complete normalized result surface. -/
def applyRecordedOutput
    (result : TransitionResult State kind)
    (before after : MachineState State) : Prop :=
  after.summary = RunSummary.add before.summary result.summaryDelta ∧
    after.observedPackets =
      result.observedPackets.foldl
        (fun current descriptor => installDescriptor descriptor current)
        before.observedPackets ∧
    after.departures =
      result.departures.foldl
        (fun current record => insertDeparture record current)
        before.departures ∧
    after.arrivals =
      result.arrivals.foldl
        (fun current record => insertArrival record current)
        before.arrivals

/--
Consecutive lifetime origin-sequence allocation in child-emission order, matching cursor
consumption at `executor/src/scalar.rs:1236-1297`.
-/
def ChildrenUseOriginSequence
    (origin : NodeId) : Nat → List Event → Prop
  | _, [] => True
  | next, child :: tail =>
      child.key.originNode = origin ∧
        child.key.originSeq = next ∧
        ChildrenUseOriginSequence origin (next + 1) tail

/--
Atomic lifetime key allocation for one transition. Consumed keys remain in `allocatedKeys`, and
children consume consecutive sequence values in result-list order.
-/
def AllocatesChildrenInOrder
    (node : NodeDescriptor)
    (children : List Event)
    (before after : MachineState State) : Prop :=
  ChildrenUseOriginSequence node.id (before.nextOriginSeq node.id) children ∧
    (children.map Event.key).Nodup ∧
    (∀ child ∈ children, child.key ∉ before.allocatedKeys) ∧
    after.allocatedKeys = before.allocatedKeys ++ children.map Event.key ∧
    after.nextOriginSeq node.id =
      before.nextOriginSeq node.id + children.length ∧
    ∀ other,
      other ≠ node.id →
      after.nextOriginSeq other = before.nextOriginSeq other

/--
Strictly key-ordered pending queue expected by both Rust executors at
`executor/src/scalar.rs:341-350` and `executor/src/safe_horizon.rs:170`.
-/
def CanonicalPending (pending : List Event) : Prop :=
  pending.Pairwise (fun left right => left.key < right.key)

/--
One event is the least currently eligible event, matching the scalar `pop_first` choice at
`executor/src/scalar.rs:344-350`.
-/
def IsLeastEligible
    (eligible : Event → Prop)
    (event : Event)
    (pending : List Event) : Prop :=
  event ∈ pending ∧
    eligible event ∧
    ∀ other ∈ pending, eligible other → event.key ≤ other.key

/--
No pending event satisfies the current boundary/cut predicate, matching loop termination at
`executor/src/scalar.rs:344-347`.
-/
def NoEligibleEvent (eligible : Event → Prop) (pending : List Event) : Prop :=
  ∀ event ∈ pending, ¬ eligible event

/--
LP-local transition application: only the processing LP's mutable role state and descriptor store
change; all normalized result effects are applied explicitly. This mirrors exclusive CPU state-slot
ownership before remote exchange at `executor/src/cpu.rs:586-690`.
-/
def AppliesTransitionResult
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (result : TransitionResult State node.kind)
    (before after : MachineState State) : Prop :=
  after.localState node = result.nextState ∧
    after.packetStore node =
      applyPacketEffects result (before.packetStore node) ∧
    (∀ other ∈ image.nodes,
      other.id ≠ node.id →
      after.localState other = before.localState other ∧
        after.packetStore other = before.packetStore other) ∧
    applyRecordedOutput result before after

/--
Scalar transition application in the model's per-LP descriptor projection. Rust scalar execution
has one shared packet map, so every emitted child can use its descriptor immediately; the
projection records that availability at the child's target while preserving exact local state and
output effects. This is the scalar counterpart of the target copies installed by
`CompleteCanonicalExchange`, not a weakening of final resident-packet comparison.
-/
def AppliesScalarTransitionResult
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (result : TransitionResult State node.kind)
    (before after : MachineState State) : Prop :=
  after.localState node = result.nextState ∧
    after.packetStore node =
      applyPacketEffects result (before.packetStore node) ∧
    (∀ other ∈ image.nodes,
      other.id ≠ node.id →
      after.localState other = before.localState other ∧
        after.packetStore other =
          installChildDescriptorsFor image other.id result.children
            (before.packetStore other)) ∧
    applyRecordedOutput result before after

/--
One arbitrary available-event step used to define an explicit candidate reordering; unlike the
canonical scalar loop, it does not itself impose least-key choice. Child descriptors become
available in their target LP projections when their events enter the scalar future list
(`executor/src/scalar.rs:344-356,362-372`).
-/
def AvailableEventStep
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (event : Event)
    (before after : MachineState State) : Prop :=
  event ∈ before.pending ∧
    ∃ node ∈ image.nodes, ∃ result,
      event.target = node.id ∧
      transition node event (before.localState node) result ∧
      FreshEventKeys result.children (before.pending.erase event) ∧
      AllocatesChildrenInOrder node result.children before after ∧
      AppliesScalarTransitionResult image node result before after ∧
      DescriptorStoreCoherent image (after.packetStore node) ∧
      ChildDescriptorsAvailable image (after.packetStore node) result.children ∧
      after.pending = insertEvents result.children (before.pending.erase event) ∧
      after.emissions =
        before.emissions ++ (result.children.map fun child => (event, child))

/--
One canonical least-key-first serial step over the same semantic image, mirroring
`executor/src/scalar.rs:335-359`.
-/
def CanonicalSerialStep
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (eligible : Event → Prop)
    (before : MachineState State)
    (event : Event)
    (after : MachineState State) : Prop :=
  IsLeastEligible eligible event before.pending ∧
    AvailableEventStep image transition event before after

/--
Finite canonical least-key-first reference execution, mirroring repeated scalar dispatch at
`executor/src/scalar.rs:335-359`.
-/
inductive CanonicalSerialExecution
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (eligible : Event → Prop) :
    MachineState State → List Event → MachineState State → Prop
  | refl (state) :
      CanonicalSerialExecution image transition eligible state [] state
  | step
      (first :
        CanonicalSerialStep image transition eligible before event middle)
      (rest :
        CanonicalSerialExecution image transition eligible middle events after) :
      CanonicalSerialExecution image transition eligible before (event :: events) after

/--
Execution in a caller-supplied event order for F5, using the same transition and immediate child
insertion semantics as `executor/src/scalar.rs:351-356`.
-/
inductive ExecutionInOrder
    (image : SimulationImage State)
    (transition : TransitionRelation State) :
    MachineState State → List Event → MachineState State → Prop
  | refl (state) :
      ExecutionInOrder image transition state [] state
  | step
      (first : AvailableEventStep image transition event before middle)
      (rest : ExecutionInOrder image transition middle events after) :
      ExecutionInOrder image transition before (event :: events) after

/--
Canonical serial execution restricted to a consistent cut: non-cut events remain pending while the
least eligible cut event runs. This is the cut projection required when per-LP bounds differ; the
global scalar loop at `executor/src/scalar.rs:344-350` is the constant-bound specialization.
-/
def CanonicalSerialRestricted
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (cut : Event → Prop)
    (before : MachineState State)
    (executed : List Event)
    (after : MachineState State) : Prop :=
  CanonicalSerialExecution image transition cut before executed after ∧
    NoEligibleEvent cut after.pending

/--
Canonical scalar execution through Rust's inclusive configured stop, intentionally distinct from
the half-open round bound (`executor/src/scalar.rs:322-359`).
-/
def CanonicalSerialThroughStop
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (before : MachineState State)
    (executed : List Event)
    (after : MachineState State) : Prop :=
  CanonicalSerialExecution image transition (withinInclusiveStop image.stopTimeNs)
      before executed after ∧
    NoEligibleEvent (withinInclusiveStop image.stopTimeNs) after.pending

/--
Canonical initial descriptor store derived for each event-owning LP. Rust's CPU constructor derives
the same ownership partition from initial events and mutable state at
`executor/src/cpu.rs:3034-3199`; the total oracle also covers dynamically generated packets.
-/
def initialPacketStore
    (image : SimulationImage State)
    (node : NodeDescriptor) : List PacketDescriptor :=
  image.initialPacketStore node.id

/--
Structural machine invariant used at round boundaries: pending events are declared and supported,
their lifetime keys remain allocated below the corresponding cursor, and their immutable packet
descriptors are installed at the target LP.
-/
def MachineWellFormed
    (image : SimulationImage State)
    (machine : MachineState State) : Prop :=
  CanonicalPending machine.pending ∧
    machine.allocatedKeys.Nodup ∧
    (∀ key ∈ machine.allocatedKeys,
      key.originSeq < machine.nextOriginSeq key.originNode ∧
        ∃ origin ∈ image.nodes, key.originNode = origin.id) ∧
    (∀ event ∈ machine.pending,
      event.key ∈ machine.allocatedKeys ∧
        ∃ node ∈ image.nodes,
          event.target = node.id ∧
            roleSupports node.kind event.kind ∧
            image.packetDescriptor event.payload ∈ machine.packetStore node) ∧
    (∀ node ∈ image.nodes,
      DescriptorStoreCoherent image (machine.packetStore node)) ∧
    DescriptorStoreCoherent image machine.observedPackets

/--
Initial machine relation resolving Rust's role arena plus `state_slot` representation at
`executor/src/image.rs:247-260` into the semantic node-indexed view used by the proof, including
the validated initial origin-sequence cursors from `executor/src/validate.rs:1876-1953`.
-/
def InitialMachine
    (image : SimulationImage State)
    (machine : MachineState State) : Prop :=
  machine.pending = canonicalizeEvents image.initialEvents ∧
    machine.summary = RunSummary.zero ∧
    machine.observedPackets = [] ∧
    machine.departures = [] ∧
    machine.arrivals = [] ∧
    machine.nextOriginSeq = image.initialNextOriginSeq ∧
    machine.allocatedKeys = image.initialEvents.map Event.key ∧
    machine.emissions = [] ∧
    (∀ node ∈ image.nodes,
      stateAt? image node = some (machine.localState node) ∧
        machine.packetStore node = initialPacketStore image node) ∧
    MachineWellFormed image machine

/--
Canonical scalar reachability invariant used for handler progress. It contains exactly successful
prefixes from an initial accepted machine, rather than arbitrary role/state combinations that Rust
handlers reject with `ExecutionError` at `executor/src/scalar.rs:80-165,863-972,1091-1233`.
-/
def CanonicallyReachableMachine
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (machine : MachineState State) : Prop :=
  ∃ initial executed,
    InitialMachine image initial ∧
      CanonicalSerialExecution image transition (fun _ => True)
        initial executed machine

/--
Successful-handler progress only at a canonically reachable least-event configuration. This is the
reachable-state invariant relied on by the theorem statements; it does not claim Rust's checked
handlers succeed on inconsistent arbitrary states. Enabledness is required for the least pending
event of each LP, which covers canonical scalar execution and ownership-preserving LP drains
without admitting invalid same-LP reorderings.
-/
def TransitionEnabledOnReachable
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  ∀ machine,
    CanonicallyReachableMachine image transition machine →
    ∀ node ∈ image.nodes,
      ∀ event,
        IsLeastEligible
          (fun candidate => candidate.target = node.id)
          event
          machine.pending →
        event.target = node.id →
        roleSupports node.kind event.kind →
        ∃ after, AvailableEventStep image transition event machine after

/--
Accepted heterogeneous image plus its successful abstract transition semantics. Static checks and
transition safety hold globally; enabledness is required only for canonically reachable
configurations, matching the validator-established preconditions consumed by the partial Rust
handlers.
-/
def AcceptedModel
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  StaticImageWellFormed image ∧
    InitialEventsRoleCorrect image ∧
    TransitionAxioms image transition ∧
    TransitionEnabledOnReachable image transition

/--
Non-arena semantic projection of the `RunResult` fields at `executor/src/scalar.rs:64-78`.
`SameMachineResult` separately compares every node's local arena state.
-/
structure RunResultView where
  summary : RunSummary
  residentPackets : List PacketDescriptor
  observedPackets : List PacketDescriptor
  departures : List PacketDeparture
  arrivals : List PacketArrivalObservation
  pendingEvents : List Event

/--
Deterministic non-arena portion of the full result projection. Resident packet data is globally
normalized from every LP descriptor store as in `executor/src/cpu.rs:3372-3433`; the accompanying
`SameMachineResult` relation projects the host/switch arenas through validated node descriptors.
-/
def projectRunResult
    (image : SimulationImage State)
    (machine : MachineState State) : RunResultView :=
  { summary := machine.summary
    residentPackets :=
      canonicalizeDescriptors (image.nodes.flatMap machine.packetStore)
    observedPackets := canonicalizeDescriptors machine.observedPackets
    departures := machine.departures.map RecordedDeparture.departure
    arrivals := machine.arrivals.map RecordedArrival.arrival
    pendingEvents := machine.pending }

/--
Complete normalized-result equality used by all cross-executor claims. Descriptor installation,
summary counters, full observations, pending events, and both role arenas must all agree; only
proof ghosts are excluded.
-/
def SameMachineResult
    (image : SimulationImage State)
    (left right : MachineState State) : Prop :=
  (∀ node ∈ image.nodes, left.localState node = right.localState node) ∧
    projectRunResult image left = projectRunResult image right

/--
One direct parent/child emission edge produced by the abstract handler relation, corresponding to
child generation at `executor/src/scalar.rs:351-356`.
-/
def PotentialEmissionEdge
    (transition : TransitionRelation State)
    (parent child : Event) : Prop :=
  ∃ node state result,
    transition node parent state result ∧ child ∈ result.children

/--
Transitive causal/emission order generated by handler children, formalizing the dependency behind
immediate local insertion at `executor/src/safe_horizon.rs:422-435`.
-/
inductive PotentialCausalBefore
    (transition : TransitionRelation State) : Event → Event → Prop
  | direct (edge : PotentialEmissionEdge transition parent child) :
      PotentialCausalBefore transition parent child
  | tail
      (edge : PotentialEmissionEdge transition parent middle)
      (rest : PotentialCausalBefore transition middle child) :
      PotentialCausalBefore transition parent child

/--
Events reachable from round-start pending work through zero or more handler emissions, including
local children drained immediately at `executor/src/safe_horizon.rs:422-435`.
-/
inductive PotentiallyReachableEvent
    (transition : TransitionRelation State)
    (startPending : List Event) : Event → Prop
  | seed (member : event ∈ startPending) :
      PotentiallyReachableEvent transition startPending event
  | child
      (parentReachable : PotentiallyReachableEvent transition startPending parent)
      (edge : PotentialEmissionEdge transition parent child) :
      PotentiallyReachableEvent transition startPending child

/--
An unseen remote event for a target is any transitively reachable child crossing an LP boundary,
not merely an event already present in the current outbox. This is the semantic closure required
by relay chains beyond `executor/src/safe_horizon.rs:309-335`.
-/
def UnseenRemoteAt
    (transition : TransitionRelation State)
    (startPending : List Event)
    (target : NodeId)
    (event : Event) : Prop :=
      event.target = target ∧
    ∃ parent,
      PotentiallyReachableEvent transition startPending parent ∧
      PotentialEmissionEdge transition parent event ∧
      parent.target ≠ event.target

/--
One parent/child edge recorded by an actual successful execution step, corresponding to the child
loop at `executor/src/scalar.rs:351-356`.
-/
def RecordedEmissionEdge
    (emissions : List (Event × Event))
  (parent child : Event) : Prop :=
  (parent, child) ∈ emissions

/--
Exact ghost-emission suffix produced since a round start. Lifetime history remains on the machine,
but consistent-cut causality consumes only this suffix so round-start pending events are causal
roots.
-/
def RoundEmissionDelta
    (start finish : MachineState State)
    (delta : List (Event × Event)) : Prop :=
  finish.emissions = start.emissions ++ delta

/--
Trace-specific causal closure of actual recorded child emissions, avoiding hypothetical handler
branches while modeling immediate insertion at `executor/src/safe_horizon.rs:422-435`.
-/
inductive RecordedCausalBefore
    (emissions : List (Event × Event)) : Event → Event → Prop
  | direct (edge : RecordedEmissionEdge emissions parent child) :
      RecordedCausalBefore emissions parent child
  | tail
      (edge : RecordedEmissionEdge emissions parent middle)
      (rest : RecordedCausalBefore emissions middle child) :
      RecordedCausalBefore emissions parent child

/--
Actual events reachable from round-start pending work through the recorded emission trace,
including local children drained immediately at `executor/src/safe_horizon.rs:422-435`.
-/
inductive RecordedReachableEvent
    (emissions : List (Event × Event))
    (startPending : List Event) : Event → Prop
  | seed (member : event ∈ startPending) :
      RecordedReachableEvent emissions startPending event
  | child
      (parentReachable : RecordedReachableEvent emissions startPending parent)
      (edge : RecordedEmissionEdge emissions parent child) :
      RecordedReachableEvent emissions startPending child

/--
Per-LP key-prefix closure over the reachable serial event universe, formalizing the cut drained by
per-LP variants of `executor/src/safe_horizon.rs:381-448`.
-/
def PerLPPrefix
    (eventUniverse cut : Event → Prop) : Prop :=
  ∀ later, cut later →
    ∀ earlier, eventUniverse earlier →
      earlier.target = later.target →
      earlier.key < later.key →
      cut earlier

/--
Backward closure of a cut under the transition emission order, formalizing the causal constraint
implicit in immediate child insertion at `executor/src/safe_horizon.rs:422-435`.
-/
def CausallyClosed
    (emissions : List (Event × Event))
    (cut : Event → Prop) : Prop :=
  ∀ parent child,
    RecordedCausalBefore emissions parent child →
    cut child →
    cut parent

/--
Consistent cut: a reachable per-LP key prefix closed under the current round's causal/emission
order, specifying the semantic set drained by `executor/src/safe_horizon.rs:268-336`.
-/
def IsConsistentCut
    (emissions : List (Event × Event))
    (startPending : List Event)
    (cut : Event → Prop) : Prop :=
  (∀ event, cut event → RecordedReachableEvent emissions startPending event) ∧
    PerLPPrefix (RecordedReachableEvent emissions startPending) cut ∧
    CausallyClosed emissions cut

/--
Global time-prefix event set corresponding to Rust's constant horizon drain at
`executor/src/safe_horizon.rs:264-271,392-395`.
-/
def TimePrefix
    (emissions : List (Event × Event))
    (startPending : List Event)
    (horizon : Nat)
    (event : Event) : Prop :=
  RecordedReachableEvent emissions startPending event ∧ event.key.timeNs < horizon

/--
The concrete drained list represents exactly the events reachable from the round-start roots below
their target LP's bound and forms a cut closed under round-local emissions, generalizing
`executor/src/safe_horizon.rs:381-448`.
-/
def DrainedConsistentCut
    (emissions : List (Event × Event))
    (startPending drained : List Event)
    (bounds : NodeId → Nat)
    (cut : Event → Prop) : Prop :=
  IsConsistentCut emissions startPending cut ∧
    (∀ event, event ∈ drained ↔ cut event) ∧
    (∀ event, cut event ↔
      RecordedReachableEvent emissions startPending event ∧ belowBound bounds event)

end DaysExecutor
