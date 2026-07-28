import DaysExecutor.Execution

namespace DaysExecutor

/--
Per-LP exclusive time-bound family generalizing Rust's one global horizon argument to
`drain_lp` at `executor/src/safe_horizon.rs:381-385`.
-/
abbrev BoundFamily := NodeId → Nat

/--
Finite time or infinity representation for empty LP frontiers, formalizing the `Option` frontier
used around `executor/src/safe_horizon.rs:248-264`.
-/
abbrev ExtendedTime := Option Nat

/--
Minimum of two finite-or-infinite times, where `none` represents infinity, matching empty frontier
handling at `executor/src/safe_horizon.rs:248-264`.
-/
def extendedMin : ExtendedTime → ExtendedTime → ExtendedTime
  | none, right => right
  | left, none => left
  | some left, some right => some (Nat.min left right)

/--
Minimum time in a finite event list, or infinity for no events, corresponding to the owner frontier
index at `executor/src/safe_horizon.rs:131-145`.
-/
def minimumEventTime (events : List Event) : ExtendedTime :=
  events.foldl
    (fun current event => extendedMin current (some event.key.timeNs))
    none

/--
Least pending event time for one LP, or infinity if it is idle, corresponding to
`executor/src/safe_horizon.rs:108-127`.
-/
def leastPendingTimeFor (pending : List Event) (node : NodeId) : ExtendedTime :=
  minimumEventTime (pending.filter fun event => event.target = node)

/--
Global least pending event time, or infinity when the run has no work, mirroring the frontier read
at `executor/src/safe_horizon.rs:248-255`.
-/
def globalLeastPendingTime (machine : MachineState State) : ExtendedTime :=
  minimumEventTime machine.pending

/--
Explicit `min_i N_i` reduction over per-LP frontiers, corresponding to the global frontier used at
`executor/src/safe_horizon.rs:248-264`.
-/
def minimumLPFrontier
    (image : SimulationImage State)
    (machine : MachineState State) : ExtendedTime :=
  image.nodes.foldl
    (fun current node =>
      extendedMin current (leastPendingTimeFor machine.pending node.id))
    none

/--
Minimum declared remote lookahead, or infinity for an image with no channels, mirroring
`executor/src/safe_horizon.rs:200-204`.
-/
def minimumChannelDelay (image : SimulationImage State) : ExtendedTime :=
  image.channels.foldl
    (fun current channel => extendedMin current (some channel.minDelayNs))
    none

/--
Finite frontier-plus-lookahead candidate; infinity propagates as in Rust's
`TIME_AFTER_U64_MAX` fallback at `executor/src/safe_horizon.rs:259-263`.
-/
def frontierPlusLookahead
    (frontier lookahead : ExtendedTime) : ExtendedTime :=
  match frontier, lookahead with
  | some next, some delay => some (next + delay)
  | _, _ => none

/--
V1 global horizon `min(stopExclusive, min_i N_i + L)`, mirroring
`executor/src/safe_horizon.rs:231-236,248-264`.

The image stores an inclusive stop, so `stopExclusive` is the formal `S`; this intentionally does
not identify the Rust `stop_time_ns` field itself with the half-open boundary.
-/
def globalHorizon
    (image : SimulationImage State)
    (machine : MachineState State) : Nat :=
  let endpoint := stopExclusive image.stopTimeNs
  match frontierPlusLookahead
      (minimumLPFrontier image machine)
      (minimumChannelDelay image) with
  | none => endpoint
  | some candidate => Nat.min endpoint candidate

/--
Constant per-LP instance of the V1 global horizon, preserving the bound-family theorem shape while
matching `executor/src/safe_horizon.rs:264-271`.
-/
def constantGlobalBounds
    (image : SimulationImage State)
    (machine : MachineState State) : BoundFamily :=
  fun _ => globalHorizon image machine

/--
Predicate identifying a constant bound family, used to state Rust's global-horizon specialization
at `executor/src/safe_horizon.rs:264-271`.
-/
def IsConstantBoundFamily (bounds : BoundFamily) (horizon : Nat) : Prop :=
  bounds = fun _ => horizon

/--
The constructed V1 bound family is definitionally the constant global horizon instance from
`executor/src/safe_horizon.rs:264-271`.
-/
theorem constantGlobalBounds_isConstant
    (image : SimulationImage State)
    (machine : MachineState State) :
    IsConstantBoundFamily
      (constantGlobalBounds image machine)
      (globalHorizon image machine) := by
  rfl

/--
Validity condition for any future per-LP bound policy: no transitively reachable unseen remote
event destined for `j` lies below `B_j`. This generalizes the runtime check for immediate outbox
events at `executor/src/safe_horizon.rs:322-327`.
-/
def BoundFamilyValid
    (transition : TransitionRelation State)
    (start : MachineState State)
    (bounds : BoundFamily) : Prop :=
  ∀ target event,
    UnseenRemoteAt transition start.pending target event →
    ¬ belowBound bounds event

/--
Named obligation that the V1 constant instance satisfies the general bound-family validity
condition; T12 proves it from the accepted channel assumptions behind
`executor/src/safe_horizon.rs:200-207,259-264`.
-/
def ConstantGlobalBoundsValid
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (start : MachineState State) : Prop :=
  BoundFamilyValid transition start (constantGlobalBounds image start)

/--
A bound makes progress whenever work remains through the inclusive stop, corresponding to the
nonempty active-set advance at `executor/src/safe_horizon.rs:248-274`.
-/
def BoundFamilyMakesProgress
    (image : SimulationImage State)
    (machine : MachineState State)
    (bounds : BoundFamily) : Prop :=
  (∃ event ∈ machine.pending, withinInclusiveStop image.stopTimeNs event) →
    ∃ event ∈ machine.pending, belowBound bounds event

/--
Every per-LP round bound is capped by the exclusive endpoint derived from Rust's inclusive stop,
mirroring `run_end.min(lookahead_end)` at `executor/src/safe_horizon.rs:231-236,264`.
-/
def BoundFamilyWithinStop
    (image : SimulationImage State)
    (bounds : BoundFamily) : Prop :=
  ∀ node ∈ image.nodes, bounds node.id ≤ stopExclusive image.stopTimeNs

/--
CPU exchange payload mirroring `executor/src/cpu.rs:694-698`, with the source LP retained as a
proof-relevant owner for transferring its counted outbox reference. Descriptor equality with the
oracle prevents event-only exchange from hiding missing or corrupt packet data.
-/
structure RemoteEnvelope where
  source : NodeId
  event : Event
  packet : PacketDescriptor
  deriving DecidableEq, Repr

/-- An envelope carries exactly the descriptor belonging to its event payload. -/
def RemoteEnvelope.Coherent
    (image : SimulationImage State)
    (envelope : RemoteEnvelope) : Prop :=
  envelope.packet = image.packetDescriptor envelope.event.payload

/--
Descriptor-carrying envelopes built in child-emission order from positive counted entries at the
source LP. Rust builds them after dispatch at `executor/src/cpu.rs:627-667`; the source field is a
Lean ownership ghost used to model the later target install at lines 4485-4500.
-/
def RemoteEnvelopesFromStore
    (image : SimulationImage State)
    (source : NodeId)
    (store : List PacketStoreEntry) :
    List Event → List RemoteEnvelope → Prop
  | [], [] => True
  | event :: events, envelope :: envelopes =>
      envelope.source = source ∧
        envelope.event = event ∧
        (∃ entry ∈ store,
          entry.descriptor = envelope.packet ∧ 0 < entry.references) ∧
        envelope.packet.id = event.payload ∧
        RemoteEnvelope.Coherent image envelope ∧
        RemoteEnvelopesFromStore image source store events envelopes
  | _, _ => False

/--
Round-local machine plus one descriptor-carrying outbox per source LP, mirroring per-LP buffering
at `executor/src/cpu.rs:586-698`.
-/
structure RoundState (State : StateFamily) where
  machine : MachineState State
  outboxes : NodeId → List RemoteEnvelope

/--
Every pending event and buffered envelope is backed by a distinct positive reference at its current
LP. This is the round-local lifetime invariant combining CPU future ownership and outboxes at
`executor/src/cpu.rs:563-569,627-667` with checked zero removal at
`executor/src/scalar.rs:1661-1698`.
-/
def RoundReferencesHeld
    (image : SimulationImage State)
    (state : RoundState State) : Prop :=
  ∀ node ∈ image.nodes,
    PacketReferencesHeld
      (((state.machine.pending.filter fun event => event.target = node.id).map Event.payload) ++
        (state.outboxes node.id).map fun envelope => envelope.event.payload)
      (state.machine.packetStore node)

/--
Local children produced by one LP, corresponding to immediate local insertion at
`executor/src/safe_horizon.rs:426-432`.
-/
def localChildren (node : NodeId) (children : List Event) : List Event :=
  children.filter fun child => child.target = node

/--
Remote children produced by one LP, corresponding to outbox buffering at
`executor/src/safe_horizon.rs:433-435`.
-/
def remoteChildren (node : NodeId) (children : List Event) : List Event :=
  children.filter fun child => child.target ≠ node

/--
All source outboxes are empty, the barrier condition established after complete exchange at
`executor/src/safe_horizon.rs:309-336`.
-/
def AllOutboxesEmpty
    (image : SimulationImage State)
    (state : RoundState State) : Prop :=
  ∀ node ∈ image.nodes, state.outboxes node.id = []

/--
Post-exchange round-start premise: every earlier remote event has been merged, no outbox remains,
and the future-event list is canonical, mirroring the barrier boundary between
`executor/src/safe_horizon.rs:309-336` and the next iteration at line 245.
-/
def PostExchangeStart
    (image : SimulationImage State)
    (state : RoundState State) : Prop :=
  AllOutboxesEmpty image state ∧
    MachineWellFormed image state.machine ∧
      RoundReferencesHeld image state

/--
One sequential local LP step below its half-open bound. Local children enter the future list
immediately while remote children enter only that LP's descriptor-carrying outbox, combining the
drain shape at `executor/src/safe_horizon.rs:381-447` with CPU envelope creation at
`executor/src/cpu.rs:627-667`.
-/
def LocalRoundStep
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (node : NodeDescriptor)
    (before : RoundState State)
    (event : Event)
    (after : RoundState State) : Prop :=
  node ∈ image.nodes ∧
    IsLeastEligible
      (fun candidate => candidate.target = node.id ∧ belowBound bounds candidate)
      event
      before.machine.pending ∧
    ∃ result,
      transition node event (before.machine.localState node) result ∧
      FreshEventKeys result.children
        (before.machine.pending ++
          (image.nodes.flatMap fun owner => before.outboxes owner.id).map
            RemoteEnvelope.event) ∧
      AllocatesChildrenInOrder node result.children before.machine after.machine ∧
      AppliesTransitionResult image node result before.machine after.machine ∧
      DescriptorStoreCoherent image (after.machine.packetStore node) ∧
      ChildDescriptorsAvailable image
        (after.machine.packetStore node) result.children ∧
      after.machine.pending =
        insertEvents
          (localChildren node.id result.children)
          (before.machine.pending.erase event) ∧
      after.machine.emissions =
        before.machine.emissions ++
          (result.children.map fun child => (event, child)) ∧
      RoundReferencesHeld image after ∧
      ∃ emittedRemote,
        RemoteEnvelopesFromStore image
          node.id
          (after.machine.packetStore node)
          (remoteChildren node.id result.children)
          emittedRemote ∧
        after.outboxes node.id =
          before.outboxes node.id ++ emittedRemote ∧
        ∀ other ∈ image.nodes,
          other.id ≠ node.id →
          after.outboxes other.id = before.outboxes other.id

/--
Sequential least-local-key drain for one LP until no event remains below that LP's bound, mirroring
the half-open loop at `executor/src/safe_horizon.rs:381-448`.
-/
inductive SequentialDrainLP
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (node : NodeDescriptor) :
    RoundState State → List Event → RoundState State → Prop
  | done
      (empty :
        NoEligibleEvent
          (fun event => event.target = node.id ∧ belowBound bounds event)
          state.machine.pending) :
      SequentialDrainLP image transition bounds node state [] state
  | step
      (first : LocalRoundStep image transition bounds node before event middle)
      (rest : SequentialDrainLP image transition bounds node middle events after) :
      SequentialDrainLP image transition bounds node before (event :: events) after

/--
Sequential composition of complete LP drains in a chosen LP schedule, formalizing whole-LP
ownership in `executor/src/safe_horizon.rs:281-290`.
-/
inductive DrainLPsInOrder
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily) :
    List NodeId → RoundState State → List Event → RoundState State → Prop
  | nil (state) :
      DrainLPsInOrder image transition bounds [] state [] state
  | cons
      (nodeMember : node ∈ image.nodes)
      (first : SequentialDrainLP image transition bounds node before localEvents middle)
      (rest :
        DrainLPsInOrder image transition bounds order middle later after) :
      DrainLPsInOrder image transition bounds
        (node.id :: order) before (localEvents ++ later) after

/--
All LPs drain sequentially below their own bounds; their scheduling order may vary but must cover
the one-image LP set exactly once, mirroring the active-LP loop at
`executor/src/safe_horizon.rs:268-290`.
-/
def SequentialRoundDrain
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (before : RoundState State)
    (drained : List Event)
    (after : RoundState State) : Prop :=
  ∃ order,
    order.Perm (image.nodes.map NodeDescriptor.id) ∧
    DrainLPsInOrder image transition bounds order before drained after

/--
Strict canonical exchange order `(target_lp, EventKey)`, mirroring
`executor/src/cpu.rs:3545-3642,4453-4500`.
-/
def exchangeLT (left right : RemoteEnvelope) : Prop :=
  left.event.target < right.event.target ∨
    (left.event.target = right.event.target ∧ left.event.key < right.event.key)

/--
Flattened per-source outboxes awaiting the common barrier exchange at
`executor/src/cpu.rs:4453-4463`.
-/
def flattenedOutboxes
    (image : SimulationImage State)
    (state : RoundState State) : List RemoteEnvelope :=
  image.nodes.flatMap fun node => state.outboxes node.id

/--
Acquire all canonically ordered incoming envelope references for one target LP, corresponding to
target installation at `executor/src/cpu.rs:4485-4500`.
-/
def installRemoteEnvelopesFor
    (target : NodeId)
    (ordered : List RemoteEnvelope)
    (store : List PacketStoreEntry) : List PacketStoreEntry :=
  ordered.foldl
    (fun current envelope =>
      if envelope.event.target = target then
        incrementDescriptorReference envelope.packet current
      else
        current)
    store

/--
Consume all canonically ordered outgoing envelope references for one source LP before their target
acquisitions. Under-consumption is excluded by `CompleteCanonicalExchange`, paralleling checked
decrement at `executor/src/scalar.rs:1661-1683`.
-/
def consumeRemoteEnvelopesFor
    (source : NodeId)
    (ordered : List RemoteEnvelope)
    (store : List PacketStoreEntry) : List PacketStoreEntry :=
  ordered.foldl
    (fun current envelope =>
      if envelope.source = source then
        consumeDescriptorReference envelope.event.payload current
      else
        current)
    store

/--
Complete exactly-once canonical exchange: all buffered remote reference counts move from source
outboxes to targets before future-event insertion, and every outbox is emptied. This models envelope
creation and target installation at `executor/src/cpu.rs:649-667,4453-4506`; multiplicity-aware
heldness excludes under-consumption and descriptor resurrection.
-/
def CompleteCanonicalExchange
    (image : SimulationImage State)
    (drained next : RoundState State) : Prop :=
  ∃ ordered,
    (flattenedOutboxes image drained).Perm ordered ∧
    ordered.Pairwise exchangeLT ∧
    (∀ envelope ∈ ordered, RemoteEnvelope.Coherent image envelope) ∧
    (∀ node ∈ image.nodes,
      PacketReferencesHeld
        ((ordered.filter fun envelope => envelope.source = node.id).map
          fun envelope => envelope.event.payload)
        (drained.machine.packetStore node)) ∧
    (∀ node ∈ image.nodes,
      next.machine.localState node = drained.machine.localState node ∧
        next.machine.packetStore node =
          installRemoteEnvelopesFor node.id ordered
            (consumeRemoteEnvelopesFor node.id ordered
              (drained.machine.packetStore node))) ∧
    next.machine.pending =
      insertEvents (ordered.map RemoteEnvelope.event) drained.machine.pending ∧
    next.machine.summary = drained.machine.summary ∧
    next.machine.observedPackets = drained.machine.observedPackets ∧
    next.machine.departures = drained.machine.departures ∧
    next.machine.arrivals = drained.machine.arrivals ∧
    next.machine.nextOriginSeq = drained.machine.nextOriginSeq ∧
    next.machine.allocatedKeys = drained.machine.allocatedKeys ∧
    next.machine.emissions = drained.machine.emissions ∧
    RoundReferencesHeld image next ∧
      AllOutboxesEmpty image next

/--
One valid safe-horizon round over a drained consistent cut: post-exchange start, valid progressive
per-LP bounds, sequential local drains, and one complete barrier exchange, mirroring
`executor/src/safe_horizon.rs:241-365`.
-/
def SafeHorizonRound
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (bounds : BoundFamily)
    (cut : Event → Prop)
    (start : RoundState State)
    (drainedEvents : List Event)
    (finish : RoundState State) : Prop :=
  PostExchangeStart image start ∧
    BoundFamilyValid transition start.machine bounds ∧
    BoundFamilyMakesProgress image start.machine bounds ∧
    BoundFamilyWithinStop image bounds ∧
    ∃ afterDrain,
      SequentialRoundDrain image transition bounds start drainedEvents afterDrain ∧
      ∃ roundEmissions,
        RoundEmissionDelta start.machine afterDrain.machine roundEmissions ∧
        DrainedConsistentCut roundEmissions
          start.machine.pending drainedEvents bounds cut ∧
      CompleteCanonicalExchange image afterDrain finish ∧
      PostExchangeStart image finish

/--
Finite composition of valid safe-horizon rounds, corresponding to the repeated barrier loop at
`executor/src/safe_horizon.rs:241-365`.
-/
inductive SafeHorizonRounds
    (image : SimulationImage State)
    (transition : TransitionRelation State) :
    RoundState State → List BoundFamily → RoundState State → Prop
  | refl (state) :
      SafeHorizonRounds image transition state [] state
  | step
      (round :
        SafeHorizonRound image transition bounds cut start drained middle)
      (rest :
        SafeHorizonRounds image transition middle boundsTail finish) :
      SafeHorizonRounds image transition start (bounds :: boundsTail) finish

/--
No pending event remains through the inclusive scenario endpoint, matching scalar termination at
`executor/src/scalar.rs:344-359`.
-/
def StoppedThroughInclusiveStop
    (image : SimulationImage State)
    (state : RoundState State) : Prop :=
  NoEligibleEvent (withinInclusiveStop image.stopTimeNs) state.machine.pending

end DaysExecutor
