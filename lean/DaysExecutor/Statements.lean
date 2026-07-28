import DaysExecutor.Counterexamples

namespace DaysExecutor

/--
F1 theorem statement from plan §8: actual unseen remote events produced by a sequential round drain
are at or beyond their destination LP's valid bound, generalizing the runtime outbox assertion at
`executor/src/safe_horizon.rs:322-327`.

This is intentionally a `Prop`-valued definition for T11; T12 supplies the proof.
-/
def F1RemoteLowerBound
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  AcceptedModel image transition →
    ∀ bounds start drainedEvents afterDrain,
      PostExchangeStart image start →
      BoundFamilyValid transition start.machine bounds →
      SequentialRoundDrain image transition bounds start drainedEvents afterDrain →
      (∀ target event,
        UnseenRemoteAt transition start.machine.pending target event →
        bounds target ≤ event.key.timeNs) ∧
      (∀ event ∈ flattenedOutboxes image afterDrain,
        ¬ belowBound bounds event)

/--
General cut-form serializability obligation behind F2: per-LP sequential half-open drains plus one
complete exchange equal least-key serial execution restricted to the same drained consistent cut,
mirroring `executor/src/scalar.rs:335-359` against
`executor/src/safe_horizon.rs:241-365`.
-/
def RoundSerializabilityOverCut
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  AcceptedModel image transition →
    ActualServiceStartDiscipline transition →
    ∀ bounds cut start drainedEvents finish,
      SafeHorizonRound image transition bounds cut start drainedEvents finish →
      ∃ serialOrder serialFinish,
        CanonicalSerialRestricted image transition cut
          start.machine serialOrder serialFinish ∧
        (∀ event, event ∈ serialOrder ↔ event ∈ drainedEvents) ∧
        SameMachineResult image serialFinish finish.machine

/--
F2 constant-instance corollary statement: a drained consistent cut under
`B_j = (H,0,0,0)` is exactly the reachable global time prefix below `H`, recovering Rust's V1
statement at `executor/src/safe_horizon.rs:264-271,392-395`.

This is intentionally a `Prop`-valued definition for T11; T12 supplies the proof.
-/
def F2GlobalTimePrefixCorollary : Prop :=
  ∀ emissions startPending drainedEvents bounds horizon cut,
    IsConstantBoundFamily bounds horizon →
    DrainedConsistentCut emissions startPending drainedEvents bounds cut →
    ∀ event,
      cut event ↔ TimePrefix emissions startPending horizon event

/--
F2 theorem statement from plan §8: round serializability over arbitrary per-LP consistent cuts,
paired with the global-horizon time-prefix corollary, matching the two executor paths at
`executor/src/scalar.rs:335-359` and `executor/src/safe_horizon.rs:241-365`.

This is intentionally a `Prop`-valued definition for T11; T12 supplies the proof.
-/
def F2RoundSerializability
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  RoundSerializabilityOverCut image transition ∧
    F2GlobalTimePrefixCorollary

/--
F3 theorem statement from plan §8: repeated valid progressive rounds compose to the canonical
least-key serial run through the image's inclusive stop, preserving complete normalized state,
pending events, and observations from `executor/src/scalar.rs:322-360` and
`executor/src/safe_horizon.rs:231-365`.

This is intentionally a `Prop`-valued definition for T11; T12 supplies the proof.
-/
def F3RunComposition
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  AcceptedModel image transition →
    ActualServiceStartDiscipline transition →
    ∀ start bounds finish,
      InitialMachine image start.machine →
      PostExchangeStart image start →
      SafeHorizonRounds image transition start bounds finish →
      StoppedThroughInclusiveStop image finish →
      ∃ serialOrder serialFinish,
        CanonicalSerialThroughStop image transition
          start.machine serialOrder serialFinish ∧
        SameMachineResult image serialFinish finish.machine

/--
F4 theorem statement from plan §8: F2/F3 are scoped to state-dependent choices made only at the
actual `TxReady` service start, and the concrete eager-selection witness shows why a sound horizon
alone is insufficient. The Rust decision handlers are
`executor/src/scalar.rs:863-921,1091-1166`.

The executable witness data is already constructed in T11; its Lean proof is deferred to T12.
-/
def F4DecisionPointScope
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  (ActualServiceStartDiscipline transition →
      F2RoundSerializability image transition ∧
      F3RunComposition image transition) ∧
    EagerSelectionCounterexampleShape

/--
Per-packet causal order required by F5, corresponding to a packet's parent/child transition chain
through `executor/src/scalar.rs:351-356`.
-/
def PacketCausalBefore
    (emissions : List (Event × Event))
    (left right : Event) : Prop :=
  left.payload = right.payload ∧ RecordedCausalBefore emissions left right

/--
Per-node same queue-conflict-type order required by F5, conservatively reflecting the shared queue
mutations in `executor/src/scalar.rs:975-1016,1091-1233`.
-/
def SameNodeSameQueueTypeBefore
    (left right : Event) : Prop :=
  left.target = right.target ∧
    fifoQueueConflictClass left.kind = fifoQueueConflictClass right.kind ∧
    left.key < right.key

/--
Complete mandatory dependency order for F5: packet causality or per-node same-type queue order,
formalizing the orders that an intra-round schedule must retain around
`executor/src/scalar.rs:975-1233`.
-/
def RequiredIntraRoundBefore
    (emissions : List (Event × Event))
    (left right : Event) : Prop :=
  PacketCausalBefore emissions left right ∨
    SameNodeSameQueueTypeBefore left right

/--
One event appears strictly before another in a proposed round order, the list-level analogue of
Rust's canonical event sequence at `executor/src/scalar.rs:344-356`.
-/
def AppearsBefore (left right : Event) (events : List Event) : Prop :=
  ∃ beforePart middle afterPart,
    events = beforePart ++ left :: middle ++ right :: afterPart

/--
A candidate round order is a permutation of the drained cut that preserves all mandatory F5
dependencies, the formal policy seam for batching beyond the chronological loop in
`executor/src/safe_horizon.rs:381-448`.
-/
def PreservesRequiredIntraRoundOrder
    (emissions : List (Event × Event))
    (drained candidate : List Event) : Prop :=
  candidate.Perm drained ∧
    ∀ left ∈ drained, ∀ right ∈ drained,
      RequiredIntraRoundBefore emissions left right →
      AppearsBefore left right candidate

/--
Two events are independent only when neither mandatory F5 order relates them, matching the
disjoint-LP scheduling freedom around `executor/src/safe_horizon.rs:281-290`.
-/
def IntraRoundIndependent
    (emissions : List (Event × Event))
    (left right : Event) : Prop :=
  ¬ RequiredIntraRoundBefore emissions left right ∧
    ¬ RequiredIntraRoundBefore emissions right left

/--
Explicit commutation premise required for abstract heterogeneous handlers: swapping any declared
independent adjacent pair yields the same normalized result. Rust's deterministic dispatch alone at
`executor/src/scalar.rs:638-663` does not imply this property.
-/
def IndependentStepsCommute
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (emissions : List (Event × Event)) : Prop :=
  ∀ before left right afterLeft afterLeftRight,
    IntraRoundIndependent emissions left right →
    AvailableEventStep image transition left before afterLeft →
    AvailableEventStep image transition right afterLeft afterLeftRight →
    ∃ afterRight afterRightLeft,
      AvailableEventStep image transition right before afterRight ∧
      AvailableEventStep image transition left afterRight afterRightLeft ∧
      SameMachineResult image afterLeftRight afterRightLeft

/--
F5 theorem statement from plan §8: under an explicit dependency-completeness/commutation premise,
any permutation preserving per-packet causality and per-node same queue-type order produces the
same final state and canonical observations. It also retains the concrete unsound reversal shape
forced by the finite-capacity handlers at
`executor/src/scalar.rs:975-1016,1091-1128`.

The commutation premise records an implementation/specification tension: those two base orders are
not consequences of deterministic abstract handlers, and literal `EventKind` equality would miss
arrival-versus-service capacity conflicts. This is intentionally a `Prop`-valued definition for
T11. T12 proves the conditional theorem; because handler bodies remain abstract, establishing
commutation for concrete handlers is a separate future obligation.
-/
def F5IntraRoundReordering
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  (AcceptedModel image transition →
    ∀ bounds cut start drainedEvents roundFinish
        canonicalOrder canonicalFinish candidateOrder candidateFinish,
      SafeHorizonRound image transition bounds cut
        start drainedEvents roundFinish →
      CanonicalSerialRestricted image transition cut
        start.machine canonicalOrder canonicalFinish →
      IndependentStepsCommute image transition canonicalFinish.emissions →
      (∀ event, event ∈ canonicalOrder ↔ event ∈ drainedEvents) →
      PreservesRequiredIntraRoundOrder canonicalFinish.emissions
        drainedEvents candidateOrder →
      ExecutionInOrder image transition
        start.machine candidateOrder candidateFinish →
      SameMachineResult image canonicalFinish candidateFinish) ∧
    UnsoundReorderingCounterexampleShape

end DaysExecutor
