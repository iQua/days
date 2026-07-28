import DaysExecutor.Counterexamples

namespace DaysExecutor

/--
V1 policy obligation missing from the generic bound-family lemma: the actual
`min(stopExclusive, G + L)` family computed at `executor/src/safe_horizon.rs:231-264` is valid at
every well-formed post-exchange start.
-/
def ConstantGlobalBoundPolicySound
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  AcceptedModel image transition →
    ∀ start,
      PostExchangeStart image start →
      ConstantGlobalBoundsValid image transition start.machine

/--
F1 theorem statement from plan §8. Its first conjunct proves Rust's concrete `G + L` policy rather
than assuming its conclusion; the second retains the general bound-family consequence and the
runtime outbox assertion at `executor/src/safe_horizon.rs:322-327`.

This is intentionally a `Prop`-valued definition for T11; T12 supplies the proof.
-/
def F1RemoteLowerBound
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  ConstantGlobalBoundPolicySound image transition ∧
    (AcceptedModel image transition →
      ∀ bounds start drainedEvents afterDrain,
        PostExchangeStart image start →
        BoundFamilyValid transition start.machine bounds →
        SequentialRoundDrain image transition bounds start drainedEvents afterDrain →
        (∀ target event,
          UnseenRemoteAt transition start.machine.pending target event →
          bounds target ≤ event.key.timeNs) ∧
        (∀ envelope ∈ flattenedOutboxes image afterDrain,
          ¬ belowBound bounds envelope.event))

/--
Post-exchange boundary reached by zero or more actual safe-horizon rounds from an initial machine.
This is plan §4.2's execution-boundary scope. The T12 halt countermodel showed that
`PostExchangeStart` alone also admits arbitrary well-formed but unreachable machines on which
round serializability is false.
-/
def ReachablePostExchangeStart
    (image : SimulationImage State)
    (transition : TransitionRelation State)
    (start : RoundState State) : Prop :=
  ∃ initial : RoundState State, ∃ prefixBounds : List BoundFamily,
    InitialMachine image initial.machine ∧
      PostExchangeStart image initial ∧
      SafeHorizonRounds image transition initial prefixBounds start

/--
Reachability glue for iterating F2 inside F3: completing one valid round from an actual
post-exchange boundary produces the next actual post-exchange boundary.

This is intentionally a `Prop`-valued definition for T11; T12 supplies the proof.
-/
def ReachablePostExchangeStartAfterRound
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  ∀ bounds cut start drainedEvents finish,
    ReachablePostExchangeStart image transition start →
    SafeHorizonRound image transition bounds cut start drainedEvents finish →
    ReachablePostExchangeStart image transition finish

/--
General cut-form serializability obligation behind F2: per-LP sequential half-open drains plus one
complete exchange equal least-key serial execution restricted to the same drained consistent cut,
mirroring `executor/src/scalar.rs:335-359` against `executor/src/safe_horizon.rs:241-365`.
Following plan §4.2, its start is a post-exchange boundary of an actual execution rather than an
arbitrary well-formed state admitted by the T12 halt countermodel.
-/
def RoundSerializabilityOverCut
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  AcceptedModel image transition →
    CompleteActualServiceStartDiscipline transition →
    ∀ bounds cut start drainedEvents finish,
      ReachablePostExchangeStart image transition start →
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
least-key serial run through the image's inclusive stop, preserving every normalized `RunResult`
field from `executor/src/scalar.rs:64-78,322-360` and
`executor/src/safe_horizon.rs:231-365`.

This is intentionally a `Prop`-valued definition for T11; T12 supplies the proof.
-/
def F3RunComposition
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  AcceptedModel image transition →
    CompleteActualServiceStartDiscipline transition →
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
actual `TxReady` service start, whose committed packet has exactly one completion and one arrival
child and cannot be duplicated, partially erased, or replaced before matching completion. The
concrete eager-selection, private-payload, erasure, and multiplicity witnesses isolate why each
clause is required. The Rust decision and completion handlers are
`executor/src/scalar.rs:863-940,1091-1200`.

The executable witness data is already constructed in T11; its Lean proof is deferred to T12.
-/
def F4DecisionPointScope
    (image : SimulationImage State)
    (transition : TransitionRelation State) : Prop :=
  (CompleteActualServiceStartDiscipline transition →
      F2RoundSerializability image transition ∧
      F3RunComposition image transition) ∧
    ReachableEagerSelectionCountermodel ∧
    PrivatePayloadSmugglerCountermodel ∧
    CommittedServiceErasurePreemptorCountermodel ∧
    CommittedServiceMultiplicityCounterexamples

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
The conservative one-conflict-class ruling still licenses same-kind clustering across distinct
LPs when there is no packet-causal edge. Within one LP it deliberately retains canonical order.
-/
def CrossLPSameKindClusteringLicensed : Prop :=
  ∀ emissions left right,
    left.kind = right.kind →
    left.target ≠ right.target →
    ¬ PacketCausalBefore emissions left right →
    ¬ PacketCausalBefore emissions right left →
    IntraRoundIndependent emissions left right

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
same complete normalized result, and the statement itself constructs an execution for every
permitted order. It also retains the concrete unsound reversal shape forced by the finite-capacity
handlers at
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
        canonicalOrder canonicalFinish candidateOrder roundEmissions,
      SafeHorizonRound image transition bounds cut
        start drainedEvents roundFinish →
      CanonicalSerialRestricted image transition cut
        start.machine canonicalOrder canonicalFinish →
      RoundEmissionDelta start.machine canonicalFinish roundEmissions →
      IndependentStepsCommute image transition roundEmissions →
      (∀ event, event ∈ canonicalOrder ↔ event ∈ drainedEvents) →
      PreservesRequiredIntraRoundOrder roundEmissions
        drainedEvents candidateOrder →
      ∃ candidateFinish,
        ExecutionInOrder image transition
          start.machine candidateOrder candidateFinish ∧
        SameMachineResult image canonicalFinish candidateFinish) ∧
    CrossLPSameKindClusteringLicensed ∧
    UnsoundReorderingCounterexampleShape

end DaysExecutor
