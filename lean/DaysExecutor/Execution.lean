import DaysExecutor.Transition

namespace DaysExecutor

/--
Semantic machine configuration shared by serial and round execution, corresponding to the complete
state and pending-event result assembled at `executor/src/scalar.rs:575-602`.

`emissions` is a proof-only ghost trace of parent/child edges: it is absent from Rust's runtime
result and deliberately excluded from `SameMachineResult`, so the model adds no proof-carrying
runtime machinery.
-/
structure MachineState (State : StateFamily) where
  localState : (node : NodeDescriptor) → State node.kind
  pending : List Event
  observations : List Observation
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
Lexicographic observation order used to normalize per-LP output, mirroring the key sort at
`executor/src/scalar.rs:583-584`.
-/
def observationLE (a b : Observation) : Prop :=
  a.eventKey < b.eventKey ∨
    (a.eventKey = b.eventKey ∧
      (a.ordinal < b.ordinal ∨
        (a.ordinal = b.ordinal ∧
          (a.tag < b.tag ∨ (a.tag = b.tag ∧ a.value ≤ b.value)))))

/-- Decidability for normalized scalar observation ordering at `executor/src/scalar.rs:583-584`. -/
instance (a b : Observation) : Decidable (observationLE a b) := by
  unfold observationLE
  infer_instance

/--
Canonical insertion of one observation into the normalized result order at
`executor/src/scalar.rs:583-600`.
-/
def insertObservation (observation : Observation) : List Observation → List Observation
  | [] => [observation]
  | head :: tail =>
      if observationLE observation head then
        observation :: head :: tail
      else
        head :: insertObservation observation tail

/--
Canonical insertion of handler observations, making LP scheduling order non-semantic as in
`executor/src/scalar.rs:583-600`.
-/
def insertObservations
    (generated current : List Observation) : List Observation :=
  generated.foldl (fun ordered item => insertObservation item ordered) current

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
Only the target LP's state changes and emitted observations enter canonical order, mirroring
exclusive state-slot ownership plus normalized finish at
`executor/src/scalar.rs:575-601,638-663`.
-/
def AppliesStateAndObservations
    (image : SimulationImage State)
    (node : NodeDescriptor)
    (result : TransitionResult State node.kind)
    (before after : MachineState State) : Prop :=
  after.localState node = result.nextState ∧
    (∀ other ∈ image.nodes,
      other.id ≠ node.id →
      after.localState other = before.localState other) ∧
    after.observations = insertObservations result.observations before.observations

/--
One arbitrary available-event step used to define an explicit candidate reordering; unlike the
canonical scalar loop, it does not itself impose least-key choice
(`executor/src/scalar.rs:344-356`).
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
      AppliesStateAndObservations image node result before after ∧
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
Initial machine relation resolving Rust's role arena plus `state_slot` representation at
`executor/src/image.rs:247-260` into the semantic node-indexed view used by the proof.
-/
def InitialMachine
    (image : SimulationImage State)
    (machine : MachineState State) : Prop :=
  machine.pending = canonicalizeEvents image.initialEvents ∧
    machine.observations = [] ∧
    machine.emissions = [] ∧
    ∀ node ∈ image.nodes, stateAt? image node = some (machine.localState node)

/--
Complete normalized-result equality used by all cross-executor claims, matching Rust's comparison
surface at `executor/src/scalar.rs:575-602`.
-/
def SameMachineResult
    (image : SimulationImage State)
    (left right : MachineState State) : Prop :=
  (∀ node ∈ image.nodes, left.localState node = right.localState node) ∧
    left.pending = right.pending ∧
    left.observations = right.observations

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
Consistent cut: a reachable per-LP key prefix closed under causal/emission order, specifying the
semantic set drained by `executor/src/safe_horizon.rs:268-336`.
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
The concrete drained list represents exactly the reachable events below their target LP's bound
and forms a consistent cut, generalizing `executor/src/safe_horizon.rs:381-448`.
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
