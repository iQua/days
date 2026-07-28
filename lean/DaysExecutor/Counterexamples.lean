import DaysExecutor.SafeHorizon

namespace DaysExecutor

/--
Concrete three-LP relay fixture for the permanently forbidden direct-predecessor bound from plan
§4.2, exercising the channel abstraction implemented at `executor/src/image.rs:209-245`.
-/
structure RelayCounterexample where
  m : NodeId
  i : NodeId
  j : NodeId
  channels : List RemoteChannel
  frontier : NodeId → ExtendedTime
  jPendingTime : Nat
  relayedArrivalTime : Nat

/--
Extended-time addition used by the one-hop relay policy rejected in plan §4.2; `none` corresponds
to the empty frontier read at `executor/src/safe_horizon.rs:248-264`.
-/
def extendedAdd (time : ExtendedTime) (delay : Nat) : ExtendedTime :=
  time.map fun finite => finite + delay

/--
Unsound direct-predecessor candidate `min_i (N_i + L_i→j)` deliberately defined for the relay
regression; it is not the V1 policy in `executor/src/safe_horizon.rs:259-264`.
-/
def oneHopBound
    (channels : List RemoteChannel)
    (frontier : NodeId → ExtendedTime)
    (target : NodeId) : ExtendedTime :=
  channels.foldl
    (fun current channel =>
      if channel.target = target then
        extendedMin current (extendedAdd (frontier channel.source) channel.minDelayNs)
      else
        current)
    none

/--
Concrete `m → i → j` unit-delay relay with frontiers `5, ∞, 100`, exactly the permanent
counterexample in plan §4.2 to any one-hop replacement for
`executor/src/safe_horizon.rs:259-264`.
-/
def relayCounterexample : RelayCounterexample :=
  { m := 0
    i := 1
    j := 2
    channels :=
      [ { source := 0, target := 1, link := 0
          eventKind := .remoteArrival, minDelayNs := 1 },
        { source := 1, target := 2, link := 1
          eventKind := .remoteArrival, minDelayNs := 1 } ]
    frontier := fun node =>
      if node = 0 then some 5
      else if node = 1 then none
      else if node = 2 then some 100
      else none
    jPendingTime := 100
    relayedArrivalTime := 7 }

/--
Executable relay regression check for the forbidden one-hop replacement of
`executor/src/safe_horizon.rs:259-264`.
-/
def relayOneHopBoundUnsoundCheck : Bool :=
  let fixture := relayCounterexample
  decide (
    oneHopBound fixture.channels fixture.frontier fixture.j = none ∧
    fixture.relayedArrivalTime = 7 ∧
    fixture.relayedArrivalTime < fixture.jPendingTime)

/--
Permanent relay regression obligation: the direct-predecessor bound for `j` is infinity while the
two-hop event arrives at `7`, before `j`'s pending event at `100`. T12 proves this concrete
proposition; T11 only states it, as required for the forbidden refinement of
`executor/src/safe_horizon.rs:259-264`.
-/
def RelayOneHopBoundUnsound : Prop :=
  relayOneHopBoundUnsoundCheck = true

/--
Event exactly at a round horizon, used by the half-open boundary regression corresponding to
`executor/src/safe_horizon.rs:638-667`.
-/
def eventAtHorizon (horizon : Nat) : Event :=
  { key :=
      { timeNs := horizon
        phase := eventPhase .remoteArrival
        originNode := 0
        originSeq := 0 }
    target := 1
    kind := .remoteArrival
    payload := 0 }

/--
Executable half-open horizon check corresponding to the boundary test at
`executor/src/safe_horizon.rs:638-667`.
-/
def halfOpenBoundaryCheck (horizon : Nat) : Bool :=
  decide (¬ belowBound (fun _ => horizon) (eventAtHorizon horizon))

/--
Half-open horizon witness statement: an event at `H` waits, unlike an event at the inclusive
scenario stop accepted by `executor/src/scalar.rs:344-347`.
-/
def HalfOpenBoundaryWitness (horizon : Nat) : Prop :=
  halfOpenBoundaryCheck horizon = true

/--
Tiny finite-capacity FIFO state capturing the queue-sensitive Rust paths at
`executor/src/scalar.rs:975-1016,1091-1128`.
-/
structure TinyQueueState where
  capacity : Nat
  waiting : List PayloadId
  inService : Option PayloadId
  accepted : List PayloadId
  dropped : List PayloadId
  deriving DecidableEq, Repr

/--
Tiny queue events needed to execute the eager-selection and unsound-reordering shapes from
`executor/src/scalar.rs:975-1016,1091-1128`.
-/
inductive TinyQueueEvent where
  | remoteArrival (packet : PayloadId)
  | txReady
  deriving DecidableEq, Repr

/--
Executable finite-capacity FIFO step matching the relevant admission and readiness effects at
`executor/src/scalar.rs:999-1015,1115-1128`.
-/
def tinyQueueStep (state : TinyQueueState) : TinyQueueEvent → TinyQueueState
  | .remoteArrival packet =>
      if state.capacity ≠ 0 ∧ state.capacity ≤ state.waiting.length then
        { state with dropped := state.dropped ++ [packet] }
      else
        { state with
          waiting := state.waiting ++ [packet]
          accepted := state.accepted ++ [packet] }
  | .txReady =>
      match state.inService, state.waiting with
      | none, packet :: rest =>
          { state with waiting := rest, inService := some packet }
      | _, _ => state

/--
Executable tiny-queue run used only for permanent counterexample data, corresponding to the two
Rust handler calls at `executor/src/scalar.rs:975-1016,1091-1128`.
-/
def runTinyQueue
    (initial : TinyQueueState)
    (events : List TinyQueueEvent) : TinyQueueState :=
  events.foldl tinyQueueStep initial

/--
Capacity-one initial queue used by both counterexample shapes, mirroring a full TailDrop queue just
before `executor/src/scalar.rs:999-1015`.
-/
def queueCounterexampleInitial : TinyQueueState :=
  { capacity := 1
    waiting := [10]
    inService := none
    accepted := []
    dropped := [] }

/--
Canonical Rust order: the earlier arrival observes a full queue before the later service-start
decision at `executor/src/event.rs:78-88`.
-/
def queueCounterexampleCanonicalOrder : List TinyQueueEvent :=
  [.remoteArrival 20, .txReady]

/--
Unsound order: an eager or otherwise reordered service choice frees the slot before the arrival,
contrary to `executor/src/scalar.rs:975-1016,1091-1128`.
-/
def queueCounterexampleUnsoundOrder : List TinyQueueEvent :=
  [.txReady, .remoteArrival 20]

/--
Concrete event metadata placing the queue arrival before the actual service-start decision, as
required by `executor/src/event.rs:78-88` and `executor/src/scalar.rs:1091-1128`.
-/
def queueCounterexampleArrival : Event :=
  { key :=
      { timeNs := 5, phase := eventPhase .remoteArrival
        originNode := 0, originSeq := 0 }
    target := 1
    kind := .remoteArrival
    payload := 20 }

/--
Concrete later `TxReady` decision point from the eager-selection counterexample, matching
`executor/src/scalar.rs:1091-1128`.
-/
def queueCounterexampleReady : Event :=
  { key :=
      { timeNs := 10, phase := eventPhase .txReady
        originNode := 1, originSeq := 0 }
    target := 1
    kind := .txReady
    payload := 10 }

/--
Post-exchange start queue for the eager-selection fixture: the remote arrival is already merged
before the round, matching the premise enforced around
`executor/src/safe_horizon.rs:309-336`.
-/
def queueCounterexampleStartPending : List Event :=
  [queueCounterexampleArrival, queueCounterexampleReady]

/--
The eager-selection fixture has no unseen remote event; its only remote arrival is already in the
post-exchange start queue, matching the validity premise behind
`executor/src/safe_horizon.rs:322-327`.
-/
def queueCounterexampleUnseenRemote : List Event :=
  []

/--
Executable finite-list form of the no-unseen-event-below-bound validity condition checked at
`executor/src/safe_horizon.rs:322-327`.
-/
def listedUnseenRemoteValidCheck
    (unseen : List Event)
    (bounds : NodeId → Nat) : Bool :=
  unseen.all fun event => decide (¬ belowBound bounds event)

/--
Executable sound-horizon check for the eager-selection fixture: both relevant events are already
merged below `H = 11` and no unseen remote event exists, so any divergence comes from choosing
early rather than from `executor/src/safe_horizon.rs:309-336`.
-/
def queueCounterexampleHorizonSoundCheck : Bool :=
  decide (
      queueCounterexampleArrival ∈ queueCounterexampleStartPending ∧
      queueCounterexampleReady ∈ queueCounterexampleStartPending ∧
      belowBound (fun _ => 11) queueCounterexampleArrival ∧
      belowBound (fun _ => 11) queueCounterexampleReady) &&
    listedUnseenRemoteValidCheck
      queueCounterexampleUnseenRemote
      (fun _ => 11)

/--
Executable eager-selection divergence check for the service-start scope enforced at
`executor/src/scalar.rs:975-1016,1091-1128`.
-/
def eagerSelectionCounterexampleCheck : Bool :=
  let initial := queueCounterexampleInitial
  queueCounterexampleHorizonSoundCheck &&
    decide (
      queueCounterexampleArrival.key < queueCounterexampleReady.key ∧
      runTinyQueue initial queueCounterexampleCanonicalOrder ≠
        runTinyQueue initial queueCounterexampleUnsoundOrder)

/--
Executable eager-selection counterexample shape: both events lie inside an otherwise sound
half-open horizon, yet reserving the waiting packet before the actual `TxReady` changes acceptance
and final state. This witnesses the scope boundary at
`executor/src/scalar.rs:975-1016,1091-1128`.
-/
def EagerSelectionCounterexampleShape : Prop :=
  eagerSelectionCounterexampleCheck = true

/--
Executable check for the unsound intra-round reversal of the finite-capacity handlers at
`executor/src/scalar.rs:975-1016,1091-1128`.
-/
def unsoundReorderingCounterexampleCheck : Bool :=
  decide (
    runTinyQueue queueCounterexampleInitial queueCounterexampleCanonicalOrder ≠
      runTinyQueue queueCounterexampleInitial queueCounterexampleUnsoundOrder)

/--
Unsound intra-round reordering shape: reversing a capacity-sensitive arrival and service decision
changes state and observations. It violates the conservative same-queue conflict order required by
the accepted handlers at `executor/src/scalar.rs:975-1016,1091-1128`.
-/
def UnsoundReorderingCounterexampleShape : Prop :=
  unsoundReorderingCounterexampleCheck = true

end DaysExecutor
