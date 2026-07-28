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
  mToILink : LinkDescriptor
  iToJLink : LinkDescriptor
  mToI : RemoteChannel
  iToJ : RemoteChannel
  frontier : NodeId → ExtendedTime
  rootTime : Nat
  payloadBytes : Nat
  jPendingTime : Nat

/-- The two declared channels in causal path order. -/
def RelayCounterexample.channels (fixture : RelayCounterexample) : List RemoteChannel :=
  [fixture.mToI, fixture.iToJ]

/-- The two physical directed links in relay path order. -/
def RelayCounterexample.links (fixture : RelayCounterexample) : List LinkDescriptor :=
  [fixture.mToILink, fixture.iToJLink]

/--
Arrival time obtained by traversing both certified channel delays. This is deliberately computed,
not fixture data: Rust computes each next-hop arrival from the current event time at
`executor/src/scalar.rs:1091-1166` using `executor/src/time.rs:54-66`.
-/
def RelayCounterexample.relayedArrivalTime (fixture : RelayCounterexample) : Nat :=
  linkArrivalTimeNs fixture.iToJLink
    (linkArrivalTimeNs fixture.mToILink fixture.rootTime fixture.payloadBytes)
    fixture.payloadBytes

/-- Root service event that launches the first remote hop. -/
def RelayCounterexample.rootReady (fixture : RelayCounterexample) : Event :=
  { key :=
      { timeNs := fixture.rootTime
        phase := eventPhase .txReady
        originNode := fixture.m
        originSeq := 0 }
    target := fixture.m
    kind := .txReady
    payload := 0 }

/-- First-hop remote arrival at the relay LP. -/
def RelayCounterexample.atRelay (fixture : RelayCounterexample) : Event :=
  { key :=
      { timeNs :=
          linkArrivalTimeNs fixture.mToILink fixture.rootTime fixture.payloadBytes
        phase := eventPhase .remoteArrival
        originNode := fixture.m
        originSeq := 1 }
    target := fixture.i
    kind := .remoteArrival
    payload := 0 }

/-- Same-time service decision scheduled by the relay arrival. -/
def RelayCounterexample.relayReady (fixture : RelayCounterexample) : Event :=
  { key :=
      { timeNs := fixture.atRelay.key.timeNs
        phase := eventPhase .txReady
        originNode := fixture.i
        originSeq := 0 }
    target := fixture.i
    kind := .txReady
    payload := 0 }

/-- Second-hop remote arrival at the destination LP. -/
def RelayCounterexample.atDestination (fixture : RelayCounterexample) : Event :=
  { key :=
      { timeNs := fixture.relayedArrivalTime
        phase := eventPhase .remoteArrival
        originNode := fixture.i
        originSeq := 1 }
    target := fixture.j
    kind := .remoteArrival
    payload := 0 }

/-- One service-to-remote-arrival causal hop along a certified channel. -/
def RelayChannelHop
    (channel : RemoteChannel)
    (link : LinkDescriptor)
    (payloadBytes : Nat)
    (service arrival : Event) : Prop :=
  channel.link = link.id ∧
    channel.source = link.source ∧
    channel.target = link.physicalTarget ∧
    channel.minDelayNs ≤ linkDelayNs link payloadBytes ∧
    service.target = channel.source ∧
    service.kind = .txReady ∧
    arrival.target = channel.target ∧
    arrival.kind = .remoteArrival ∧
    arrival.payload = service.payload ∧
    arrival.key.timeNs =
      linkArrivalTimeNs link service.key.timeNs payloadBytes

/-- Local causal edge by which a remote arrival schedules the relay's same-time service decision. -/
def RelayArrivalSchedulesReady
    (arrival ready : Event) : Prop :=
  arrival.target = ready.target ∧
    arrival.payload = ready.payload ∧
    arrival.kind = .remoteArrival ∧
    ready.kind = .txReady ∧
    arrival.key.timeNs = ready.key.timeNs ∧
    arrival.key < ready.key

/--
Arithmetic and causal shape of an `m → i → j` relay. The endpoint equalities and recurrence make
the final arrival a consequence of the two channel edges rather than an unrelated timestamp.
-/
def TwoHopRelayPath (fixture : RelayCounterexample) : Prop :=
  fixture.frontier fixture.m = some fixture.rootTime ∧
    fixture.mToI ∈ fixture.channels ∧
    fixture.iToJ ∈ fixture.channels ∧
    fixture.mToILink ∈ fixture.links ∧
    fixture.iToJLink ∈ fixture.links ∧
    fixture.mToI.source = fixture.m ∧
    fixture.mToI.target = fixture.i ∧
    fixture.iToJ.source = fixture.i ∧
    fixture.iToJ.target = fixture.j ∧
    fixture.mToI.eventKind = .remoteArrival ∧
    fixture.iToJ.eventKind = .remoteArrival ∧
    linkDelayNs fixture.mToILink fixture.payloadBytes = 1 ∧
    linkDelayNs fixture.iToJLink fixture.payloadBytes = 1 ∧
    RelayChannelHop fixture.mToI fixture.mToILink fixture.payloadBytes
      fixture.rootReady fixture.atRelay ∧
    RelayArrivalSchedulesReady fixture.atRelay fixture.relayReady ∧
    RelayChannelHop fixture.iToJ fixture.iToJLink fixture.payloadBytes
      fixture.relayReady fixture.atDestination ∧
    fixture.atDestination.key.timeNs = fixture.relayedArrivalTime ∧
    fixture.relayedArrivalTime =
      (fixture.rootTime + 1) + 1

/-- The finite relay fixture's causal path predicate is executable. -/
instance (fixture : RelayCounterexample) : Decidable (TwoHopRelayPath fixture) := by
  unfold TwoHopRelayPath RelayChannelHop RelayArrivalSchedulesReady
    RelayCounterexample.channels RelayCounterexample.relayedArrivalTime
    RelayCounterexample.links
    RelayCounterexample.rootReady RelayCounterexample.atRelay
    RelayCounterexample.relayReady RelayCounterexample.atDestination
  infer_instance

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
    mToILink :=
      { id := 0, source := 0, physicalTarget := 1
        rateBps := 8_000_000_000, propagationNs := 0 }
    iToJLink :=
      { id := 1, source := 1, physicalTarget := 2
        rateBps := 8_000_000_000, propagationNs := 0 }
    mToI :=
      { source := 0, target := 1, link := 0
        eventKind := .remoteArrival, minDelayNs := 1 }
    iToJ :=
      { source := 1, target := 2, link := 1
        eventKind := .remoteArrival, minDelayNs := 1 }
    frontier := fun node =>
      if node = 0 then some 5
      else if node = 1 then none
      else if node = 2 then some 100
      else none
    rootTime := 5
    payloadBytes := 1
    jPendingTime := 100
  }

/--
Executable relay regression check for the forbidden one-hop replacement of
`executor/src/safe_horizon.rs:259-264`.
-/
def relayOneHopBoundUnsoundCheck : Bool :=
  let fixture := relayCounterexample
  decide (
    oneHopBound fixture.channels fixture.frontier fixture.j = none ∧
    TwoHopRelayPath fixture ∧
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
Executable inclusive-stop translation check. An event at configured stop is accepted by the
scalar endpoint and lies below `stopExclusive stop`, while the exclusive endpoint itself waits.
-/
def inclusiveStopTranslationCheck (stopTimeNs : Nat) : Bool :=
  decide (
    withinInclusiveStop stopTimeNs (eventAtHorizon stopTimeNs) ∧
    belowBound (fun _ => stopExclusive stopTimeNs) (eventAtHorizon stopTimeNs) ∧
    ¬ belowBound
      (fun _ => stopExclusive stopTimeNs)
      (eventAtHorizon (stopExclusive stopTimeNs)))

/-- Largest Rust `u64` timestamp. -/
def u64MaxTimeNs : Nat :=
  18_446_744_073_709_551_615

/--
Executable maximal-stop translation check for Rust's `u128(stop_time_ns) + 1` boundary at
`executor/src/safe_horizon.rs:14,231-236`.
-/
def maximumStopTranslationCheck : Bool :=
  decide (
    stopExclusive u64MaxTimeNs = (2 : Nat) ^ 64 ∧
    withinInclusiveStop u64MaxTimeNs (eventAtHorizon u64MaxTimeNs) ∧
    belowBound
      (fun _ => stopExclusive u64MaxTimeNs)
      (eventAtHorizon u64MaxTimeNs))

/--
Complete boundary witness: an event at a round `H` waits, the configured scenario stop remains
inclusive, and `u64::MAX` translates exactly to the representable `2^64` exclusive boundary.
-/
def HalfOpenBoundaryWitness (horizon : Nat) : Prop :=
  halfOpenBoundaryCheck horizon = true ∧
    inclusiveStopTranslationCheck horizon = true ∧
    maximumStopTranslationCheck = true

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

/-- Unused source-host state in the concrete heterogeneous countermodel. -/
def queueCounterexampleSourceState : TinyQueueState :=
  { capacity := 0
    waiting := []
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
Execute the same chronological event list under the forbidden eager policy: a remote arrival first
performs the future `TxReady` selection, then applies its own arrival effect.
-/
def runTinyQueueEagerPolicy
    (initial : TinyQueueState)
    (events : List TinyQueueEvent) : TinyQueueState :=
  events.foldl
    (fun state event =>
      match event with
      | .remoteArrival packet =>
          tinyQueueStep (tinyQueueStep state .txReady) (.remoteArrival packet)
      | .txReady => tinyQueueStep state .txReady)
    initial

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
        runTinyQueueEagerPolicy initial queueCounterexampleCanonicalOrder)

/--
Executable eager-selection counterexample shape: both events lie inside an otherwise sound
half-open horizon, yet reserving the waiting packet before the actual `TxReady` changes acceptance
and final state. This witnesses the scope boundary at
`executor/src/scalar.rs:975-1016,1091-1128`.
-/
def EagerSelectionCounterexampleShape : Prop :=
  eagerSelectionCounterexampleCheck = true

/-- Constant role-indexed state family for the concrete modeled queue counterexample. -/
abbrev TinyQueueStateFamily : StateFamily :=
  fun _ => TinyQueueState

/-- Immutable descriptor data for the two concrete packet IDs. -/
def queueCounterexampleDescriptor (payload : PayloadId) : PacketDescriptor :=
  { id := payload
    flow := 0
    sizeBytes := 1
    kind := .data }

/--
Concrete heterogeneous image whose canonical initial queue is exactly the two counterexample
events. The source host supplies the remote event's origin; the switch owns the finite-capacity
queue under test.
-/
def queueCounterexampleImage : SimulationImage TinyQueueStateFamily :=
  { stopTimeNs := 10
    nodes :=
      [ { id := 0, kind := .host, stateSlot := 0 },
        { id := 1, kind := .switch, stateSlot := 0 } ]
    stateArena := fun kind =>
      match kind with
      | .host =>
          [ { privateState := queueCounterexampleSourceState
              committedService := [] } ]
      | .switch =>
          [ { privateState := queueCounterexampleInitial
              committedService := [] } ]
    links := []
    channels := []
    initialEvents := queueCounterexampleStartPending
    packetDescriptor := queueCounterexampleDescriptor
    initialPacketStore := fun node =>
      if node = 1 then
        [queueCounterexampleDescriptor 10, queueCounterexampleDescriptor 20]
      else
        []
    initialNextOriginSeq := fun _ => 1
    payloadBytes := fun _ => 1 }

/-- Role-correct initial state selected through the concrete image's state arenas. -/
def queueCounterexampleLocalState
    (node : NodeDescriptor) : RoleState TinyQueueStateFamily node.kind :=
  match node.kind with
  | .host =>
      { privateState := queueCounterexampleSourceState
        committedService := [] }
  | .switch =>
      { privateState := queueCounterexampleInitial
        committedService := [] }

/-- The one service decision introduced by a successful tiny `TxReady`, if any. -/
def tinyQueueDecisions
    (node : NodeDescriptor)
    (event : Event)
    (before after : TinyQueueState) : List ServiceDecision :=
  match before.inService, after.inService with
  | none, some packet =>
      [ { node := node.id
          decisionKey := event.key
          packet
          committedNonPreemptively := true } ]
  | _, _ => []

/-- Canonical or eager next-state policy for the concrete accepted transition relation. -/
def tinyQueueNextState
    (eager : Bool)
    (state : TinyQueueState)
    (event : Event) : TinyQueueState :=
  match event.kind with
  | .remoteArrival =>
      if eager then
        tinyQueueStep (tinyQueueStep state .txReady) (.remoteArrival event.payload)
      else
        tinyQueueStep state (.remoteArrival event.payload)
  | .txReady => tinyQueueStep state .txReady
  | .packetArrival | .txComplete => state

/-- Complete transition result for the concrete canonical/eager policies. -/
def tinyQueueTransitionResult
    (eager : Bool)
    (node : NodeDescriptor)
    (event : Event)
    (state : RoleState TinyQueueStateFamily node.kind) :
    TransitionResult TinyQueueStateFamily node.kind :=
  let nextPrivate := tinyQueueNextState eager state.privateState event
  let next : RoleState TinyQueueStateFamily node.kind :=
    { privateState := nextPrivate
      committedService := nextPrivate.inService.toList }
  { nextState := next
    children := []
    packetInstalls := []
    packetRemovals := []
    summaryDelta := RunSummary.zero
    observedPackets := []
    departures := []
    arrivals := []
    decisions :=
      if event.kind = .txReady then
        tinyQueueDecisions node event state.privateState nextPrivate
      else
        [] }

/--
Concrete successful transition relation. Both policies are deterministic and enabled for every
role-supported reachable configuration; they differ only in whether arrival eagerly performs the
future service selection.
-/
def tinyQueueTransition (eager : Bool) : TransitionRelation TinyQueueStateFamily :=
  fun node event state result =>
    state.committedService = state.privateState.inService.toList ∧
      event.target = node.id ∧
      roleSupports node.kind event.kind ∧
      result = tinyQueueTransitionResult eager node event state

/-- Concrete initial machine for both modeled executions. -/
def queueCounterexampleMachine : MachineState TinyQueueStateFamily :=
  { localState := queueCounterexampleLocalState
    packetStore := initialPacketStore queueCounterexampleImage
    pending := canonicalizeEvents queueCounterexampleImage.initialEvents
    summary := RunSummary.zero
    observedPackets := []
    departures := []
    arrivals := []
    nextOriginSeq := queueCounterexampleImage.initialNextOriginSeq
    allocatedKeys := queueCounterexampleImage.initialEvents.map Event.key
    emissions := [] }

/--
Reachable semantic countermodel for F4. The same accepted image and canonical event order execute
under deterministic canonical and eager policies. The horizon is valid, yet results differ; the
eager policy satisfies the old one-way metadata check but cannot satisfy decision completeness.

This is a `Prop`-valued T11 statement over concrete executable data. T12 proves the proposition
alongside F4.
-/
def ReachableEagerSelectionCountermodel : Prop :=
  EagerSelectionCounterexampleShape ∧
    AcceptedModel queueCounterexampleImage (tinyQueueTransition false) ∧
    AcceptedModel queueCounterexampleImage (tinyQueueTransition true) ∧
    InitialMachine queueCounterexampleImage queueCounterexampleMachine ∧
    ConstantGlobalBoundsValid
      queueCounterexampleImage
      (tinyQueueTransition true)
      queueCounterexampleMachine ∧
    CompleteActualServiceStartDiscipline (tinyQueueTransition false) ∧
    ActualServiceStartDiscipline (tinyQueueTransition true) ∧
    ¬ CompleteActualServiceStartDiscipline (tinyQueueTransition true) ∧
    ∃ canonicalFinish eagerFinish,
      ExecutionInOrder
        queueCounterexampleImage
        (tinyQueueTransition false)
        queueCounterexampleMachine
        [queueCounterexampleArrival, queueCounterexampleReady]
        canonicalFinish ∧
      ExecutionInOrder
        queueCounterexampleImage
        (tinyQueueTransition true)
        queueCounterexampleMachine
        [queueCounterexampleArrival, queueCounterexampleReady]
        eagerFinish ∧
      ¬ SameMachineResult queueCounterexampleImage canonicalFinish eagerFinish

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
