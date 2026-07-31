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

/--
Earliest remote child certified by one declared channel. Using the channel bound here is exact for
this finite witness because `RelayChannelMatchesLink` equates it with the physical link delay
derived at `executor/src/validate.rs:698-755` and checked against the channel declaration at lines
1053-1068.
-/
def RemoteChannel.boundArrival
    (channel : RemoteChannel)
    (service : Event)
    (originSeq : Nat) : Event :=
  { key :=
      { timeNs := service.key.timeNs + channel.minDelayNs
        phase := eventPhase channel.eventKind
        originNode := channel.source
        originSeq }
    target := channel.target
    kind := channel.eventKind
    payload := service.payload }

/-- First-hop remote arrival, constructed directly from the first declared channel. -/
def RelayCounterexample.atRelay (fixture : RelayCounterexample) : Event :=
  fixture.mToI.boundArrival fixture.rootReady 1

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

/-- Second-hop remote arrival, constructed directly from the second declared channel. -/
def RelayCounterexample.atDestination (fixture : RelayCounterexample) : Event :=
  fixture.iToJ.boundArrival fixture.relayReady 1

/--
Final arrival time obtained by traversing both declared channel bounds. The two remote event
constructors, rather than unrelated fixture timestamps, determine this value.
-/
def RelayCounterexample.relayedArrivalTime (fixture : RelayCounterexample) : Nat :=
  fixture.atDestination.key.timeNs

/--
Static certificate making a channel-bound arrival exact for this witness: the declared lower bound
equals the concrete serialization-plus-propagation delay used at
`executor/src/scalar.rs:893-920,1139-1165`.
-/
def RelayChannelMatchesLink
    (channel : RemoteChannel)
    (link : LinkDescriptor)
    (payloadBytes : Nat) : Prop :=
  channel.link = link.id ∧
    channel.source = link.source ∧
    channel.target = link.physicalTarget ∧
    channel.eventKind = .remoteArrival ∧
    channel.minDelayNs = linkDelayNs link payloadBytes

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
Minimum executable channel-bound shape of an `m → i → j` relay. This fixture does not claim a full
`AcceptedModel` trace: instead, each remote event is constructed from its parent and declared
channel, and the exact link certificates prevent an unrelated timestamp from satisfying the check.
-/
def TwoHopRelayPath (fixture : RelayCounterexample) : Prop :=
  fixture.frontier fixture.m = some fixture.rootTime ∧
    fixture.frontier fixture.j = some fixture.jPendingTime ∧
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
    fixture.mToI.minDelayNs = 1 ∧
    fixture.iToJ.minDelayNs = 1 ∧
    RelayChannelMatchesLink fixture.mToI fixture.mToILink fixture.payloadBytes ∧
    RelayArrivalSchedulesReady fixture.atRelay fixture.relayReady ∧
    RelayChannelMatchesLink fixture.iToJ fixture.iToJLink fixture.payloadBytes ∧
    fixture.atDestination.key.timeNs = fixture.relayedArrivalTime ∧
    fixture.relayedArrivalTime =
      (fixture.rootTime + 1) + 1

/-- The finite relay fixture's causal path predicate is executable. -/
instance (fixture : RelayCounterexample) : Decidable (TwoHopRelayPath fixture) := by
  unfold TwoHopRelayPath RelayChannelMatchesLink RelayArrivalSchedulesReady
    RelayCounterexample.channels RelayCounterexample.relayedArrivalTime
    RelayCounterexample.links
    RelayCounterexample.atDestination RelayCounterexample.relayReady
    RelayCounterexample.atRelay RelayCounterexample.rootReady
    RemoteChannel.boundArrival
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
Private bookkeeping for the accepted-model fixture. The authoritative waiting queue and committed
service slot live in `RoleState`; `hiddenReservation` exists only to exercise the forbidden eager
private-state smuggling policy.
-/
structure TinyQueuePrivateState where
  capacity : Nat
  hiddenReservation : Option PayloadId
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
    waiting := [12]
    inService := none
    accepted := []
    dropped := [] }

/-- Unused source-host bookkeeping in the concrete heterogeneous countermodel. -/
def queueCounterexampleSourceState : TinyQueuePrivateState :=
  { capacity := 0
    hiddenReservation := none
    accepted := []
    dropped := [] }

/-- Initial switch bookkeeping; its authoritative FIFO is exposed separately in `RoleState`. -/
def queueCounterexamplePrivateState : TinyQueuePrivateState :=
  { capacity := 1
    hiddenReservation := none
    accepted := []
    dropped := [] }

/--
Canonical Rust order: the earlier arrival observes a full queue before the later service-start
decision at `executor/src/event.rs:78-88`.
-/
def queueCounterexampleCanonicalOrder : List TinyQueueEvent :=
  [.remoteArrival 21, .txReady]

/--
Unsound order: an eager or otherwise reordered service choice frees the slot before the arrival,
contrary to `executor/src/scalar.rs:975-1016,1091-1128`.
-/
def queueCounterexampleUnsoundOrder : List TinyQueueEvent :=
  [.txReady, .remoteArrival 21]

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
    payload := 21 }

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
    payload := 12 }

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
  fun _ => TinyQueuePrivateState

/-- Immutable descriptor data for the two concrete packet IDs. -/
def queueCounterexampleDescriptor (payload : PayloadId) : PacketDescriptor :=
  { id := payload
    flow := 0
    sizeBytes := 1
    kind := .data }

/--
Positive owned fixture entry whose aggregate compatibility count is derived from its owner list.
-/
def queueCounterexampleEntry
    (payload : PayloadId)
    (references : Nat := 1) : PacketStoreEntry :=
  { descriptor := queueCounterexampleDescriptor payload
    owners :=
      (List.range references).map fun index =>
        .pendingEvent
          { timeNs := payload
            phase := 0
            originNode := 0
            originSeq := index } }

/-- Owned-store fixture entry with explicitly supplied provenance. -/
def ownedReferenceFixtureEntry
    (payload : PayloadId)
    (owners : List ReferenceOwner) : PacketStoreEntry :=
  { descriptor := queueCounterexampleDescriptor payload, owners }

/--
Concrete heterogeneous image whose canonical initial queue is exactly the two counterexample
events. The source host supplies the remote event's origin; the switch owns the finite-capacity
queue under test, and the terminal host completes the switch's declared route. The incoming
channel satisfies initial `RemoteArrival` route validation at `executor/src/validate.rs:1203-1215`;
the outgoing switch link satisfies initial `TxReady` ownership validation at lines 1179-1200.
Packet IDs 12 and 21 belong to source LP 0 under the three-node allocation rule at lines
1956-1971, and the unused third link lets terminal host 2 name a source-owned egress as required at
lines 791-807.
-/
def queueCounterexampleImage : SimulationImage TinyQueueStateFamily :=
  { stopTimeNs := 11
    nodes :=
      [ { id := 0, kind := .host, stateSlot := 0 },
        { id := 1, kind := .switch, stateSlot := 0 },
        { id := 2, kind := .host, stateSlot := 1 } ]
    stateArena := fun kind =>
      match kind with
      | .host =>
          [ { privateState := queueCounterexampleSourceState
              serviceQueue := []
              committedService := [] },
            { privateState := queueCounterexampleSourceState
              serviceQueue := []
              committedService := [] } ]
      | .switch =>
          [ { privateState := queueCounterexamplePrivateState
              serviceQueue := [12]
              committedService := [] } ]
    links :=
      [ { id := 0
          source := 0
          physicalTarget := 1
          rateBps := 8_000_000_000
          propagationNs := 4 },
        { id := 1
          source := 1
          physicalTarget := 2
          rateBps := 8_000_000_000
          propagationNs := 0 },
        { id := 2
          source := 2
          physicalTarget := 0
          rateBps := 8_000_000_000
          propagationNs := 0 } ]
    channels :=
      [ { source := 0
          target := 1
          link := 0
          eventKind := .remoteArrival
          minDelayNs := 5 },
        { source := 1
          target := 2
          link := 1
          eventKind := .remoteArrival
          minDelayNs := 1 } ]
    initialEvents := queueCounterexampleStartPending
    packetDescriptor := queueCounterexampleDescriptor
    initialPacketStore := fun node =>
      if node = 1 then
        [ownedReferenceFixtureEntry 12
            [.pendingEvent queueCounterexampleReady.key, .queueEntry 1 12],
          ownedReferenceFixtureEntry 21
            [.pendingEvent queueCounterexampleArrival.key]]
      else
        []
    initialNextOriginSeq := fun _ => 1
    payloadBytes := fun _ => 1 }

/--
Executable owned-store order regression. Distinct descriptor acquisitions have the same derived
counted store, exact-owner releases commute, and acquisition commutes with a foreign-owner release.
The same owner's acquire/release remains sequenced by its lifecycle.
-/
def descriptorStoreOrderIndependenceCheck : Bool :=
  let r1 : OwnedPacketReference :=
    { descriptor := queueCounterexampleDescriptor 1
      owner := .queueEntry 0 1 }
  let r3 : OwnedPacketReference :=
    { descriptor := queueCounterexampleDescriptor 3
      owner := .queueEntry 0 3 }
  let acquired1 : OwnedPacketReference :=
    { descriptor := queueCounterexampleDescriptor 1
      owner := .inService 0 1 }
  let released1 : OwnedPacketReference :=
    { descriptor := queueCounterexampleDescriptor 1
      owner := .queueEntry 0 1 }
  let releasedService1 : OwnedPacketReference :=
    { descriptor := queueCounterexampleDescriptor 1
      owner := .inService 0 1 }
  let r5 : OwnedPacketReference :=
    { descriptor := queueCounterexampleDescriptor 5
      owner := .queueEntry 0 5 }
  let e1 := ownedReferenceFixtureEntry 1 [r1.owner]
  let e1Shared :=
    ownedReferenceFixtureEntry 1 [released1.owner, releasedService1.owner]
  let e3 := ownedReferenceFixtureEntry 3 [r3.owner]
  let e5 := ownedReferenceFixtureEntry 5 [r5.owner]
  decide (
    deriveCountedPacketStore
        (acquireOwnedReference r3 (acquireOwnedReference r1 [])) =
      deriveCountedPacketStore
        (acquireOwnedReference r1 (acquireOwnedReference r3 [])) ∧
      releaseOwnedReference r5 (releaseOwnedReference r1 [e1, e3, e5]) =
        releaseOwnedReference r1 (releaseOwnedReference r5 [e1, e3, e5]) ∧
      releaseOwnedReference releasedService1
          (releaseOwnedReference released1 [e1Shared]) =
        releaseOwnedReference released1
          (releaseOwnedReference releasedService1 [e1Shared]) ∧
      acquireOwnedReference acquired1
          (releaseOwnedReference released1 [e1]) =
        releaseOwnedReference released1
          (acquireOwnedReference acquired1 [e1]) ∧
      releaseOwnedReference r1 (acquireOwnedReference r1 []) = [])

/--
Canonical owned stores are independent of owner-disjoint acquisition/release order.
-/
def DescriptorStoreOrderIndependenceWitness : Prop :=
  descriptorStoreOrderIndependenceCheck = true

/--
The exact b/p/a/c events from the round-9 obstruction. Queue/in-service ownership follows
`executor/src/scalar.rs:876-956,1123-1218`; CPU emissions and exchange ownership are at
`executor/src/cpu.rs:649-667,4485-4500`.
-/
def referenceCountBEvent : Event :=
  { key := { timeNs := 1, phase := 0, originNode := 0, originSeq := 0 }
    target := 0
    kind := .packetArrival
    payload := 12 }

def referenceCountPEvent : Event :=
  { key := { timeNs := 2, phase := 2, originNode := 1, originSeq := 0 }
    target := 1
    kind := .txReady
    payload := 12 }

def referenceCountAEvent : Event :=
  { key := { timeNs := 3, phase := 0, originNode := 1, originSeq := 2 }
    target := 2
    kind := .remoteArrival
    payload := 12 }

def referenceCountCEvent : Event :=
  { key := { timeNs := 4, phase := 1, originNode := 1, originSeq := 1 }
    target := 1
    kind := .txComplete
    payload := 12 }

def referenceCountPChildren : List Event :=
  [referenceCountCEvent, referenceCountAEvent]

def referenceCountRoundOneBounds : BoundFamily :=
  fun node => if node = 0 then 1 else 3

def referenceCountRoundOneLPOrder : List NodeId :=
  [1, 0, 2]

def referenceCountRoundTwoBounds : BoundFamily :=
  fun _ => 5

def referenceCountRoundTwoLPOrder : List NodeId :=
  [2, 1, 0]

private def applyReferenceCountBpacEvent
    (event : Event)
    (stores : NodeId → List PacketStoreEntry) :
    NodeId → List PacketStoreEntry :=
  if event = referenceCountPEvent then
    fun node =>
      installChildDescriptorsFor queueCounterexampleImage node referenceCountPChildren
        (if node = event.target then
          acquireOwnedReference
            (ownedInServiceReference queueCounterexampleImage node event.payload)
            (releaseOwnedReference
              (ownedQueueReference queueCounterexampleImage node event.payload)
              (releaseOwnedReference
                (ownedEventReference queueCounterexampleImage event)
                (stores node)))
        else
          stores node)
  else
    fun node =>
      if node = event.target then
        let afterEvent :=
          releaseOwnedReference
            (ownedEventReference queueCounterexampleImage event)
            (stores node)
        if event = referenceCountCEvent then
          releaseOwnedReference
            (ownedInServiceReference queueCounterexampleImage node event.payload)
            afterEvent
        else
          afterEvent
      else
        stores node

/--
Executable owned equality regression for the former countermodel. Before `p`, B holds `b`; S holds
the `p` future and queue residency. CPU emission transfers queue residency to in-service and leaves
three holds at S (in-service, local `c`, outbound-envelope `a`); exchange moves the envelope owner
to A's exact pending-event owner, yielding the same stores as scalar immediate target installation.
Canonical `[b,p,a,c]` and round order `[p,a,c,b]` then consume exactly the same references and
finish with identical empty per-LP stores. Round one retains bounds B↦1/S↦3/A↦3 and LP order S/B/A;
round two retains bound 5 and order A/S/B.
The modeled Rust sites are `executor/src/scalar.rs:876-956,1123-1218` and
`executor/src/cpu.rs:649-667,4485-4500`.
-/
def referenceCountBpacCommutativityCheck : Bool :=
  let initialStores : NodeId → List PacketStoreEntry :=
    fun node =>
      if node = 0 then
        [ownedReferenceFixtureEntry 12
          [(ownedEventReference queueCounterexampleImage referenceCountBEvent).owner]]
      else if node = 1 then
        [ownedReferenceFixtureEntry 12
          [(ownedEventReference queueCounterexampleImage referenceCountPEvent).owner,
           (ownedQueueReference queueCounterexampleImage 1 12).owner]]
      else
        []
  let runFrom :
      (NodeId → List PacketStoreEntry) →
        List Event → (NodeId → List PacketStoreEntry) :=
    fun stores (order : List Event) =>
      order.foldl
        (fun stores event => applyReferenceCountBpacEvent event stores)
        stores
  let scalarAfterP :=
    applyReferenceCountBpacEvent referenceCountPEvent initialStores
  let roundAfterPEmission : NodeId → List PacketStoreEntry :=
    fun node =>
      if node = referenceCountPEvent.target then
        holdEmittedChildReferences queueCounterexampleImage node referenceCountPChildren
          (acquireOwnedReference
            (ownedInServiceReference queueCounterexampleImage node
              referenceCountPEvent.payload)
            (releaseOwnedReference
              (ownedQueueReference queueCounterexampleImage node
                referenceCountPEvent.payload)
              (releaseOwnedReference
                (ownedEventReference queueCounterexampleImage referenceCountPEvent)
                (initialStores node))))
      else
        initialStores node
  let remoteEnvelope : RemoteEnvelope :=
    { source := referenceCountPEvent.target
      event := referenceCountAEvent
      packet := queueCounterexampleDescriptor referenceCountAEvent.payload }
  let roundAfterExchange : NodeId → List PacketStoreEntry :=
    fun node =>
      installRemoteEnvelopesFor node [remoteEnvelope]
        (consumeRemoteEnvelopesFor node [remoteEnvelope] (roundAfterPEmission node))
  let canonical := runFrom initialStores
    [referenceCountBEvent, referenceCountPEvent, referenceCountAEvent, referenceCountCEvent]
  let round := runFrom roundAfterExchange
    [referenceCountAEvent, referenceCountCEvent, referenceCountBEvent]
  decide (
    referenceCountPChildren = [referenceCountCEvent, referenceCountAEvent] ∧
      referenceCountRoundOneLPOrder = [1, 0, 2] ∧
      referenceCountRoundTwoLPOrder = [2, 1, 0] ∧
      ¬ belowBound referenceCountRoundOneBounds referenceCountBEvent ∧
      belowBound referenceCountRoundOneBounds referenceCountPEvent ∧
      ¬ belowBound referenceCountRoundOneBounds referenceCountAEvent ∧
      ¬ belowBound referenceCountRoundOneBounds referenceCountCEvent ∧
      belowBound referenceCountRoundTwoBounds referenceCountBEvent ∧
      belowBound referenceCountRoundTwoBounds referenceCountPEvent ∧
      belowBound referenceCountRoundTwoBounds referenceCountAEvent ∧
      belowBound referenceCountRoundTwoBounds referenceCountCEvent ∧
      descriptorReferenceCount 12 (roundAfterPEmission 0) = 1 ∧
      descriptorReferenceCount 12 (roundAfterPEmission 1) = 3 ∧
      descriptorReferenceCount 12 (roundAfterPEmission 2) = 0 ∧
      descriptorReferenceCount 12 (roundAfterExchange 0) = 1 ∧
      descriptorReferenceCount 12 (roundAfterExchange 1) = 2 ∧
      descriptorReferenceCount 12 (roundAfterExchange 2) = 1 ∧
      ([0, 1, 2] : List NodeId).map scalarAfterP =
        ([0, 1, 2] : List NodeId).map roundAfterExchange ∧
      ([0, 1, 2] : List NodeId).map canonical =
        ([0, 1, 2] : List NodeId).map round ∧
      (([0, 1, 2] : List NodeId).all fun node => (canonical node).isEmpty) = true)

theorem referenceCountBpacCommutativity :
    referenceCountBpacCommutativityCheck = true := by
  decide

theorem reversedDescriptorStoreCountermodel_incoherent :
    ¬ DescriptorStoreCoherent queueCounterexampleImage
      [queueCounterexampleEntry 5, queueCounterexampleEntry 1] := by
  simp [DescriptorStoreCoherent, DescriptorStoreSorted,
    queueCounterexampleImage, queueCounterexampleEntry, queueCounterexampleDescriptor]

/--
Embedding the reversed descriptor store at the switch LP violates the machine invariant required
at every post-exchange round boundary.
-/
theorem reversedDescriptorStoreCountermodel_not_wellFormed
    (machine : MachineState TinyQueueStateFamily)
    (hstore :
      machine.packetStore
          { id := 1, kind := .switch, stateSlot := 0 } =
        [queueCounterexampleEntry 5, queueCounterexampleEntry 1]) :
    ¬ MachineWellFormed queueCounterexampleImage machine := by
  intro hwellFormed
  let node : NodeDescriptor :=
    { id := 1, kind := .switch, stateSlot := 0 }
  have hnode : node ∈ queueCounterexampleImage.nodes := by
    simp [node, queueCounterexampleImage]
  have hcoherent := hwellFormed.2.2.2.2.1 node hnode
  have hstore' :
      machine.packetStore node =
        [queueCounterexampleEntry 5, queueCounterexampleEntry 1] := by
    simpa [node] using hstore
  rw [hstore'] at hcoherent
  exact reversedDescriptorStoreCountermodel_incoherent hcoherent

/-- The reversed-store round countermodel cannot be a valid post-exchange start. -/
theorem reversedDescriptorStoreCountermodel_not_postExchangeStart
    (round : RoundState TinyQueueStateFamily)
    (hstore :
      round.machine.packetStore
          { id := 1, kind := .switch, stateSlot := 0 } =
        [queueCounterexampleEntry 5, queueCounterexampleEntry 1]) :
    ¬ PostExchangeStart queueCounterexampleImage round := by
  intro hstart
  exact
    reversedDescriptorStoreCountermodel_not_wellFormed
      round.machine hstore hstart.2.1

/-- Role-correct initial state selected through the concrete image's state arenas. -/
def queueCounterexampleLocalState
    (node : NodeDescriptor) : RoleState TinyQueueStateFamily node.kind :=
  match node.kind with
  | .host =>
      { privateState := queueCounterexampleSourceState
        serviceQueue := []
        committedService := [] }
  | .switch =>
      { privateState := queueCounterexamplePrivateState
        serviceQueue := [12]
        committedService := [] }

/--
Packet selected at `TxReady`. The canonical FIFO reads only the observable queue and ledger; the
forbidden eager policy first consults its hidden private reservation.
-/
def tinyQueueSelectedPacket
    (eager : Bool)
    (state : RoleState TinyQueueStateFamily kind) : Option PayloadId :=
  match state.committedService with
  | _ :: _ => none
  | [] =>
      match eager, state.privateState.hiddenReservation with
      | true, some packet => some packet
      | _, _ => state.serviceQueue.head?

/-- The one service decision introduced by a successful tiny `TxReady`, if any. -/
def tinyQueueDecisions
    (node : NodeDescriptor)
    (event : Event) : Option PayloadId → List ServiceDecision
  | some packet =>
      [ { node := node.id
          decisionKey := event.key
          packet
          committedNonPreemptively := true } ]
  | none => []

/--
The FIFO fixture's successful service start emits completion first and remote arrival second, with
both children carrying the selected packet as in
`executor/src/scalar.rs:899-921,1145-1166`. The concrete switch sends remotely to terminal host 2;
the unused host cases remain local so the total contract sketch stays role-correct.
-/
def tinyQueueServiceChildren
    (node : NodeDescriptor)
    (event : Event) : Option PayloadId → List Event
  | none => []
  | some packet =>
      let remoteTarget := if node.id = 1 then 2 else node.id
      [ { key :=
            { timeNs := event.key.timeNs + 1
              phase := eventPhase .txComplete
              originNode := node.id
              originSeq := 1 }
          target := node.id
          kind := .txComplete
          payload := packet },
        { key :=
            { timeNs := event.key.timeNs + 1
              phase := eventPhase .remoteArrival
              originNode := node.id
              originSeq := 2 }
          target := remoteTarget
          kind := .remoteArrival
          payload := packet } ]

/--
Forbidden arrival-time reservation: remove the current public FIFO head and hide it in private
state without committing the service ledger or recording a decision.
-/
def tinyQueueReservePrivately
    (state : RoleState TinyQueueStateFamily kind) :
    TinyQueuePrivateState × List PayloadId :=
  match state.committedService, state.privateState.hiddenReservation, state.serviceQueue with
  | [], none, packet :: rest =>
      ({ state.privateState with hiddenReservation := some packet }, rest)
  | _, _, _ => (state.privateState, state.serviceQueue)

/-- Arrival effect after any forbidden eager reservation has been applied. -/
def tinyQueueArrivalState
    (eager : Bool)
    (state : RoleState TinyQueueStateFamily kind)
    (packet : PayloadId) : RoleState TinyQueueStateFamily kind :=
  let reserved :=
    if eager then tinyQueueReservePrivately state
    else (state.privateState, state.serviceQueue)
  let privateState := reserved.1
  let serviceQueue := reserved.2
  if privateState.capacity ≠ 0 ∧ privateState.capacity ≤ serviceQueue.length then
    { privateState :=
        { privateState with dropped := privateState.dropped ++ [packet] }
      serviceQueue
      committedService := state.committedService }
  else
    { privateState :=
        { privateState with accepted := privateState.accepted ++ [packet] }
      serviceQueue := serviceQueue ++ [packet]
      committedService := state.committedService }

/-- Service-start effect for the packet selected by the canonical or eager policy. -/
def tinyQueueReadyState
    (selected : Option PayloadId)
    (state : RoleState TinyQueueStateFamily kind) :
    RoleState TinyQueueStateFamily kind :=
  match selected with
  | none => state
  | some packet =>
      { privateState :=
          { state.privateState with hiddenReservation := none }
        serviceQueue := state.serviceQueue.erase packet
        committedService := [packet] }

/-- Complete transition result for the concrete canonical/eager policies. -/
def tinyQueueTransitionResult
    (eager : Bool)
    (node : NodeDescriptor)
    (event : Event)
    (state : RoleState TinyQueueStateFamily node.kind) :
    TransitionResult TinyQueueStateFamily node.kind :=
  let selected :=
    if event.kind = .txReady then tinyQueueSelectedPacket eager state else none
  let nextState :=
    match event.kind with
    | .remoteArrival => tinyQueueArrivalState eager state event.payload
    | .txReady => tinyQueueReadyState selected state
    | .txComplete =>
        { state with
          committedService := state.committedService.erase event.payload }
    | .packetArrival | .retransmissionTimeout => state
  { nextState
    children := tinyQueueServiceChildren node event selected
    packetReferenceIncrements :=
      stateReferenceIncrements queueCounterexampleImage node state nextState
    packetReferenceConsumptions :=
      ownedEventReference queueCounterexampleImage event ::
        stateReferenceConsumptions queueCounterexampleImage node state nextState
    summaryDelta := RunSummary.zero
    observedPackets := []
    departures := []
    arrivals := []
    decisions := tinyQueueDecisions node event selected }

/--
Concrete successful transition relation. Both policies are deterministic and enabled for every
role-supported reachable configuration; they differ only in whether arrival eagerly performs the
future service selection. There is deliberately no fixture-local equality relating private state
to the service ledger: the general private-irrelevance premise must reject smuggling.
-/
def tinyQueueTransition (eager : Bool) : TransitionRelation TinyQueueStateFamily :=
  fun node event state result =>
    node ∈ queueCounterexampleImage.nodes ∧
      event.target = node.id ∧
      roleSupports node.kind event.kind ∧
      (event.kind = .txComplete → event.payload ∈ state.committedService) ∧
      result = tinyQueueTransitionResult eager node event state

/--
Executable per-LP commutation sketch for the accepted FIFO instance. Two decisionless arrivals on
distinct LPs consume their own held event references, so each local application touches only its
own exact owner store. Updating the components in either order gives pointwise-equal stores. This
is the concrete owner-disjoint case required by `IndependentStepsCommute`.
The ownership split mirrors `CpuLp.transitions` at `executor/src/cpu.rs:563-569` and local drain at
`executor/src/cpu.rs:586-690`.
-/
def fifoPerLPStoreCommutationCheck : Bool :=
  let firstNode : NodeDescriptor :=
    { id := 1, kind := .switch, stateSlot := 0 }
  let secondNode : NodeDescriptor :=
    { id := 2, kind := .host, stateSlot := 1 }
  let firstEvent := queueCounterexampleArrival
  let secondEvent : Event :=
    { queueCounterexampleArrival with
      key :=
        { timeNs := 6, phase := eventPhase .remoteArrival
          originNode := 2, originSeq := 0 }
      target := secondNode.id
      payload := 12 }
  let firstState := queueCounterexampleLocalState firstNode
  let secondState := queueCounterexampleLocalState secondNode
  let firstResult :=
    tinyQueueTransitionResult false firstNode firstEvent firstState
  let secondResult :=
    tinyQueueTransitionResult false secondNode secondEvent secondState
  let initialStores := initialPacketStore queueCounterexampleImage
  let stores : NodeDescriptor → List PacketStoreEntry :=
    fun owner =>
      if owner.id = secondNode.id then
        [ownedReferenceFixtureEntry secondEvent.payload
          [(ownedEventReference queueCounterexampleImage secondEvent).owner]]
      else
        initialStores owner
  let replaceStore :=
    fun (owner : NodeDescriptor) (value : List PacketStoreEntry)
        (current : NodeDescriptor → List PacketStoreEntry) =>
      fun candidate =>
        if candidate.id = owner.id then value else current candidate
  let firstStore := applyPacketEffects firstResult (stores firstNode)
  let secondStore := applyPacketEffects secondResult (stores secondNode)
  let afterFirstThenSecond :=
    replaceStore secondNode secondStore
      (replaceStore firstNode firstStore stores)
  let afterSecondThenFirst :=
    replaceStore firstNode firstStore
      (replaceStore secondNode secondStore stores)
  decide (
    firstNode.id ≠ secondNode.id ∧
      firstResult.children = [] ∧
      secondResult.children = [] ∧
      firstResult.packetReferenceIncrements = [] ∧
      firstResult.packetReferenceConsumptions =
        [ownedEventReference queueCounterexampleImage firstEvent] ∧
      secondResult.packetReferenceIncrements =
        [ownedQueueReference queueCounterexampleImage secondNode.id secondEvent.payload] ∧
      secondResult.packetReferenceConsumptions =
        [ownedEventReference queueCounterexampleImage secondEvent] ∧
      queueCounterexampleImage.nodes.map afterFirstThenSecond =
        queueCounterexampleImage.nodes.map afterSecondThenFirst)

/--
Executable strengthened-FIFO satisfiability sketch. Canonical selection is identical under private
replacement, consumes the ready-event reference, creates two child references, transfers the remote
one, and consumes the completion reference to zero. Exact source/target counts therefore match the
transmitter decrement and CPU exchange sites at
`executor/src/scalar.rs:1645-1683` and `executor/src/cpu.rs:649-667,4485-4500`.
-/
def fifoStrengthenedServiceContractCheck : Bool :=
  let node : NodeDescriptor :=
    { id := 1, kind := .switch, stateSlot := 0 }
  let initial : RoleState TinyQueueStateFamily .switch :=
    { privateState := queueCounterexamplePrivateState
      serviceQueue := [12]
      committedService := [] }
  let alternate : RoleState TinyQueueStateFamily .switch :=
    { initial with
      privateState :=
        { initial.privateState with
          hiddenReservation := some 21
          accepted := [30] } }
  let readyResult :=
    tinyQueueTransitionResult false node queueCounterexampleReady initial
  let alternateReadyResult :=
    tinyQueueTransitionResult false node queueCounterexampleReady alternate
  match readyResult.children with
  | [completion, remote] =>
      let afterArrival :=
        tinyQueueTransitionResult false node remote readyResult.nextState
      let afterCompletion :=
        tinyQueueTransitionResult false node completion afterArrival.nextState
      let remoteStore :=
        installChildDescriptorsFor
          queueCounterexampleImage 2 readyResult.children []
      let readyStore :=
        [ownedReferenceFixtureEntry 12
          [(ownedEventReference queueCounterexampleImage
              queueCounterexampleReady).owner,
           (ownedQueueReference queueCounterexampleImage node.id 12).owner]]
      let sourceStoreAfterEmission :=
        holdEmittedChildReferences queueCounterexampleImage node.id readyResult.children
          (applyPacketEffects readyResult readyStore)
      let sourceStoreAfterExchange :=
        releaseOwnedReference
          (ownedEnvelopeReference
            { source := node.id
              event := remote
              packet := queueCounterexampleDescriptor remote.payload })
          sourceStoreAfterEmission
      let sourceStoreAfterCompletion :=
        applyPacketEffects afterCompletion sourceStoreAfterExchange
      decide (
        fifoPerLPStoreCommutationCheck = true ∧
          SameServiceSelectionResult readyResult alternateReadyResult ∧
          ObservationRecordsUseEventKey queueCounterexampleReady readyResult ∧
          descriptorReferenceCount 12 sourceStoreAfterEmission = 3 ∧
          descriptorReferenceCount 12 sourceStoreAfterExchange = 2 ∧
          descriptorReferenceCount 12 remoteStore = 1 ∧
          sourceStoreAfterCompletion = [] ∧
          initial.committedService.Nodup ∧
          SelectionIntroduced initial readyResult.nextState 12 ∧
          CommittedServiceTransitionValid
            queueCounterexampleReady readyResult.decisions initial readyResult.nextState ∧
          readyResult.nextState.committedService = [12] ∧
          readyResult.nextState.committedService.Nodup ∧
          readyResult.decisions.map ServiceDecision.packet = [12] ∧
          ServiceDecisionChildrenMatch readyResult ∧
          readyResult.children.map Event.kind =
            [.txComplete, .remoteArrival] ∧
          readyResult.children.map Event.payload = [12, 12] ∧
          afterArrival.nextState.committedService =
            readyResult.nextState.committedService ∧
          afterArrival.nextState.committedService.Nodup ∧
          afterArrival.decisions = [] ∧
          ObservationRecordsUseEventKey remote afterArrival ∧
          ServiceDecisionChildrenMatch afterArrival ∧
          CommittedServiceTransitionValid
            remote afterArrival.decisions readyResult.nextState afterArrival.nextState ∧
          afterArrival.nextState.committedService = [12] ∧
          afterCompletion.nextState.committedService =
            afterArrival.nextState.committedService.erase completion.payload ∧
          afterCompletion.nextState.committedService.Nodup ∧
          afterCompletion.decisions = [] ∧
          ObservationRecordsUseEventKey completion afterCompletion ∧
          ServiceDecisionChildrenMatch afterCompletion ∧
          CommittedServiceTransitionValid
            completion afterCompletion.decisions
              afterArrival.nextState afterCompletion.nextState ∧
          afterCompletion.nextState.committedService = [])
  | _ => false

/--
Executable owned-form regression for the former packet-lifetime removal race. A pending-event
release commutes with acquisition of the distinct envelope owner, both exact holds are available,
and a duplicated event-owner release is rejected.
-/
def packetLifetimeRemovalRaceRejectedCheck : Bool :=
  let payload : PayloadId := 12
  let descriptor := queueCounterexampleDescriptor payload
  let node : NodeDescriptor :=
    { id := 2, kind := .host, stateSlot := 1 }
  let event : Event :=
    { key :=
        { timeNs := 20, phase := eventPhase .remoteArrival
          originNode := 1, originSeq := 2 }
      target := node.id
      kind := .remoteArrival
      payload }
  let envelope : RemoteEnvelope :=
    { source := 1, event, packet := descriptor }
  let state := queueCounterexampleLocalState node
  let eventReference := ownedEventReference queueCounterexampleImage event
  let envelopeReference := ownedEnvelopeReference envelope
  let consumption := tinyQueueTransitionResult false node event state
  let underConsumption :=
    { consumption with
      packetReferenceConsumptions := [eventReference, eventReference] }
  let held :=
    [ownedReferenceFixtureEntry payload [eventReference.owner]]
  let envelopeHeld :=
    [ownedReferenceFixtureEntry payload [envelopeReference.owner]]
  let consumeThenIncrement :=
    acquireOwnedReference envelopeReference
      (releaseOwnedReference eventReference held)
  let incrementThenConsume :=
    releaseOwnedReference eventReference
      (acquireOwnedReference envelopeReference held)
  decide (consumeThenIncrement = incrementThenConsume) &&
    decide (envelope.packet =
      queueCounterexampleImage.packetDescriptor envelope.event.payload) &&
    decide (PacketReferencesHeld [eventReference] held) &&
    decide (PacketReferencesHeld [envelopeReference] envelopeHeld) &&
    decide (ReferenceConsumptionsValid
      queueCounterexampleImage node event state consumption held) &&
    decide (¬ ReferenceConsumptionsValid
      queueCounterexampleImage node event state underConsumption held)

/--
The former removal race commutes, while duplicate release of one structural owner remains
impossible.
-/
def PacketLifetimeRemovalRaceRejectedShape : Prop :=
  packetLifetimeRemovalRaceRejectedCheck = true

/--
Executable rejection witness for the session-6 consume-and-reacquire launderer. The processed e2
owns payload 12 and changes no queue or in-service residency, while its forged effects name the
payload-99 completion and envelope holds created by e1. Aggregate source counts permit the update;
the owned lifecycle delta must reject it structurally.
-/
def referenceProvenanceLaundererRejectedCheck : Bool :=
  let node : NodeDescriptor :=
    { id := 1, kind := .host, stateSlot := 1 }
  let e1 : Event :=
    { key :=
        { timeNs := 1, phase := eventPhase .txReady
          originNode := 1, originSeq := 0 }
      target := node.id
      kind := .txReady
      payload := 11 }
  let e2 : Event :=
    { key :=
        { timeNs := 2, phase := eventPhase .remoteArrival
          originNode := 1, originSeq := 1 }
      target := node.id
      kind := .remoteArrival
      payload := 12 }
  let completion : Event :=
    { key :=
        { timeNs := 10, phase := eventPhase .txComplete
          originNode := 1, originSeq := 2 }
      target := node.id
      kind := .txComplete
      payload := 99 }
  let remote : Event :=
    { key :=
        { timeNs := 10, phase := eventPhase .remoteArrival
          originNode := 1, originSeq := 3 }
      target := 2
      kind := .remoteArrival
      payload := 99 }
  let state : RoleState TinyQueueStateFamily .host :=
    { privateState := queueCounterexampleSourceState
      serviceQueue := []
      committedService := [] }
  let foreignCompletion :=
    ownedEventReference queueCounterexampleImage completion
  let foreignEnvelope :=
    ownedEnvelopeReference
      { source := node.id
        event := remote
        packet := queueCounterexampleDescriptor remote.payload }
  let forged :=
    { tinyQueueTransitionResult false node e2 state with
      packetReferenceIncrements := [foreignCompletion, foreignEnvelope]
      packetReferenceConsumptions := [foreignCompletion, foreignEnvelope] }
  let store :=
    [ownedReferenceFixtureEntry 12
      [(ownedEventReference queueCounterexampleImage e2).owner],
     ownedReferenceFixtureEntry 99
      [foreignCompletion.owner, foreignEnvelope.owner]]
  decide (e1 ≠ e2) &&
    decide (PacketReferencesHeld
      [foreignCompletion, foreignEnvelope] store) &&
    decide (¬ ReferenceConsumptionsValid
      queueCounterexampleImage node e2 state forged store)

/-- The session-6 foreign-owner consume-and-reacquire transition is structurally rejected. -/
theorem referenceProvenanceLaundererRejected :
    referenceProvenanceLaundererRejectedCheck = true := by
  decide

/--
Executable regression for the former cross-LP observation-key countermodel. Two independent LPs
process distinct completion events but are given departure records with the first event's key.
The first transition is coherent, while the aliased second transition violates the
processed-event key binding.
-/
def aliasedObservationKeyRejectedCheck : Bool :=
  let firstNode : NodeDescriptor :=
    { id := 0, kind := .host, stateSlot := 0 }
  let secondNode : NodeDescriptor :=
    { id := 2, kind := .host, stateSlot := 1 }
  let initial : RoleState TinyQueueStateFamily .host :=
    { privateState := queueCounterexampleSourceState
      serviceQueue := []
      committedService := [12, 21] }
  let firstEvent : Event :=
    { key :=
        { timeNs := 5, phase := eventPhase .txComplete
          originNode := 0, originSeq := 0 }
      target := 0
      kind := .txComplete
      payload := 12 }
  let secondEvent : Event :=
    { key :=
        { timeNs := 6, phase := eventPhase .txComplete
          originNode := 2, originSeq := 0 }
      target := 2
      kind := .txComplete
      payload := 21 }
  let firstResult :=
    { tinyQueueTransitionResult false firstNode firstEvent initial with
      departures :=
        [ { eventKey := firstEvent.key
            departure := { payload := 12, timeNs := firstEvent.key.timeNs } } ] }
  let aliasedSecondResult :=
    { tinyQueueTransitionResult false secondNode secondEvent initial with
      departures :=
        [ { eventKey := firstEvent.key
            departure := { payload := 21, timeNs := secondEvent.key.timeNs } } ] }
  decide (
    firstEvent.key ≠ secondEvent.key ∧
      ObservationRecordsUseEventKey firstEvent firstResult ∧
      ¬ ObservationRecordsUseEventKey secondEvent aliasedSecondResult)

/--
Executable private-smuggling check behind the eager model's failure of the general premise. The
observable queue and ledger are identical, but replacing only the hidden reservation changes the
`TxReady` service result.
-/
def eagerSelectionPrivateSmugglingCheck : Bool :=
  let node : NodeDescriptor :=
    { id := 1, kind := .switch, stateSlot := 0 }
  let smuggled : RoleState TinyQueueStateFamily .switch :=
    { privateState :=
        { capacity := 1
          hiddenReservation := some 12
          accepted := [21]
          dropped := [] }
      serviceQueue := [21]
      committedService := [] }
  let alternate : RoleState TinyQueueStateFamily .switch :=
    { smuggled with
      privateState := { smuggled.privateState with hiddenReservation := none } }
  decide (
    smuggled.serviceQueue = alternate.serviceQueue ∧
      smuggled.committedService = alternate.committedService ∧
      ¬ SameServiceSelectionResult
        (tinyQueueTransitionResult true node queueCounterexampleReady smuggled)
        (tinyQueueTransitionResult true node queueCounterexampleReady alternate))

/--
Forbidden private-payload smuggler. Public FIFO selection still commits and records packet 12, but
the `TxComplete` and `RemoteArrival` payload is taken from private state. Under replacement private
state every old comparison field agrees; only the emitted children diverge.
-/
def tinyQueuePrivatePayloadResult
    (node : NodeDescriptor)
    (event : Event)
    (state : RoleState TinyQueueStateFamily node.kind) :
    TransitionResult TinyQueueStateFamily node.kind :=
  let selected :=
    if event.kind = .txReady then tinyQueueSelectedPacket false state else none
  let emitted :=
    match selected, state.privateState.hiddenReservation with
    | some _, some hidden => some hidden
    | _, _ => selected
  { tinyQueueTransitionResult false node event state with
    children := tinyQueueServiceChildren node event emitted }

/-- Successful transition relation for the private-payload smuggler shape. -/
def tinyQueuePrivatePayloadTransition : TransitionRelation TinyQueueStateFamily :=
  fun node event state result =>
    node ∈ queueCounterexampleImage.nodes ∧
      event.target = node.id ∧
      roleSupports node.kind event.kind ∧
      (event.kind = .txComplete → event.payload ∈ state.committedService) ∧
      result = tinyQueuePrivatePayloadResult node event state

/--
Executable payload-smuggling regression. Queue, commitment, decisions, and every non-child effect
agree after private replacement, but the private payload produces `[21, 21]` children instead of
the committed packet's `[12, 12]`. The widened `SameServiceSelectionResult` and the explicit
decision/child payload axiom therefore both reject the shape.
-/
def privatePayloadSmugglingCheck : Bool :=
  let node : NodeDescriptor :=
    { id := 1, kind := .switch, stateSlot := 0 }
  let smuggled : RoleState TinyQueueStateFamily .switch :=
    { privateState :=
        { capacity := 1
          hiddenReservation := some 21
          accepted := []
          dropped := [] }
      serviceQueue := [12]
      committedService := [] }
  let alternate : RoleState TinyQueueStateFamily .switch :=
    { smuggled with
      privateState := { smuggled.privateState with hiddenReservation := none } }
  let smuggledResult :=
    tinyQueuePrivatePayloadResult node queueCounterexampleReady smuggled
  let alternateResult :=
    tinyQueuePrivatePayloadResult node queueCounterexampleReady alternate
  decide (
    smuggledResult.nextState.serviceQueue =
        alternateResult.nextState.serviceQueue ∧
      smuggledResult.nextState.committedService =
        alternateResult.nextState.committedService ∧
      smuggledResult.decisions = alternateResult.decisions ∧
      smuggledResult.packetReferenceIncrements =
        alternateResult.packetReferenceIncrements ∧
      smuggledResult.packetReferenceConsumptions =
        alternateResult.packetReferenceConsumptions ∧
      smuggledResult.summaryDelta = alternateResult.summaryDelta ∧
      smuggledResult.observedPackets = alternateResult.observedPackets ∧
      smuggledResult.departures = alternateResult.departures ∧
      smuggledResult.arrivals = alternateResult.arrivals ∧
      smuggledResult.nextState.committedService = [12] ∧
      smuggledResult.decisions.map ServiceDecision.packet = [12] ∧
      smuggledResult.children.map Event.payload = [21, 21] ∧
      alternateResult.children.map Event.payload = [12, 12] ∧
      ¬ SameServiceSelectionResult smuggledResult alternateResult)

/--
Countermodel-shaped statement for the former emitted-child channel. It obeys the actual-start,
exact decision-trace, and non-preemption clauses, but violates both new protections: its children
do not carry the decided packet, and private replacement changes a public transition effect.
-/
def PrivatePayloadSmugglerCountermodel : Prop :=
  privatePayloadSmugglingCheck = true ∧
    ActualServiceStartDiscipline tinyQueuePrivatePayloadTransition ∧
    ServiceDecisionTraceComplete tinyQueuePrivatePayloadTransition ∧
    CommittedServiceNonPreemptive tinyQueuePrivatePayloadTransition ∧
    ¬ ServiceDecisionEmissionsMatch tinyQueuePrivatePayloadTransition ∧
    ¬ TxReadySelectionPrivateIrrelevant tinyQueuePrivatePayloadTransition ∧
    ¬ CompleteActualServiceStartDiscipline tinyQueuePrivatePayloadTransition

/--
Forbidden erasure-preemptor. A `RemoteArrival` applies its ordinary queue effect and then silently
clears all committed service; the next `TxReady` can consequently choose a different packet.
-/
def tinyQueueErasurePreemptingResult
    (node : NodeDescriptor)
    (event : Event)
    (state : RoleState TinyQueueStateFamily node.kind) :
    TransitionResult TinyQueueStateFamily node.kind :=
  let ordinary := tinyQueueTransitionResult false node event state
  if event.kind = .remoteArrival then
    { ordinary with
      nextState := { ordinary.nextState with committedService := [] } }
  else
    ordinary

/-- Successful transition relation for the committed-service erasure shape. -/
def tinyQueueErasurePreemptingTransition : TransitionRelation TinyQueueStateFamily :=
  fun node event state result =>
    node ∈ queueCounterexampleImage.nodes ∧
      event.target = node.id ∧
      roleSupports node.kind event.kind ∧
      (event.kind = .txComplete → event.payload ∈ state.committedService) ∧
      result = tinyQueueErasurePreemptingResult node event state

/--
Executable erasure-preemption regression. Arrival removes commitment 12 without introducing any
selection or decision; the later readiness event then commits packet 21. The exact decisionless
delta rejects the erasure immediately, before the later selection can replace packet 12.
-/
def committedServiceErasureCheck : Bool :=
  let node : NodeDescriptor :=
    { id := 1, kind := .switch, stateSlot := 0 }
  let arrival : Event :=
    { queueCounterexampleArrival with payload := 30 }
  let ready : Event :=
    { queueCounterexampleReady with payload := 21 }
  let transmitting : RoleState TinyQueueStateFamily .switch :=
    { privateState := queueCounterexamplePrivateState
      serviceQueue := [21]
      committedService := [12] }
  let erased :=
    tinyQueueErasurePreemptingResult node arrival transmitting
  let restarted :=
    tinyQueueErasurePreemptingResult node ready erased.nextState
  decide (
    12 ∈ transmitting.committedService ∧
      12 ∉ erased.nextState.committedService ∧
      erased.nextState.committedService = [] ∧
      erased.decisions = [] ∧
      ¬ (12 ∉ transmitting.committedService ∧
        12 ∈ erased.nextState.committedService) ∧
      ¬ (21 ∉ transmitting.committedService ∧
        21 ∈ erased.nextState.committedService) ∧
      restarted.nextState.committedService = [21] ∧
      restarted.decisions.map ServiceDecision.packet = [21] ∧
      restarted.children.map Event.payload = [21, 21])

/--
Executable multiplicity regression for the forbidden decisionless duplicator `[12] → [12, 12]`
on a non-completion. The exact selection delta now observes the append, while exact
decision-payload accounting and `Nodup` preservation both make the step fail non-preemption.
-/
def committedServiceDuplicatorCheck : Bool :=
  let before : RoleState TinyQueueStateFamily .switch :=
    { privateState := queueCounterexamplePrivateState
      serviceQueue := [21]
      committedService := [12] }
  let after : RoleState TinyQueueStateFamily .switch :=
    { before with committedService := [12, 12] }
  decide (
    before.committedService.Nodup ∧
      ¬ after.committedService.Nodup ∧
      SelectionIntroduced before after 12 ∧
      ¬ CommittedServiceTransitionValid
        queueCounterexampleArrival [] before after)

/--
Executable multiplicity regression for the forbidden decisionless partial eraser
`[12, 12] → [12]` on a non-completion. Even from the deliberately malformed duplicate source, the
unconditional exact list delta makes the step fail non-preemption.
-/
def committedServicePartialEraserCheck : Bool :=
  let before : RoleState TinyQueueStateFamily .switch :=
    { privateState := queueCounterexamplePrivateState
      serviceQueue := [21]
      committedService := [12, 12] }
  let after : RoleState TinyQueueStateFamily .switch :=
    { before with committedService := [12] }
  decide (
    ¬ SelectionIntroduced before after 12 ∧
      ¬ CommittedServiceTransitionValid
        queueCounterexampleArrival [] before after)

/-- Both commitment-multiplicity attacks execute and fail the exact non-preemption statement. -/
def CommittedServiceMultiplicityCounterexamples : Prop :=
  committedServiceDuplicatorCheck = true ∧
    committedServicePartialEraserCheck = true

/--
Countermodel-shaped statement for decision-trace completeness. The erasure policy retains actual
start, exact decision tracing, child payload binding, and private irrelevance, but violates exact
non-preemption and therefore no longer satisfies the complete F4 contract.
-/
def CommittedServiceErasurePreemptorCountermodel : Prop :=
  committedServiceErasureCheck = true ∧
    ActualServiceStartDiscipline tinyQueueErasurePreemptingTransition ∧
    ServiceDecisionTraceComplete tinyQueueErasurePreemptingTransition ∧
    TxReadySelectionPrivateIrrelevant tinyQueueErasurePreemptingTransition ∧
    ServiceDecisionEmissionsMatch tinyQueueErasurePreemptingTransition ∧
    ¬ CommittedServiceNonPreemptive tinyQueueErasurePreemptingTransition ∧
    ¬ CompleteActualServiceStartDiscipline tinyQueueErasurePreemptingTransition

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
eager policy records its eventual `TxReady` decision completely but fails the general
private-state-irrelevance premise. With public queue `[21]` and an empty ledger, hidden reservation
`some 12` selects packet 12 while alternate private state with no reservation selects packet 21.

The accepted FIFO instance remains satisfiable: the arrival at 5 sees public queue `[12]` at
capacity one and drops packet 21; `TxReady` at 10 removes public head 12, commits `[12]`, and emits
exactly one completion and one remote-arrival child carrying packet 12. Its selection and every
public effect are unchanged by arbitrary private bookkeeping; the duplicate-free list `[12]`
persists by exact equality until a matching `TxComplete 12` produces `[12].erase 12`.

This is a `Prop`-valued T11 statement over concrete executable data. T12 proves the proposition
alongside F4.
-/
def ReachableEagerSelectionCountermodel : Prop :=
  EagerSelectionCounterexampleShape ∧
    eagerSelectionPrivateSmugglingCheck = true ∧
    AcceptedModel queueCounterexampleImage (tinyQueueTransition false) ∧
    AcceptedModel queueCounterexampleImage (tinyQueueTransition true) ∧
    InitialMachine queueCounterexampleImage queueCounterexampleMachine ∧
    ConstantGlobalBoundsValid
      queueCounterexampleImage
      (tinyQueueTransition true)
      queueCounterexampleMachine ∧
    CompleteActualServiceStartDiscipline (tinyQueueTransition false) ∧
    ActualServiceStartDiscipline (tinyQueueTransition true) ∧
    ServiceDecisionTraceComplete (tinyQueueTransition true) ∧
    ServiceDecisionEmissionsMatch (tinyQueueTransition true) ∧
    CommittedServiceNonPreemptive (tinyQueueTransition true) ∧
    ¬ TxReadySelectionPrivateIrrelevant (tinyQueueTransition true) ∧
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
