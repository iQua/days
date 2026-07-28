import DaysExecutor.Event

namespace DaysExecutor

/--
Closed semantic LP role mirroring `executor/src/model.rs:5-11` (`NodeKind`).
-/
inductive NodeKind where
  | host
  | switch
  deriving DecidableEq, Repr, Ord

/-- Stable flow identifier used by immutable packet descriptors. -/
abbrev FlowId := Nat

/--
Closed packet direction mirroring `executor/src/image.rs:175-181` (`PacketKind`).
-/
inductive PacketKind where
  | data
  | feedback
  deriving DecidableEq, Repr, Ord

/--
Immutable packet data carried alongside remote events by the CPU backend, mirroring
`executor/src/image.rs:162-173` (`PacketDescriptor`).
-/
structure PacketDescriptor where
  id : PayloadId
  flow : FlowId
  sizeBytes : Nat
  kind : PacketKind
  deriving DecidableEq, Repr

/--
Kind-indexed mutable-state family formalizing Rust's separate host and switch arenas at
`executor/src/image.rs:247-260`.
-/
abbrev StateFamily := NodeKind → Type

/--
Role state with the queue and committed non-preemptive service slots exposed as first-class
semantic fields. `serviceQueue` is the ordered packet view consulted at service start, while
`committedService` represents the host/switch `in_service` slots. Both are semantic projections of
the role-specific state at `executor/src/image.rs:25-69`; `privateState` contains only data that
cannot influence service selection except by an explicit `serviceQueue` mutation.
-/
structure RoleState (State : StateFamily) (kind : NodeKind) where
  privateState : State kind
  serviceQueue : List PayloadId
  committedService : List PayloadId

/--
Semantic LP identity and role-specific state slot mirroring
`executor/src/image.rs:15-23` (`NodeDescriptor`).
-/
structure NodeDescriptor where
  id : NodeId
  kind : NodeKind
  stateSlot : Nat
  deriving DecidableEq, Repr

/--
Kind-indexed state arenas mirroring `SimulationImage.host_states` and `switch_states` at
`executor/src/image.rs:252-254`.
-/
abbrev StateArena (State : StateFamily) := (kind : NodeKind) → List (RoleState State kind)

/--
One constant-rate directed link mirroring `executor/src/image.rs:183-195`
(`LinkDescriptor`).
-/
structure LinkDescriptor where
  id : LinkId
  source : NodeId
  physicalTarget : NodeId
  rateBps : Nat
  propagationNs : Nat
  deriving DecidableEq, Repr

/--
Exact ceiling division used on Rust's successful checked-arithmetic path at
`executor/src/time.rs:36-51`.
-/
def ceilDiv (numerator denominator : Nat) : Nat :=
  numerator / denominator + if numerator % denominator = 0 then 0 else 1

/--
Exact integer serialization delay `ceil(8 * bytes * 10^9 / rate)` mirroring
`executor/src/time.rs:36-51`; image validity separately excludes a zero rate.
-/
def serializationTimeNs (bytes rateBps : Nat) : Nat :=
  ceilDiv (8 * bytes * 1_000_000_000) rateBps

/--
Successful-path serialization-plus-propagation delay mirroring
`executor/src/image.rs:197-205` and `executor/src/time.rs:54-66`.

Lean's unbounded `Nat` deliberately models only executions for which Rust's checked `u64`
arithmetic succeeds.
-/
def linkDelayNs (link : LinkDescriptor) (bytes : Nat) : Nat :=
  serializationTimeNs bytes link.rateBps + link.propagationNs

/--
Successful-path directed-link arrival time mirroring `executor/src/time.rs:54-66`
(`link_arrival_time_ns`).
-/
def linkArrivalTimeNs (link : LinkDescriptor) (startTimeNs bytes : Nat) : Nat :=
  startTimeNs + linkDelayNs link bytes

/--
Declared cross-LP event channel mirroring `executor/src/image.rs:209-220`
(`RemoteChannel`).
-/
structure RemoteChannel where
  source : NodeId
  target : NodeId
  link : LinkId
  eventKind : EventKind
  minDelayNs : Nat
  deriving DecidableEq, Repr

/--
One heterogeneous accepted-image candidate mirroring
`executor/src/image.rs:247-262` (`SimulationImage`).
-/
structure SimulationImage (State : StateFamily) where
  stopTimeNs : Nat
  nodes : List NodeDescriptor
  stateArena : StateArena State
  links : List LinkDescriptor
  channels : List RemoteChannel
  initialEvents : List Event
  /--
  Immutable semantic descriptor oracle. Rust materializes generated descriptors on demand and
  transports them in `RemoteEnvelope`; the model uses this total oracle to state descriptor
  identity without pretending `initial_packets` is a whole-run table.
  -/
  packetDescriptor : PayloadId → PacketDescriptor
  /--
  Per-LP initial descriptor ownership derived by the CPU constructor from initial events and
  mutable queue/service state; this is semantic prepared-image data, not an added Rust field.
  -/
  initialPacketStore : NodeId → List PacketDescriptor
  /-- Initial per-origin allocation cursor derived from role state during image preparation. -/
  initialNextOriginSeq : NodeId → Nat
  payloadBytes : PayloadId → Nat

/--
Small total list lookup used for Rust's checked state-slot indexing at
`executor/src/scalar.rs:1321-1362`.
-/
def listGet? (items : List α) (index : Nat) : Option α :=
  match items, index with
  | [], _ => none
  | head :: _, 0 => some head
  | _ :: tail, index + 1 => listGet? tail index

/--
Role-selected state lookup corresponding to `state_slot` indexing at
`executor/src/scalar.rs:1321-1362`.
-/
def stateAt?
    (image : SimulationImage State)
    (node : NodeDescriptor) : Option (RoleState State node.kind) :=
  listGet? (image.stateArena node.kind) node.stateSlot

/--
Unique semantic LP identities required by the validator and indexed lookup at
`executor/src/validate.rs:196-230` and `executor/src/scalar.rs:1315-1319`.
-/
def UniqueNodeIds (image : SimulationImage State) : Prop :=
  (image.nodes.map NodeDescriptor.id).Nodup

/--
Every descriptor indexes an existing state slot in its selected role arena, mirroring
`executor/src/validate.rs:200-210`.
-/
def DescriptorSlotsValid (image : SimulationImage State) : Prop :=
  ∀ node ∈ image.nodes, node.stateSlot < (image.stateArena node.kind).length

/--
Every mutable role-state slot has exactly one LP owner, mirroring
`executor/src/validate.rs:196-230`.
-/
def ExactStateOwnership (image : SimulationImage State) : Prop :=
  DescriptorSlotsValid image ∧
    ∀ (kind : NodeKind) (slot : Nat),
      slot < (image.stateArena kind).length ↔
        ∃ node,
          node ∈ image.nodes ∧
          node.kind = kind ∧
          node.stateSlot = slot ∧
          ∀ other,
            other ∈ image.nodes →
            other.kind = kind →
            other.stateSlot = slot →
            other = node

/--
Unique initial persistent event keys, matching validation at
`executor/src/validate.rs:1097-1115`.
-/
def UniqueEventKeys (events : List Event) : Prop :=
  (events.map Event.key).Nodup

/--
Initial events are stored in strict canonical-key order before executor construction, mirroring
validation at `executor/src/validate.rs:1097-1115`.
-/
def InitialEventsOrdered (image : SimulationImage State) : Prop :=
  image.initialEvents.Pairwise (fun left right => left.key < right.key)

/--
Unique directed-link identifiers required by checked Rust lookup at
`executor/src/scalar.rs:1365-1368`.
-/
def UniqueLinkIds (image : SimulationImage State) : Prop :=
  (image.links.map LinkDescriptor.id).Nodup

/--
Unique `(link, route-selected target)` channel declarations, mirroring duplicate rejection at
`executor/src/validate.rs:1047-1051`.
-/
def UniqueChannelRoutes (image : SimulationImage State) : Prop :=
  (image.channels.map fun channel => (channel.link, channel.target)).Nodup

/--
Every initial event targets a declared LP, matching validation at
`executor/src/validate.rs:1123-1140`.
-/
def InitialTargetsDeclared (image : SimulationImage State) : Prop :=
  ∀ event ∈ image.initialEvents, ∃ node ∈ image.nodes, node.id = event.target

/--
Initial event phases and origin LPs are canonical, mirroring validation at
`executor/src/validate.rs:1117-1140`.
-/
def InitialKeysCanonical (image : SimulationImage State) : Prop :=
  ∀ event ∈ image.initialEvents,
    event.key.phase = eventPhase event.kind ∧
    ∃ origin ∈ image.nodes, origin.id = event.key.originNode

/--
The immutable descriptor oracle is keyed by payload and agrees with the byte-size function used by
link timing. This represents the descriptors created at `executor/src/scalar.rs:760-767` and
transported at `executor/src/cpu.rs:664-667`.
-/
def DescriptorOracleWellFormed (image : SimulationImage State) : Prop :=
  ∀ payload,
    (image.packetDescriptor payload).id = payload ∧
      (image.packetDescriptor payload).sizeBytes = image.payloadBytes payload

/--
Prepared initial descriptor stores contain unique oracle values and cover every initial event at
its target LP, matching ownership derivation at `executor/src/cpu.rs:3034-3199`.
-/
def InitialPacketStoresWellFormed (image : SimulationImage State) : Prop :=
  (∀ node ∈ image.nodes,
    ((image.initialPacketStore node.id).map PacketDescriptor.id).Nodup ∧
      ∀ descriptor ∈ image.initialPacketStore node.id,
        descriptor = image.packetDescriptor descriptor.id) ∧
    ∀ event ∈ image.initialEvents,
      ∃ node ∈ image.nodes,
        event.target = node.id ∧
          image.packetDescriptor event.payload ∈ image.initialPacketStore node.id

/--
Every origin's prepared cursor lies above all of its initial event sequences, matching validation
at `executor/src/validate.rs:1876-1927`.
-/
def InitialOriginSequencesReserved (image : SimulationImage State) : Prop :=
  ∀ node ∈ image.nodes, ∀ event ∈ image.initialEvents,
    event.key.originNode = node.id →
    event.key.originSeq < image.initialNextOriginSeq node.id

/--
Every directed link has a positive serialization rate, mirroring the checked error at
`executor/src/time.rs:41-44`.
-/
def PositiveLinkRates (image : SimulationImage State) : Prop :=
  ∀ link ∈ image.links, 0 < link.rateBps

/--
Every directed link endpoint names a declared LP, mirroring link validation at
`executor/src/validate.rs:240-260`.
-/
def DeclaredLinkEndpoints (image : SimulationImage State) : Prop :=
  ∀ link ∈ image.links,
    (∃ source ∈ image.nodes, source.id = link.source) ∧
    (∃ target ∈ image.nodes, target.id = link.physicalTarget)

/--
Every channel used by a safe-horizon backend has positive certified lookahead. Load-time validation
rejects zero specifically for parallel backends at `executor/src/validate.rs:1059-1062`; the
single-threaded safe-horizon constructor independently rejects it at
`executor/src/safe_horizon.rs:200-207` (the CPU constructors do likewise at
`executor/src/cpu.rs:247-255,366-374,462-470`).
-/
def PositiveChannelBounds (image : SimulationImage State) : Prop :=
  ∀ channel ∈ image.channels, 0 < channel.minDelayNs

/--
Every channel endpoint names a declared LP, mirroring target/source validation at
`executor/src/validate.rs:1023-1039`.
-/
def DeclaredChannelEndpoints (image : SimulationImage State) : Prop :=
  ∀ channel ∈ image.channels,
    (∃ source ∈ image.nodes, source.id = channel.source) ∧
    (∃ target ∈ image.nodes, target.id = channel.target)

/--
Channel/link source consistency mirroring `executor/src/validate.rs:1015-1045`.

The channel target is intentionally not equated with `LinkDescriptor.physicalTarget`: after the
port-LP split, Rust permits a route-selected egress LP behind the physical receiving node
(`executor/src/image.rs:188-192`).
-/
def ChannelsReferenceDirectedLinks (image : SimulationImage State) : Prop :=
  ∀ channel ∈ image.channels,
    ∃ link ∈ image.links,
      link.id = channel.link ∧
      link.source = channel.source ∧
      channel.eventKind = .remoteArrival

/--
Static accepted-image assumptions checked independently of transition bodies, mirroring the
load-time checks around `executor/src/validate.rs:196-245,1010-1080`.
-/
def StaticImageWellFormed (image : SimulationImage State) : Prop :=
  UniqueNodeIds image ∧
    ExactStateOwnership image ∧
    UniqueEventKeys image.initialEvents ∧
    InitialEventsOrdered image ∧
    UniqueLinkIds image ∧
    UniqueChannelRoutes image ∧
    InitialTargetsDeclared image ∧
    InitialKeysCanonical image ∧
    DescriptorOracleWellFormed image ∧
    InitialPacketStoresWellFormed image ∧
    InitialOriginSequencesReserved image ∧
    PositiveLinkRates image ∧
    DeclaredLinkEndpoints image ∧
    PositiveChannelBounds image ∧
    DeclaredChannelEndpoints image ∧
    ChannelsReferenceDirectedLinks image

end DaysExecutor
