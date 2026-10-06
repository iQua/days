import Std
import LeanGuard.P10c.Roce.Semantics

namespace LeanGuard.P10c.Collective

/-- Exact upper bounds of the Rust integers represented by the certificate. -/
def maxU16 : Nat := 2 ^ 16 - 1

def maxU32 : Nat := 2 ^ 32 - 1

def maxU64 : Nat := 2 ^ 64 - 1

inductive Algorithm
  | allGather
  | ringAllReduce
  /-- P16 H1: one ring phase. -/
  | reduceScatter
  /-- P16 H1: one message per ordered pair of ranks, all independent. -/
  | allToAll
  /-- P16 H1: one message from rank 0 to rank 1. -/
  | sendRecv
  deriving DecidableEq, Repr

inductive Phase
  | reduceScatter
  | allGather
  /-- An all-to-all's one phase; `step` is the offset `k` of the destination `rank + k`. -/
  | allToAll
  | sendRecv
  deriving DecidableEq, Repr, Hashable

/-- How a collective's declared total becomes its messages (P16 H1). -/
inductive ChunkPolicy
  | equalRemainderLast
  /-- Every message `floor(floor(S / n) / c)` bytes over `c` channels; an all-to-all's
  `floor(S / n)`. -/
  | uniformFloor
  /-- A seeded routing matrix's per-pair bytes (not re-derived here). -/
  | seeded
  deriving DecidableEq, Repr

/-- Which messages a collective sends (P16 H1). -/
inductive ChannelPolicy
  | ringNext
  | channels
  | allPairs
  | pair
  deriving DecidableEq, Repr

/-- Whether an algorithm runs on rings. -/
def Algorithm.isRing : Algorithm → Bool
  | .allGather | .ringAllReduce | .reduceScatter => true
  | .allToAll | .sendRecv => false

/-- The phase a collective's stages start in, and the one they end in. -/
def Algorithm.firstPhase : Algorithm → Phase
  | .ringAllReduce | .reduceScatter => .reduceScatter
  | .allGather => .allGather
  | .allToAll => .allToAll
  | .sendRecv => .sendRecv

def Algorithm.lastPhase : Algorithm → Phase
  | .ringAllReduce | .allGather => .allGather
  | .reduceScatter => .reduceScatter
  | .allToAll => .allToAll
  | .sendRecv => .sendRecv

/-- Ring phases per channel. -/
def Algorithm.phases : Algorithm → Nat
  | .ringAllReduce => 2
  | _ => 1

inductive Cause
  | localCompletion
  | inboundArrival
  deriving DecidableEq, Repr

inductive Status
  | blocked
  | scheduled
  | finished
  | stopped
  deriving DecidableEq, Repr

/-- The generator that carries a dependency-gated stage. -/
inductive StageKind
  /-- A collective stage carried by an ordinary TCP flow. -/
  | tcp
  /-- A delay-only compute interval. -/
  | compute
  /-- A collective stage carried by a RoCE queue pair (schema Amendment 4). -/
  | roce
  deriving DecidableEq, Repr, Hashable

/-- A stage that moves bytes over a reliable transport, TCP or a RoCE queue pair. -/
def StageKind.isTransport : StageKind → Bool
  | .tcp | .roce => true
  | .compute => false

/-- The owner offset in the lowering recurrence. -/
def ownerOffset : Algorithm → Phase → Nat
  | .ringAllReduce, .allGather => 2
  | _, _ => 1

/-- Owner of the chunk propagated by a collective stage. -/
def stageOwner (algorithm : Algorithm) (phase : Phase) (groupSize rank step : Nat) : Nat :=
  (rank + groupSize - step + ownerOffset algorithm phase) % groupSize

/-- EqualRemainderLast partition bounds for one owner. -/
def chunkBounds (totalBytes groupSize owner : Nat) : Nat × Nat :=
  let base := totalBytes / groupSize
  let offset := owner * base
  let bytes := if owner + 1 = groupSize then totalBytes - offset else base
  (offset, bytes)

def legalPosition
    (algorithm : Algorithm) (phase : Phase) (groupSize rank step : Nat) : Bool :=
  groupSize ≥ 2 && rank < groupSize && step > 0 && step < groupSize &&
    match algorithm with
    | .allGather => phase = .allGather
    | .ringAllReduce => phase = .reduceScatter || phase = .allGather
    | .reduceScatter => phase = .reduceScatter
    | .allToAll => phase = .allToAll
    | .sendRecv => phase = .sendRecv && groupSize = 2 && rank = 0 && step = 1

/-- A root stage has no collective predecessor: step one of the algorithm's first phase. -/
def rootPosition (algorithm : Algorithm) (phase : Phase) (step : Nat) : Bool :=
  match algorithm with
  | .allToAll | .sendRecv => true
  | _ => step = 1 && phase = algorithm.firstPhase

/-- Non-root stages each rank runs: the ring's `2n - 3` or AllGather's `n - 2`. -/
def nonRootStagesPerRank (algorithm : Algorithm) (groupSize : Nat) : Nat :=
  match algorithm with
  | .allGather | .reduceScatter => groupSize - 2
  | .ringAllReduce => 2 * groupSize - 3
  | .allToAll | .sendRecv => 0

/-- Stages a complete trace of a transport collective releases. TCP and RoCE collectives carry at
least one byte per rank, so every chunk is nonempty; a compute-gated collective also releases its
`n` roots. -/
def expectedActivationCount (algorithm : Algorithm) (groupSize : Nat) (gated : Bool) : Nat :=
  groupSize * nonRootStagesPerRank algorithm groupSize + (if gated then groupSize else 0)

/-- P16 H1: stages a complete trace releases over `channels` ring channels; an all-to-all's
`n (n - 1)` pairs and a send/recv's one message are all roots, logged when gated. -/
def expectedActivationCountOf (algorithm : Algorithm) (groupSize channels : Nat) (gated : Bool) :
    Nat :=
  match algorithm with
  | .allToAll => if gated then groupSize * (groupSize - 1) else 0
  | .sendRecv => if gated then 1 else 0
  | _ => channels * expectedActivationCount algorithm groupSize gated

/-- P16 H1: the bytes of one message under `UniformFloor`: `floor(floor(S / n) / c)` for a ring of
`c` channels, `floor(S / n)` for an all-to-all pair. -/
def uniformFloorBytes (algorithm : Algorithm) (totalBytes groupSize channels : Nat) : Nat :=
  match algorithm with
  | .allToAll => totalBytes / groupSize
  | _ => totalBytes / groupSize / channels

/-- The largest initial congestion window of the executor's TCP controllers (Reno: two MSS). -/
def maxInitialWindowSegments : Nat := 2

/-- A released TCP stage fills its first window from sequence zero with full-MSS segments, the
last one trimmed to the chunk. The certificate omits the controller, so the window is bounded by
the largest initial window rather than fixed. -/
def tcpFirstWindow (mss chunkBytes packets bytes : Nat) : Bool :=
  packets ≥ 1 && packets ≤ maxInitialWindowSegments && (packets - 1) * mss < chunkBytes &&
    bytes = min chunkBytes (packets * mss)

/-- Schema Amendment 4: a released RoCE stage arms its first pacing tick at the release instant,
on a grid anchored there (ruling C2), so the release itself sends nothing. Its status is the
pacer's armed-status prediction, `Scheduled` or `Blocked`, which depends on the controller's rate
that the collective certificate does not carry. -/
def roceRelease (timeNs : Nat) (status : Status) (nextTimeNs packets bytes : Nat) : Bool :=
  packets = 0 && bytes = 0 && nextTimeNs = timeNs &&
    (status = .scheduled || status = .blocked)

/-- The Go-back-N receiver's in-order frontier after a data packet `[psn, psn + bytes)`: it
advances by the packet exactly when the PSN is the frontier. A duplicate or an out-of-order
packet leaves it where it is, and nothing is buffered, so no later packet fills a hole. -/
def goBackNFrontier (frontier psn bytes : Nat) : Nat :=
  if psn = frontier then frontier + bytes else frontier

/-- `goBackNFrontier` is the frontier of the queue-pair receiver that the RoCE checker replays
(`Roce.onData`, `roce.rs` `receive`), whatever its configuration, state and the packet's CE mark. -/
theorem goBackNFrontier_onData (config : Roce.ReceiverConfig) (state : Roce.ReceiverState)
    (timeNs : Nat) (packet : Roce.DataArrival) :
    (Roce.onData config state timeNs packet).state.expectedPsn =
      goBackNFrontier state.expectedPsn packet.psn packet.bytes := by
  -- Sending feedback resets only the ACK cadence, never the frontier.
  unfold Roce.onData goBackNFrontier
  by_cases hin : packet.psn = state.expectedPsn
  · by_cases hack : state.packetsSinceAck + 1 ≥ config.ackEveryPackets ∨
        state.expectedPsn + packet.bytes = config.totalBytes <;>
      simp [hin, hack, Roce.Action.sendsFeedback]
  · by_cases hdup : packet.psn < state.expectedPsn
    · by_cases hdupAck : config.duplicateAck = true <;>
        simp [hin, hdup, hdupAck, Roce.Action.sendsFeedback]
    · by_cases hnack : Roce.nackAdmitted config state timeNs = true <;>
        simp [hin, hdup, hnack, Roce.Action.sendsFeedback]

/-- A data packet of a RoCE queue pair whose chunk is `total` bytes: its PSN is a packet boundary
(a multiple of the MTU) and its size is `min(mtu, total - psn)` (`Roce.packetSize`, §1). -/
def roceSegment (mtu total psn bytes : Nat) : Bool :=
  mtu > 0 && psn % mtu = 0 && psn < total && bytes = min mtu (total - psn)

/-- Status and deadline written when a compute interval is released at `timeNs`. -/
def computeTimerAfter (timeNs durationNs stopTimeNs : Nat) : Status × Nat :=
  let deadline := timeNs + durationNs
  (if deadline ≤ stopTimeNs then .scheduled else .stopped, deadline)

end LeanGuard.P10c.Collective
