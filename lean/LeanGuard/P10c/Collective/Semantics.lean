import Std

namespace LeanGuard.P10c.Collective

/-- Exact upper bounds of the Rust integers represented by the certificate. -/
def maxU16 : Nat := 2 ^ 16 - 1

def maxU32 : Nat := 2 ^ 32 - 1

def maxU64 : Nat := 2 ^ 64 - 1

inductive Algorithm
  | allGather
  | ringAllReduce
  deriving DecidableEq, Repr

inductive Phase
  | reduceScatter
  | allGather
  deriving DecidableEq, Repr

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
  deriving DecidableEq, Repr

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
    | .ringAllReduce => true

/-- A root stage has no collective predecessor: step one of the algorithm's first phase. -/
def rootPosition (algorithm : Algorithm) (phase : Phase) (step : Nat) : Bool :=
  step = 1 &&
    match algorithm, phase with
    | .allGather, .allGather => true
    | .ringAllReduce, .reduceScatter => true
    | _, _ => false

/-- Non-root stages each rank runs: the ring's `2n - 3` or AllGather's `n - 2`. -/
def nonRootStagesPerRank (algorithm : Algorithm) (groupSize : Nat) : Nat :=
  match algorithm with
  | .allGather => groupSize - 2
  | .ringAllReduce => 2 * groupSize - 3

/-- Stages a complete TCP trace releases. TCP collectives carry at least one byte per rank, so
every chunk is nonempty; a compute-gated collective also releases its `n` roots. -/
def expectedActivationCount (algorithm : Algorithm) (groupSize : Nat) (gated : Bool) : Nat :=
  groupSize * nonRootStagesPerRank algorithm groupSize + (if gated then groupSize else 0)

/-- The largest initial congestion window of the executor's TCP controllers (Reno: two MSS). -/
def maxInitialWindowSegments : Nat := 2

/-- A released TCP stage fills its first window from sequence zero with full-MSS segments, the
last one trimmed to the chunk. The certificate omits the controller, so the window is bounded by
the largest initial window rather than fixed. -/
def tcpFirstWindow (mss chunkBytes packets bytes : Nat) : Bool :=
  packets ≥ 1 && packets ≤ maxInitialWindowSegments && (packets - 1) * mss < chunkBytes &&
    bytes = min chunkBytes (packets * mss)

/-- Status and deadline written when a compute interval is released at `timeNs`. -/
def computeTimerAfter (timeNs durationNs stopTimeNs : Nat) : Status × Nat :=
  let deadline := timeNs + durationNs
  (if deadline ≤ stopTimeNs then .scheduled else .stopped, deadline)

end LeanGuard.P10c.Collective
