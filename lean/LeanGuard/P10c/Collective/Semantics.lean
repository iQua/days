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

/-- Roots are scheduled while the image is built; only dependency-unblocked stages are logged. -/
def loggedActivationPosition (algorithm : Algorithm) (phase : Phase) (step : Nat) : Bool :=
  match algorithm, phase with
  | .allGather, .allGather => step > 1
  | .ringAllReduce, .reduceScatter => step > 1
  | .ringAllReduce, .allGather => true
  | .allGather, .reduceScatter => false

/-- EqualRemainderLast has one nonempty owner below one byte per rank, otherwise all owners. -/
def nonzeroOwnerCount (totalBytes groupSize : Nat) : Nat :=
  if totalBytes < groupSize then 1 else groupSize

/-- Number of nonempty dependency-unblocked stages in a complete scalar progress trace. -/
def expectedActivationCount (algorithm : Algorithm) (groupSize totalBytes : Nat) : Nat :=
  let stagesPerOwner :=
    match algorithm with
    | .allGather => groupSize - 2
    | .ringAllReduce => 2 * groupSize - 3
  nonzeroOwnerCount totalBytes groupSize * stagesPerOwner

/-- Stages needing progress have a nonempty own chunk or nonempty local predecessor chunk. -/
def expectedProgressStageCount (algorithm : Algorithm) (groupSize totalBytes : Nat) : Nat :=
  let stagesPerOwner :=
    match algorithm with
    | .allGather => groupSize - 2
    | .ringAllReduce => 2 * groupSize - 3
  let progressingOwners := if totalBytes < groupSize then 2 else groupSize
  progressingOwners * stagesPerOwner

def firstPacketBytes (packetSize chunkBytes : Nat) : Nat :=
  min packetSize chunkBytes

structure ActivationAfter where
  packetsEmitted : Nat
  bytesEmitted : Nat
  status : Status
  nextTimeNs : Nat
  deriving DecidableEq, Repr

/-- State written by the scalar executor after a blocked stage emits its first packet. -/
def activationAfter
    (timeNs packetSize chunkBytes intervalNs stopTimeNs : Nat) : ActivationAfter :=
  let first := firstPacketBytes packetSize chunkBytes
  let remaining := chunkBytes - first
  if remaining = 0 then
    { packetsEmitted := 1
      bytesEmitted := first
      status := .finished
      nextTimeNs := timeNs }
  else
    let next := timeNs + intervalNs
    { packetsEmitted := 1
      bytesEmitted := first
      status := if next ≤ stopTimeNs then .scheduled else .stopped
      nextTimeNs := next }

end LeanGuard.P10c.Collective
