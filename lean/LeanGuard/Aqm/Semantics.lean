import Std
import LeanGuard.Shared.Check

namespace LeanGuard.Aqm.Semantics

inductive CapacityUnit where
  | bytes
  | packets
  deriving DecidableEq, Repr

inductive Action where
  | enqueue
  | drop
  | markEcn
  deriving DecidableEq, Repr

inductive Strategy where
  | tailDrop
  | red
  | redEcn
  | ecnThreshold
  deriving DecidableEq, Repr

structure Event where
  timeNs : Nat
  eventId : Nat
  schedulerId : Nat
  queueId : Nat
  packetId : Nat
  flowId : Nat
  sizeBytes : Nat
  action : Action
  capacity : Nat
  capacityUnit : CapacityUnit
  queueLength : Nat
  byteLength : Nat
  ecnBefore : String
  ecnAfter : String
  strategy : Strategy
  ecnThresholdPpb : Option Nat
  redMinThresholdPpb : Option Nat
  redMaxThresholdPpb : Option Nat
  redMaxProbabilityPpb : Option Nat
  redAvgQueueLength : Option Nat
  redRandPpb : Option Nat
  deriving Repr

structure Global where
  deriving Repr

def ecnMarkAllowed (before after : String) : Bool :=
  if before == "not_ect" && after == "ce" then false else true

def redDecisionOk (e : Event) : Bool :=
  match e.redMinThresholdPpb, e.redMaxThresholdPpb, e.redMaxProbabilityPpb,
    e.redAvgQueueLength, e.redRandPpb with
  | some _, some _, some _, some _, some _ => true
  | _, _, _, _, _ => false

def step (lineNo : Nat) (g : Global) (e : Event) : Except String Global := do
  LeanGuard.Shared.require lineNo (ecnMarkAllowed e.ecnBefore e.ecnAfter) "invalid ECN mark"
  match e.strategy with
  | Strategy.red | Strategy.redEcn =>
      LeanGuard.Shared.require lineNo (redDecisionOk e) "missing RED witness"
  | _ => pure ()
  pure g

end LeanGuard.Aqm.Semantics
