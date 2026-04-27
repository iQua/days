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
  redRandMaxPpb : Option Nat
  redRandMinPpb : Option Nat
  deriving Repr

structure Global where
  deriving Repr

def ecnMarkAllowed (before after : String) : Bool :=
  if before == "not_ect" && after == "ce" then false else true

def ppbDenom : Nat := 1000000000

def thresholdCap (ppb capacity : Nat) : Nat :=
  (ppb * capacity) / ppbDenom

def redThresholdCap (ppb capacity : Nat) : Nat :=
  thresholdCap ppb capacity

def queueOverflow (e : Event) : Bool :=
  if e.capacity == 0 then
    false
  else
    match e.capacityUnit with
    | CapacityUnit.bytes => e.byteLength + e.sizeBytes > e.capacity
    | CapacityUnit.packets => e.queueLength + 1 > e.capacity

def exceedsThreshold (e : Event) (ppb : Nat) : Bool :=
  if e.capacity == 0 then
    false
  else
    match e.capacityUnit with
    | CapacityUnit.bytes => e.byteLength + e.sizeBytes > thresholdCap ppb e.capacity
    | CapacityUnit.packets => e.queueLength + 1 > thresholdCap ppb e.capacity

def redProbPpb (e : Event) (minPpb maxPpb maxProbPpb avg : Nat) : Nat :=
  if maxPpb <= minPpb then
    0
  else
    let minCap := thresholdCap minPpb e.capacity
    let diff := if avg > minCap then avg - minCap else 0
    diff * e.capacity * maxProbPpb / (maxPpb - minPpb)

def redDecisionOk (e : Event) : Bool :=
  match e.redMinThresholdPpb, e.redMaxThresholdPpb, e.redAvgQueueLength with
  | some minPpb, some maxPpb, some avg =>
      let overMax := avg >= redThresholdCap maxPpb e.capacity
      let overMin := avg >= redThresholdCap minPpb e.capacity
      let minProbPpb :=
        match e.redMaxProbabilityPpb with
        | some maxProbPpb => redProbPpb e minPpb maxPpb maxProbPpb avg
        | none => 0
      let maxProbOk := overMax || e.redMaxProbabilityPpb.isSome
      let minRandOk := if overMin && !overMax then e.redRandMinPpb.isSome else true
      let minHit :=
        match e.redRandMinPpb with
        | some r => r <= minProbPpb
        | none => false
      let overflow := queueOverflow e
      let shouldMark := overMax || (overMin && minHit)
      if !maxProbOk || !minRandOk then
        false
      else if overflow then
        e.action = Action.drop
      else
        match e.strategy with
        | Strategy.red =>
            if shouldMark then e.action = Action.drop else e.action = Action.enqueue
        | Strategy.redEcn =>
            if shouldMark then
              (e.action = Action.markEcn || (e.action = Action.drop && e.ecnBefore == "not_ect"))
            else
              e.action = Action.enqueue
        | _ => false
  | _, _, _ => false

def step (lineNo : Nat) (g : Global) (e : Event) : Except String Global := do
  LeanGuard.Shared.require lineNo (ecnMarkAllowed e.ecnBefore e.ecnAfter) "invalid ECN mark"
  match e.strategy with
  | Strategy.red | Strategy.redEcn =>
      LeanGuard.Shared.require lineNo (redDecisionOk e) "missing RED witness"
  | Strategy.tailDrop =>
      let overflow := queueOverflow e
      let ok :=
        if overflow then
          e.action = Action.drop
        else
          e.action = Action.enqueue
      LeanGuard.Shared.require lineNo ok "invalid TailDrop decision"
  | Strategy.ecnThreshold =>
      let overflow := queueOverflow e
      let threshVal := e.ecnThresholdPpb
      let thresh :=
        match threshVal with
        | some t => exceedsThreshold e t
        | none => false
      let ok :=
        if threshVal.isNone then
          false
        else if overflow then
          e.action = Action.drop
        else if thresh then
          (e.action = Action.markEcn || (e.action = Action.drop && e.ecnBefore == "not_ect"))
        else
          e.action = Action.enqueue
      LeanGuard.Shared.require lineNo ok "invalid ECN threshold decision"
  pure g

end LeanGuard.Aqm.Semantics
