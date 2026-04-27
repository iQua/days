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

abbrev RedKey := Nat × Nat

structure Global where
  redCounts : Std.HashMap RedKey Nat := {}
  deriving Repr

def isEcnCapable (ecn : String) : Bool :=
  ecn == "ect0" || ecn == "ect1" || ecn == "ce"

def isValidEcn (ecn : String) : Bool :=
  ecn == "not_ect" || isEcnCapable ecn

def validEcnTransition (action : Action) (before after : String) : Bool :=
  isValidEcn before && isValidEcn after &&
    match action with
    | Action.markEcn => isEcnCapable before && after == "ce"
    | Action.enqueue | Action.drop => before == after

def ppbDenom : Nat := 1000000000

def thresholdCap (ppb capacity : Nat) : Nat :=
  (ppb * capacity) / ppbDenom

def mulDiv (a b c : Nat) : Nat :=
  (a * b) / c

def redThresholdUnits (ppb capacity : Nat) : Nat :=
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
  let minTh := redThresholdUnits minPpb e.capacity
  let maxTh := redThresholdUnits maxPpb e.capacity
  if maxTh <= minTh then
    0
  else
    let diff := if avg > minTh then avg - minTh else 0
    mulDiv maxProbPpb diff (maxTh - minTh)

def redPaPpb (pbPpb count : Nat) : Nat :=
  let scaled := count * pbPpb
  if scaled >= ppbDenom then
    ppbDenom
  else
    let raw := (pbPpb * ppbDenom) / (ppbDenom - scaled)
    if raw > ppbDenom then ppbDenom else raw

def redSignalActionOk (e : Event) : Bool :=
  match e.strategy with
  | Strategy.red => e.action = Action.drop
  | Strategy.redEcn =>
      (e.action = Action.markEcn && isEcnCapable e.ecnBefore) ||
        (e.action = Action.drop && e.ecnBefore == "not_ect")
  | _ => false

def redDecisionNextCount (e : Event) (prevCount : Option Nat) : Option (Option Nat) :=
  if queueOverflow e then
    if e.action = Action.drop then some prevCount else none
  else
    match e.redMinThresholdPpb, e.redMaxThresholdPpb, e.redAvgQueueLength with
    | some minPpb, some maxPpb, some avg =>
        let minTh := redThresholdUnits minPpb e.capacity
        let maxTh := redThresholdUnits maxPpb e.capacity
        let thresholdsOk := minTh < maxTh
        let overMax := thresholdsOk && avg >= maxTh
        let overMin := thresholdsOk && avg >= minTh
        if !thresholdsOk then
          none
        else if overMax then
          if redSignalActionOk e then some (some 0) else none
        else if !overMin then
          if e.action = Action.enqueue then some none else none
        else
          match e.redMaxProbabilityPpb, e.redRandMinPpb with
          | some maxProbPpb, some r =>
              let count := match prevCount with | some c => c + 1 | none => 0
              let pbPpb := redProbPpb e minPpb maxPpb maxProbPpb avg
              let paPpb := redPaPpb pbPpb count
              if r <= paPpb then
                if redSignalActionOk e then some (some 0) else none
              else
                if e.action = Action.enqueue then some (some count) else none
          | _, _ => none
    | _, _, _ => none

def redKey (e : Event) : RedKey :=
  (e.schedulerId, e.queueId)

def updateRedCount (g : Global) (key : RedKey) (next : Option Nat) : Global :=
  match next with
  | some count => { g with redCounts := g.redCounts.insert key count }
  | none => { g with redCounts := g.redCounts.erase key }

def step (lineNo : Nat) (g : Global) (e : Event) : Except String Global := do
  let ecnError :=
    match e.action with
    | Action.markEcn => "invalid ECN mark"
    | _ => "invalid ECN transition"
  LeanGuard.Shared.require lineNo (validEcnTransition e.action e.ecnBefore e.ecnAfter) ecnError
  match e.strategy with
  | Strategy.red | Strategy.redEcn =>
      let key := redKey e
      let next ←
        match redDecisionNextCount e (g.redCounts.get? key) with
        | some next => pure next
        | none => throw s!"line {lineNo}: missing RED witness"
      pure (updateRedCount g key next)
  | Strategy.tailDrop =>
      let overflow := queueOverflow e
      let ok :=
        if overflow then
          e.action = Action.drop
        else
          e.action = Action.enqueue
      LeanGuard.Shared.require lineNo ok "invalid TailDrop decision"
      pure g
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
