import Std

import LeanGuard.Shared.Check

namespace LeanGuard.P10c.Semantics

open LeanGuard.Shared

namespace Aqm

def averageScale : Nat := 2 ^ 32

inductive DepthUnit
  | packets
  | bytes
  deriving DecidableEq, Repr

inductive Action
  | enqueue
  | mark
  | drop
  deriving DecidableEq, Repr

structure ThresholdConfig where
  unit : DepthUnit
  capacity : Nat
  threshold : Nat
  deriving DecidableEq, Repr

structure RedState where
  unit : DepthUnit
  capacity : Nat
  minThreshold : Nat
  maxThreshold : Nat
  maxProbabilityNumerator : Nat
  maxProbabilityDenominator : Nat
  averageScaled : Nat
  counter : Nat
  markEcn : Bool
  deriving DecidableEq, Repr

def postDepth
    (unit : DepthUnit)
    (queuedPackets queuedBytes packetSizeBytes : Nat) : Nat :=
  match unit with
  | .packets => queuedPackets + 1
  | .bytes => queuedBytes + packetSizeBytes

def thresholdDecision
    (config : ThresholdConfig)
    (queuedPackets queuedBytes packetSizeBytes : Nat) : Action :=
  let depth := postDepth config.unit queuedPackets queuedBytes packetSizeBytes
  if config.capacity ≠ 0 && config.capacity < depth then
    .drop
  else if config.threshold ≤ depth then
    .mark
  else
    .enqueue

def redDecision
    (state : RedState)
    (queuedPackets queuedBytes packetSizeBytes : Nat) : RedState × Action :=
  let sample :=
    match state.unit with
    | .packets => queuedPackets
    | .bytes => queuedBytes
  let depth := postDepth state.unit queuedPackets queuedBytes packetSizeBytes
  let average := (state.averageScaled * 511 + sample * averageScale) / 512
  let state := { state with averageScaled := average }
  if state.capacity ≠ 0 && state.capacity < depth then
    (state, .drop)
  else
    let minimum := state.minThreshold * averageScale
    let maximum := state.maxThreshold * averageScale
    if average ≤ minimum then
      ({ state with counter := 0 }, .enqueue)
    else if maximum ≤ average then
      ({ state with counter := 0 }, if state.markEcn then .mark else .drop)
    else
      let counter := state.counter + 1
      let left :=
        counter * state.maxProbabilityNumerator * (average - minimum)
      let right :=
        state.maxProbabilityDenominator *
          (state.maxThreshold - state.minThreshold) * averageScale
      if right ≤ left then
        ({ state with counter := 0 }, if state.markEcn then .mark else .drop)
      else
        ({ state with counter }, .enqueue)

end Aqm

namespace Rate

inductive Status
  | scheduled
  | blocked
  | finished
  | stopped
  deriving DecidableEq, Repr

structure State where
  rateNumeratorBitsPerSecond : Nat
  rateDenominator : Nat
  pacingIntervalNs : Nat
  packetSizeBytes : Nat
  totalBytes : Nat
  emittedBytes : Nat
  creditQuanta : Nat
  deriving DecidableEq, Repr

structure TickResult where
  state : State
  emittedBytes : Nat
  nextPacketBytes : Nat
  nextStatus : Status
  nextTimeNs : Option Nat
  deriving DecidableEq, Repr

def packetCost (state : State) (sizeBytes : Nat) : Nat :=
  sizeBytes * 8 * state.rateDenominator * 1_000_000_000

def tickCredit (state : State) : Nat :=
  state.rateNumeratorBitsPerSecond * state.pacingIntervalNs

def tick
    (state : State)
    (currentPacketBytes timeNs stopTimeNs : Nat) : TickResult :=
  let credit := state.creditQuanta + tickCredit state
  let emitted := if packetCost state currentPacketBytes ≤ credit then currentPacketBytes else 0
  let credit := if emitted = 0 then credit else credit - packetCost state emitted
  let emittedTotal := state.emittedBytes + emitted
  let state := { state with emittedBytes := emittedTotal, creditQuanta := credit }
  if state.totalBytes ≤ emittedTotal then
    { state
      emittedBytes := emitted
      nextPacketBytes := 0
      nextStatus := .finished
      nextTimeNs := none }
  else
    let nextBytes := min state.packetSizeBytes (state.totalBytes - emittedTotal)
    let nextTime := timeNs + state.pacingIntervalNs
    if stopTimeNs < nextTime then
      { state
        emittedBytes := emitted
        nextPacketBytes := nextBytes
        nextStatus := .stopped
        nextTimeNs := some nextTime }
    else
      let nextCredit := credit + tickCredit state
      { state
        emittedBytes := emitted
        nextPacketBytes := nextBytes
        nextStatus :=
          if packetCost state nextBytes ≤ nextCredit then .scheduled else .blocked
        nextTimeNs := some nextTime }

end Rate

namespace Pfc

structure ThresholdState where
  xonBytes : Nat
  xoffBytes : Nat
  asserted : Bool
  deriving DecidableEq, Repr

inductive Control
  | pause
  | resume
  deriving DecidableEq, Repr

def occupancyTransition (state : ThresholdState) (occupancyBytes : Nat) :
    ThresholdState × Option Control :=
  if !state.asserted && state.xoffBytes ≤ occupancyBytes then
    ({ state with asserted := true }, some .pause)
  else if state.asserted && occupancyBytes ≤ state.xonBytes then
    ({ state with asserted := false }, some .resume)
  else
    (state, none)

abbrev PauseMask := Std.HashMap Nat Bool

def applyControl (mask : PauseMask) (priority : Nat) : Control → PauseMask
  | .pause => mask.insert priority true
  | .resume => mask.insert priority false

def eligible (mask : PauseMask) (priority : Nat) : Bool :=
  !(mask.getD priority false)

end Pfc

structure Packet where
  id : Nat
  flow : Nat
  sizeBytes : Nat
  deriving DecidableEq, Repr

def packetClass (packet : Packet) (classCount : Nat) : Nat :=
  packet.flow % classCount

namespace Drr

structure State where
  classCount : Nat
  quanta : Std.HashMap Nat Nat
  deficits : Std.HashMap Nat Nat := ∅
  queues : Std.HashMap Nat (List Packet) := ∅
  currentClass : Nat := 0
  waiting : Nat := 0
  deriving Repr

def queue (state : State) (classId : Nat) : List Packet :=
  state.queues.getD classId []

def deficit (state : State) (classId : Nat) : Nat :=
  state.deficits.getD classId 0

def enqueue (state : State) (packet : Packet) : State :=
  let classId := packetClass packet state.classCount
  { state with
    queues := state.queues.insert classId (queue state classId ++ [packet])
    waiting := state.waiting + 1 }

def addRoundQuanta (state : State) : State :=
  let classes := List.range state.classCount
  classes.foldl
    (fun state classId =>
      if (queue state classId).isEmpty then
        { state with deficits := state.deficits.insert classId 0 }
      else
        let quantum := state.quanta.getD classId 0
        { state with
          deficits := state.deficits.insert classId (deficit state classId + quantum) })
    state

def advance (state : State) : State :=
  let next := state.currentClass + 1
  if next < state.classCount then
    { state with currentClass := next }
  else
    addRoundQuanta { state with currentClass := 0 }

def headEligible (state : State) : Bool :=
  match queue state state.currentClass with
  | [] => false
  | packet :: _ => packet.sizeBytes ≤ deficit state state.currentClass

def scanIneligible (lineNo : Nat) (state : State) : Except String State := do
  require lineNo (!headEligible state) "DRR scan skipped an eligible head"
  pure (advance state)

def scan (lineNo steps : Nat) (state : State) : Except String State :=
  match steps with
  | 0 => pure state
  | steps + 1 => do
      let state ← scanIneligible lineNo state
      scan lineNo steps state

def schedule (lineNo scanSteps : Nat) (state : State) : Except String (State × Packet) := do
  require lineNo (state.classCount > 0) "class_count must be > 0"
  require lineNo (state.waiting > 0) "schedule on empty DRR"
  let state ← scan lineNo scanSteps state
  let classId := state.currentClass
  match queue state classId with
  | [] => throw s!"line {lineNo}: DRR scan ended on an empty class"
  | packet :: rest => do
      let available := deficit state classId
      require lineNo (packet.sizeBytes ≤ available) "DRR head exceeds deficit"
      let state :=
        { state with
          deficits := state.deficits.insert classId (available - packet.sizeBytes)
          queues := state.queues.insert classId rest
          waiting := state.waiting - 1 }
      pure (state, packet)

end Drr

namespace Wrr

structure State where
  classCount : Nat
  weights : Std.HashMap Nat Nat
  sent : Std.HashMap Nat Nat := ∅
  queues : Std.HashMap Nat (List Packet) := ∅
  currentClass : Nat := 0
  waiting : Nat := 0
  deriving Repr

def queue (state : State) (classId : Nat) : List Packet :=
  state.queues.getD classId []

def enqueue (state : State) (packet : Packet) : State :=
  let classId := packetClass packet state.classCount
  { state with
    queues := state.queues.insert classId (queue state classId ++ [packet])
    waiting := state.waiting + 1 }

def advance (state : State) : State :=
  let classId := state.currentClass
  { state with
    sent := state.sent.insert classId 0
    currentClass := (classId + 1) % state.classCount }

def scheduleLoop (lineNo : Nat) : Nat → State → Except String (State × Packet)
  | 0, _ => throw s!"line {lineNo}: WRR scan exhausted"
  | fuel + 1, state => do
      let classId := state.currentClass
      let weight ←
        match state.weights.get? classId with
        | none => throw s!"line {lineNo}: missing WRR weight for class {classId}"
        | some weight => pure weight
      require lineNo (weight > 0) s!"WRR weight must be > 0 for class {classId}"
      match queue state classId with
      | packet :: rest =>
          let count := state.sent.getD classId 0
          if count < weight then
            pure
              ({ state with
                  sent := state.sent.insert classId (count + 1)
                  queues := state.queues.insert classId rest
                  waiting := state.waiting - 1 },
                packet)
          else
            scheduleLoop lineNo fuel (advance state)
      | [] => scheduleLoop lineNo fuel (advance state)

def schedule (lineNo : Nat) (state : State) : Except String (State × Packet) := do
  require lineNo (state.classCount > 0) "class_count must be > 0"
  require lineNo (state.waiting > 0) "schedule on empty WRR"
  scheduleLoop lineNo (state.classCount + 1) state

end Wrr

end LeanGuard.P10c.Semantics
