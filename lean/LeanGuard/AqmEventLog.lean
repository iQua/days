import Std

import LeanGuard.Shared.Check
import LeanGuard.Shared.Csv
import LeanGuard.Shared.Key
import LeanGuard.Shared.Coverage
import LeanGuard.Aqm.Semantics

namespace LeanGuard.AqmEventLog

open LeanGuard.Shared
open LeanGuard.Aqm.Semantics

inductive Kind where
  | decision
  deriving DecidableEq, Repr

/-- 1:1 with a single row in `aqm_events.csv` emitted by Days under `--features lean`. -/
structure Row where
  timeNs : Nat
  eventId : Nat
  kind : Kind
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
  srcLine : Nat
  deriving DecidableEq, Repr

def key (r : Row) : Nat × Nat :=
  (r.timeNs, r.eventId)

def parseKind (s : String) : Except String Kind :=
  match s with
  | "decision" => pure Kind.decision
  | other => throw s!"invalid kind: {other}"

def parseAction (s : String) : Except String Action :=
  match s with
  | "enqueue" => pure Action.enqueue
  | "drop" => pure Action.drop
  | "mark_ecn" => pure Action.markEcn
  | other => throw s!"invalid action: {other}"

def parseStrategy (s : String) : Except String Strategy :=
  match s with
  | "tail_drop" => pure Strategy.tailDrop
  | "red" => pure Strategy.red
  | "red_ecn" => pure Strategy.redEcn
  | "ecn_threshold" => pure Strategy.ecnThreshold
  | other => throw s!"invalid drop_strategy: {other}"

def parseCapacityUnit (s : String) : Except String CapacityUnit :=
  match s with
  | "bytes" => pure CapacityUnit.bytes
  | "packets" => pure CapacityUnit.packets
  | other => throw s!"invalid capacity_unit: {other}"

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let res : Except String Row := do
    let timeNs ← parseNat (← getField idx fields "time_ns")
    let eventId ← parseNat (← getField idx fields "event_id")
    let kind ← parseKind (← getField idx fields "kind")
    let schedulerId ← parseNat (← getField idx fields "scheduler_id")
    let queueId ← parseNat (← getField idx fields "queue_id")
    let packetId ← parseNat (← getField idx fields "packet_id")
    let flowId ← parseNat (← getField idx fields "flow_id")
    let sizeBytes ← parseNat (← getField idx fields "size_bytes")
    let action ← parseAction (← getField idx fields "action")
    let capacity ← parseNat (← getField idx fields "capacity")
    let capacityUnit ← parseCapacityUnit (← getField idx fields "capacity_unit")
    let queueLength ← parseNat (← getField idx fields "queue_length")
    let byteLength ← parseNat (← getField idx fields "byte_length")
    let ecnBefore ← getField idx fields "ecn_before"
    let ecnAfter ← getField idx fields "ecn_after"
    let strategy ← parseStrategy (← getField idx fields "drop_strategy")
    let ecnThresholdPpb ← parseOpt parseNat (← getField idx fields "ecn_threshold_ppb")
    let redMinThresholdPpb ← parseOpt parseNat (← getField idx fields "red_min_threshold_ppb")
    let redMaxThresholdPpb ← parseOpt parseNat (← getField idx fields "red_max_threshold_ppb")
    let redMaxProbabilityPpb ← parseOpt parseNat (← getField idx fields "red_max_probability_ppb")
    let redAvgQueueLength ← parseOpt parseNat (← getField idx fields "red_avg_queue_length")
    let redRandMaxPpb ← parseOpt parseNat (← getField idx fields "red_rand_max_ppb")
    let redRandMinPpb ← parseOpt parseNat (← getField idx fields "red_rand_min_ppb")
    pure
      { timeNs
        eventId
        kind
        schedulerId
        queueId
        packetId
        flowId
        sizeBytes
        action
        capacity
        capacityUnit
        queueLength
        byteLength
        ecnBefore
        ecnAfter
        strategy
        ecnThresholdPpb
        redMinThresholdPpb
        redMaxThresholdPpb
        redMaxProbabilityPpb
        redAvgQueueLength
        redRandMaxPpb
        redRandMinPpb
        srcLine := lineNo }
  match res with
  | .ok r => pure r
  | .error e => throw s!"line {lineNo}: {e}"

def parseCsv (content : String) : Except String (List Row) := do
  let lines :=
    content.splitOn "\n" |>.map stripCR |>.map (fun l => l.trim) |>.filter (· != "")
  match lines with
  | [] => throw "empty CSV"
  | header :: data =>
      let idx := mkIndex (splitCsvLine header)
      let rec go (lineNo : Nat) (data : List String) (acc : List Row) : Except String (List Row) := do
        match data with
        | [] => pure acc.reverse
        | line :: rest => do
            let fields := splitCsvLine line |>.toArray
            let row ← parseRow lineNo idx fields
            go (lineNo + 1) rest (row :: acc)
      go 2 data []

def toEvent (r : Row) : Event :=
  { timeNs := r.timeNs
    eventId := r.eventId
    schedulerId := r.schedulerId
    queueId := r.queueId
    packetId := r.packetId
    flowId := r.flowId
    sizeBytes := r.sizeBytes
    action := r.action
    capacity := r.capacity
    capacityUnit := r.capacityUnit
    queueLength := r.queueLength
    byteLength := r.byteLength
    ecnBefore := r.ecnBefore
    ecnAfter := r.ecnAfter
    strategy := r.strategy
    ecnThresholdPpb := r.ecnThresholdPpb
    redMinThresholdPpb := r.redMinThresholdPpb
    redMaxThresholdPpb := r.redMaxThresholdPpb
    redMaxProbabilityPpb := r.redMaxProbabilityPpb
    redAvgQueueLength := r.redAvgQueueLength
    redRandMaxPpb := r.redRandMaxPpb
    redRandMinPpb := r.redRandMinPpb }

def recordRedSignalCover (cov : CoverageState) (e : Event) : CoverageState :=
  match e.strategy with
  | Strategy.red =>
      if e.action = Action.drop then
        covHit cov "red_signal_drop"
      else
        cov
  | Strategy.redEcn =>
      if e.action = Action.markEcn && isEcnCapable e.ecnBefore && e.ecnAfter == "ce" then
        covHit cov "red_ecn_signal_mark"
      else if e.action = Action.drop && e.ecnBefore == "not_ect" then
        covHit cov "red_ecn_signal_drop_not_ect"
      else
        cov
  | _ => cov

def recordRedCover (cov : CoverageState) (g : Global) (e : Event) : CoverageState :=
  if queueOverflow e then
    if e.action = Action.drop then
      covHit cov "red_overflow_drop"
    else
      cov
  else
    match e.redMinThresholdPpb, e.redMaxThresholdPpb, e.redAvgQueueLength with
    | some minPpb, some maxPpb, some avg =>
        let minTh := redThresholdUnits minPpb e.capacity
        let maxTh := redThresholdUnits maxPpb e.capacity
        if !(minTh < maxTh) then
          cov
        else if avg >= maxTh then
          recordRedSignalCover (covHit cov "red_avg_over_max") e
        else if avg < minTh then
          covHit cov "red_avg_under_min"
        else
          let cov := covHit cov "red_avg_between"
          match e.redMaxProbabilityPpb, e.redRandMinPpb with
          | some maxProbPpb, some r =>
              let prevCount := g.redCounts.get? (redKey e)
              let count := match prevCount with | some c => c + 1 | none => 0
              let pbPpb := redProbPpb e minPpb maxPpb maxProbPpb avg
              let paPpb := redPaPpb pbPpb count
              if r <= paPpb then
                recordRedSignalCover (covHit cov "red_probability_hit") e
              else
                covHit cov "red_probability_miss"
          | _, _ => cov
    | _, _, _ => cov

def recordCover (cov : CoverageState) (g : Global) (r : Row) : CoverageState :=
  let e := toEvent r
  match e.strategy with
  | Strategy.tailDrop =>
      let overflow := queueOverflow e
      if overflow then
        covHit cov "taildrop_overflow_drop"
      else
        covHit cov "taildrop_enqueue"
  | Strategy.ecnThreshold =>
      match e.ecnThresholdPpb with
      | none => cov
      | some t =>
          let overflow := queueOverflow e
          let thresh := exceedsThreshold e t
          let cov :=
            if overflow then
              covHit cov "ecn_threshold_drop_overflow"
            else if thresh then
              covHit cov "ecn_threshold_mark"
            else
              covHit cov "ecn_threshold_pass"
          if (!overflow) && thresh && e.action = Action.drop && e.ecnBefore == "not_ect" then
            covHit cov "mark_non_ecn_packet_drop"
          else
            cov
    | Strategy.red | Strategy.redEcn =>
        recordRedCover cov g e

def checkRowsWithCoverage (rows : List Row) : CheckOutcome := do
  let rowsSorted ←
    match canonicalizeRows rows key (fun r => r.srcLine) with
    | .ok rs => pure rs
    | .error e => throw (e, {})

  let rec go (g : Global) (prevKey : Option (Nat × Nat)) (rows : List Row)
      (cov : CoverageState) : CheckOutcome := do
    match rows with
    | [] => pure cov
    | r :: rs => do
        let cov := covTick cov
        let cov := recordCover cov g r
        match prevKey with
        | none => pure ()
        | some pk =>
            match require r.srcLine (keyLt pk (key r)) "global key went backwards" with
            | .ok _ => pure ()
            | .error e => throw (e, cov)
        let g' ←
          match step r.srcLine g (toEvent r) with
          | .ok g' => pure g'
          | .error e => throw (e, cov)
        go g' (some (key r)) rs cov
  go {} none rowsSorted {}

def checkRows (rows : List Row) : Except String Unit := do
  match checkRowsWithCoverage rows with
  | .ok _ => pure ()
  | .error (e, _) => throw e

end LeanGuard.AqmEventLog
