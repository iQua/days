import Std

import LeanGuard.Shared.Check
import LeanGuard.Shared.Csv
import LeanGuard.Shared.Key
import LeanGuard.Shared.Coverage
import LeanGuard.Drr.Semantics

namespace LeanGuard.DrrEventLog

open LeanGuard.Shared
open LeanGuard.Drr.Semantics

/-- 1:1 with a single row in `drr_events.csv` emitted by Days under `--features lean`. -/
structure Row where
  timeNs : Nat
  eventId : Nat
  kind : Kind
  schedulerId : Nat
  classCount : Nat
  batchId : Option Nat
  packetId : Nat
  flowId : Nat
  classId : Nat
  sizeBytes : Nat
  quantumBytes : Nat
  deficitBytes : Nat
  rateBps : Nat
  currentQueue : Nat
  scanSteps : Nat
  departureTimeNs : Option Nat
  srcLine : Nat
deriving DecidableEq, Repr

def key (r : Row) : Nat × Nat :=
  (r.timeNs, r.eventId)

def parseKind (s : String) : Except String Kind :=
  match s with
  | "enqueue" => pure Kind.enqueue
  | "schedule" => pure Kind.schedule
  | other => throw s!"invalid kind: {other}"

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let res : Except String Row := do
    let timeNs ← parseNat (← getField idx fields "time_ns")
    let eventId ← parseNat (← getField idx fields "event_id")
    let kind ← parseKind (← getField idx fields "kind")
    let schedulerId ← parseNat (← getField idx fields "scheduler_id")
    let classCount ← parseNat (← getField idx fields "class_count")
    let batchId ← parseOpt parseNat (← getField idx fields "batch_id")
    let packetId ← parseNat (← getField idx fields "packet_id")
    let flowId ← parseNat (← getField idx fields "flow_id")
    let classId ← parseNat (← getField idx fields "class_id")
    let sizeBytes ← parseNat (← getField idx fields "size_bytes")
    let quantumBytes ← parseNat (← getField idx fields "quantum_bytes")
    let deficitBytes ← parseNat (← getField idx fields "deficit_bytes")
    let rateBps ← parseNat (← getField idx fields "rate_bps")
    let currentQueue ← parseNat (← getField idx fields "current_queue")
    let scanSteps ← parseNat (← getField idx fields "scan_steps")
    let departureTimeNs ← parseOpt parseNat (← getField idx fields "departure_time_ns")
    pure
      { timeNs
        eventId
        kind
        schedulerId
        classCount
        batchId
        packetId
        flowId
        classId
        sizeBytes
        quantumBytes
        deficitBytes
        rateBps
        currentQueue
        scanSteps
        departureTimeNs
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
    kind := r.kind
    schedulerId := r.schedulerId
    classCount := r.classCount
    batchId := r.batchId
    packetId := r.packetId
    flowId := r.flowId
    classId := r.classId
    sizeBytes := r.sizeBytes
    quantumBytes := r.quantumBytes
    deficitBytes := r.deficitBytes
    rateBps := r.rateBps
    currentQueue := r.currentQueue
    scanSteps := r.scanSteps
    departureTimeNs := r.departureTimeNs }

def listAny (xs : List Nat) (p : Nat → Bool) : Bool :=
  match xs with
  | [] => false
  | x :: rest => if p x then true else listAny rest p

def recordCover (cov : CoverageState) (g : Global) (r : Row) : CoverageState :=
  let cov :=
    if r.classCount >= 2 then
      covHit cov "class_count_ge_2"
    else
      cov
  match r.kind with
  | Kind.enqueue => cov
  | Kind.schedule =>
      let cov :=
        if r.scanSteps > 0 then
          covHit cov "scanSteps_gt_0"
        else
          cov
      let st := g.schedulers.getD r.schedulerId {}
      let classCount := r.classCount
      let cov :=
        if r.scanSteps > 0 && classCount > 0 &&
            (st.currentQueue + r.scanSteps >= classCount) then
          covHit cov "wraparound_updates_deficits"
        else
          cov
      let cov :=
        if r.scanSteps > 0 && classCount > 0 &&
            (st.currentQueue + r.scanSteps >= classCount) then
          let needsReset := listAny (classIds classCount)
            (fun cid => queueEmpty st cid && getDeficit st cid > 0)
          if needsReset then
            covHit cov "deficit_reset_on_empty"
          else
            cov
        else
          cov
      let cov :=
        if r.deficitBytes = 0 then
          covHit cov "size_eq_deficit"
        else
          covHit cov "size_lt_deficit"
      cov

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

end LeanGuard.DrrEventLog
