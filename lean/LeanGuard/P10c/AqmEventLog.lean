import DaysExecutor.Event
import LeanGuard.P10c.Semantics
import LeanGuard.Shared.Csv

namespace LeanGuard.P10c.AqmEventLog

open LeanGuard.Shared
open LeanGuard.P10c.Semantics

structure Row where
  timeNs : Nat
  eventPhase : Nat
  originNode : Nat
  originSeq : Nat
  nodeId : Nat
  queueId : Nat
  payloadId : Nat
  packetKind : String
  queuedBytesBefore : Nat
  packetSizeBytes : Nat
  ecnBefore : Bool
  ecnAfter : Bool
  seed : Nat
  capacityBytes : Nat
  kminBytes : Nat
  kmaxBytes : Nat
  pmaxNumerator : Nat
  pmaxDenominator : Nat
  action : Aqm.Action
  srcLine : Nat
deriving DecidableEq, Repr

def key (row : Row) : DaysExecutor.EventKey :=
  { timeNs := row.timeNs
    phase := row.eventPhase
    originNode := row.originNode
    originSeq := row.originSeq }

def parseBit (value : String) : Except String Bool :=
  match value with
  | "0" => pure false
  | "1" => pure true
  | other => throw s!"invalid bit: '{other}'"

/-- The executor's packet kinds (`aqm_trace.rs`), and whether each is a data packet, the only
kind Days AGO marks. -/
def dataKind : String → Except String Bool
  | "data" | "tcp_data" | "roce_data" => pure true
  | "feedback" | "tcp_ack" | "pfc" | "dcqcn_cnp" | "roce_ack" | "roce_nack"
  | "roce_pacing_timer" | "stage_notify" => pure false
  | other => throw s!"invalid packet kind: '{other}'"

def parsePacketKind (value : String) : Except String String := do
  let _ ← dataKind value
  pure value

def Row.isData (row : Row) : Bool :=
  match dataKind row.packetKind with
  | .ok isData => isData
  | .error _ => false

def parseAction : String → Except String Aqm.Action
  | "enqueue" => pure .enqueue
  | "mark" => pure .mark
  | "drop" => pure .drop
  | other => throw s!"invalid AQM action: '{other}'"

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let result : Except String Row := do
    pure
      { timeNs := ← parseNat (← getField idx fields "time_ns")
        eventPhase := ← parseNat (← getField idx fields "event_phase")
        originNode := ← parseNat (← getField idx fields "event_origin_node")
        originSeq := ← parseNat (← getField idx fields "event_origin_sequence")
        nodeId := ← parseNat (← getField idx fields "node_id")
        queueId := ← parseNat (← getField idx fields "queue_id")
        payloadId := ← parseNat (← getField idx fields "payload_id")
        packetKind := ← parsePacketKind (← getField idx fields "packet_kind")
        queuedBytesBefore := ← parseNat (← getField idx fields "queued_bytes_before")
        packetSizeBytes := ← parseNat (← getField idx fields "packet_size_bytes")
        ecnBefore := ← parseBit (← getField idx fields "ecn_before")
        ecnAfter := ← parseBit (← getField idx fields "ecn_after")
        seed := ← parseNat (← getField idx fields "seed")
        capacityBytes := ← parseNat (← getField idx fields "capacity_bytes")
        kminBytes := ← parseNat (← getField idx fields "kmin_bytes")
        kmaxBytes := ← parseNat (← getField idx fields "kmax_bytes")
        pmaxNumerator := ← parseNat (← getField idx fields "pmax_numerator")
        pmaxDenominator := ← parseNat (← getField idx fields "pmax_denominator")
        action := ← parseAction (← getField idx fields "action")
        srcLine := lineNo }
  match result with
  | .ok row => pure row
  | .error error => throw s!"line {lineNo}: {error}"

def parseCsv (content : String) : Except String (List Row) := do
  let lines :=
    content.splitOn "\n" |>.map stripCR |>.map String.trim |>.filter (· != "")
  match lines with
  | [] => throw "empty CSV"
  | header :: data =>
      let idx := mkIndex (splitCsvLine header)
      let rec go (lineNo : Nat) (remaining : List String) (rows : List Row) := do
        match remaining with
        | [] => pure rows.reverse
        | line :: rest =>
            let row ← parseRow lineNo idx (splitCsvLine line).toArray
            go (lineNo + 1) rest (row :: rows)
      go 2 data []

def expectedEcnAfter (before : Bool) : Aqm.Action → Bool
  | .mark => true
  | .enqueue | .drop => before

def Row.config (row : Row) : Aqm.RampConfig :=
  { capacityBytes := row.capacityBytes
    kminBytes := row.kminBytes
    kmaxBytes := row.kmaxBytes
    pmaxNumerator := row.pmaxNumerator
    pmaxDenominator := row.pmaxDenominator }

/-- The decision a row's inputs determine. A parameter of the checks below, so the differential
reference (`P10c/Test/AqmReference.lean`) can supply its own, independently written. -/
abbrev Decision := Row → Aqm.Action

/-- The shipped decision: the executor's rule (`Aqm.rampDecision`) on the row's config, its
packet's kind (Days AGO marks only data packets) and the draw it recomputes from the seed, the
queue and the payload. -/
def decision : Decision := fun row =>
  Aqm.rampDecision row.config row.isData row.queuedBytesBefore row.packetSizeBytes
    (Aqm.draw (Aqm.queueKey row.seed row.nodeId row.queueId) row.payloadId)

def checkRow (decide : Decision) (row : Row) : Except String Unit := do
  require row.srcLine (row.eventPhase = 0) "AQM enqueue certificate must have phase 0"
  require row.srcLine (row.packetSizeBytes > 0) "packet size must be positive"
  require row.srcLine
    (row.queuedBytesBefore ≤ Aqm.maxQueueBytes && row.packetSizeBytes ≤ Aqm.maxQueueBytes)
    "AQM byte operands exceed the u64 representation domain"
  require row.srcLine
    (row.seed ≤ Aqm.maxQueueBytes && row.nodeId ≤ Aqm.maxQueueBytes &&
      row.queueId ≤ Aqm.maxQueueBytes && row.payloadId ≤ Aqm.maxQueueBytes)
    "AQM draw inputs exceed the u64 representation domain"
  require row.srcLine row.config.wellFormed "ECN ramp configuration is not well formed"
  require row.srcLine
    (row.ecnAfter = expectedEcnAfter row.ecnBefore row.action)
    "ECN mark transition mismatch"
  require row.srcLine (row.action = decide row)
    s!"ECN ramp decision mismatch ({row.packetKind} packet)"

def canonicalize (rows : List Row) : Except String (List Row) := do
  let sorted := rows.toArray.qsort (fun a b => decide (key a < key b)) |>.toList
  let rec check : List Row → Except String Unit
    | [] | [_] => pure ()
    | first :: second :: rest => do
        require second.srcLine (key first < key second) "duplicate canonical event key"
        check (second :: rest)
  check sorted
  pure sorted

def sameConfig (first second : Row) : Bool :=
  first.config = second.config

/-- Each queue's rows continue its configuration, and every row names the run's one seed. One pass
in canonical order keeps each queue's most recent row in a hash map, so each row is compared with
the same prior row in O(1) expected. -/
def checkContinuity (rows : List Row) : Except String Unit := do
  let mut last : Std.HashMap (Nat × Nat) Row := ∅
  let mut seed : Option Nat := none
  for row in rows do
    match seed with
    | none => seed := some row.seed
    | some first =>
        require row.srcLine (row.seed = first) "the seed differs from the run's first row"
    match last.get? (row.nodeId, row.queueId) with
    | none => pure ()
    | some prior =>
        require row.srcLine (sameConfig prior row)
          s!"AQM config does not continue the prior config for queue (node_id={row.nodeId}, queue_id={row.queueId})"
    last := last.insert (row.nodeId, row.queueId) row

def kindChangeMessage (row : Row) (firstKind : String) (firstLine : Nat) : String :=
  s!"packet kind of payload {row.payloadId} changes between its rows: {firstKind} at line {firstLine}, {row.packetKind} here"

/-- A packet's kind is the same on every row of its payload (one per queue it crosses). One pass in
canonical order keeps each payload's first row in a hash map, so a row whose kind differs is
rejected against that first row. -/
def checkPacketKinds (rows : List Row) : Except String Unit := do
  let mut first : Std.HashMap Nat (String × Nat) := ∅
  for row in rows do
    match first.get? row.payloadId with
    | none => first := first.insert row.payloadId (row.packetKind, row.srcLine)
    | some (kind, line) =>
        require row.srcLine (kind = row.packetKind) (kindChangeMessage row kind line)

def checkRows (rows : List Row) : Except String Unit := do
  let rows ← canonicalize rows
  for row in rows do
    checkRow decision row
  checkContinuity rows
  checkPacketKinds rows

end LeanGuard.P10c.AqmEventLog
