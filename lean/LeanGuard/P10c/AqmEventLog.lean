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
  queuedPacketsBefore : Nat
  queuedBytesBefore : Nat
  packetSizeBytes : Nat
  ecnBefore : Bool
  ecnAfter : Bool
  policy : String
  depthUnit : Aqm.DepthUnit
  capacity : Nat
  threshold : Option Nat
  minThreshold : Option Nat
  maxThreshold : Option Nat
  maxProbabilityNumerator : Option Nat
  maxProbabilityDenominator : Option Nat
  markEcn : Bool
  beforeAverageScaled : Option Nat
  beforeCounter : Option Nat
  afterAverageScaled : Option Nat
  afterCounter : Option Nat
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

def parseUnit : String → Except String Aqm.DepthUnit
  | "packets" => pure .packets
  | "bytes" => pure .bytes
  | other => throw s!"invalid depth unit: '{other}'"

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
        queuedPacketsBefore := ← parseNat (← getField idx fields "queued_packets_before")
        queuedBytesBefore := ← parseNat (← getField idx fields "queued_bytes_before")
        packetSizeBytes := ← parseNat (← getField idx fields "packet_size_bytes")
        ecnBefore := ← parseBit (← getField idx fields "ecn_before")
        ecnAfter := ← parseBit (← getField idx fields "ecn_after")
        policy := ← getField idx fields "policy"
        depthUnit := ← parseUnit (← getField idx fields "depth_unit")
        capacity := ← parseNat (← getField idx fields "capacity")
        threshold := ← parseOpt parseNat (← getField idx fields "threshold")
        minThreshold := ← parseOpt parseNat (← getField idx fields "min_threshold")
        maxThreshold := ← parseOpt parseNat (← getField idx fields "max_threshold")
        maxProbabilityNumerator :=
          ← parseOpt parseNat (← getField idx fields "max_probability_numerator")
        maxProbabilityDenominator :=
          ← parseOpt parseNat (← getField idx fields "max_probability_denominator")
        markEcn := ← parseBit (← getField idx fields "mark_ecn")
        beforeAverageScaled :=
          ← parseOpt parseNat (← getField idx fields "before_average_scaled")
        beforeCounter := ← parseOpt parseNat (← getField idx fields "before_counter")
        afterAverageScaled :=
          ← parseOpt parseNat (← getField idx fields "after_average_scaled")
        afterCounter := ← parseOpt parseNat (← getField idx fields "after_counter")
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

def requireOption (line : Nat) (name : String) : Option Nat → Except String Nat
  | some value => pure value
  | none => throw s!"line {line}: missing required field: {name}"

/-- The admission a decision becomes for this row's packet: Days AGO marks only data packets
(`Aqm.exemptNonData`). A parameter of the checks below, so the differential reference
(`P10c/Test/AqmReference.lean`) can supply its own. -/
abbrev Exemption := Row → Aqm.Action → Aqm.Action

def exemption : Exemption := fun row => Aqm.exemptNonData row.isData

def checkThreshold (exempt : Exemption) (row : Row) : Except String Unit := do
  let threshold ← requireOption row.srcLine "threshold" row.threshold
  require row.srcLine
    (row.minThreshold.isNone && row.maxThreshold.isNone &&
      row.maxProbabilityNumerator.isNone && row.maxProbabilityDenominator.isNone &&
      row.beforeAverageScaled.isNone && row.beforeCounter.isNone &&
      row.afterAverageScaled.isNone && row.afterCounter.isNone)
    "threshold certificate contains RED-only state"
  let config : Aqm.ThresholdConfig :=
    { unit := row.depthUnit, capacity := row.capacity, threshold }
  let expected := exempt row <|
    Aqm.thresholdDecision config row.queuedPacketsBefore row.queuedBytesBefore row.packetSizeBytes
  require row.srcLine (row.action = expected)
    s!"threshold decision mismatch ({row.packetKind} packet)"

def checkRed (exempt : Exemption) (row : Row) : Except String Unit := do
  let minimum ← requireOption row.srcLine "min_threshold" row.minThreshold
  let maximum ← requireOption row.srcLine "max_threshold" row.maxThreshold
  let numerator ←
    requireOption row.srcLine "max_probability_numerator" row.maxProbabilityNumerator
  let denominator ←
    requireOption row.srcLine "max_probability_denominator" row.maxProbabilityDenominator
  let beforeAverage ←
    requireOption row.srcLine "before_average_scaled" row.beforeAverageScaled
  let beforeCounter ← requireOption row.srcLine "before_counter" row.beforeCounter
  let afterAverage ← requireOption row.srcLine "after_average_scaled" row.afterAverageScaled
  let afterCounter ← requireOption row.srcLine "after_counter" row.afterCounter
  require row.srcLine row.threshold.isNone "RED certificate contains threshold-only state"
  let before : Aqm.RedState :=
    { unit := row.depthUnit
      capacity := row.capacity
      minThreshold := minimum
      maxThreshold := maximum
      maxProbabilityNumerator := numerator
      maxProbabilityDenominator := denominator
      averageScaled := beforeAverage
      counter := beforeCounter
      markEcn := row.markEcn }
  let (after, action) :=
    Aqm.redDecision before row.queuedPacketsBefore row.queuedBytesBefore row.packetSizeBytes
  require row.srcLine (row.action = exempt row action)
    s!"RED decision mismatch ({row.packetKind} packet)"
  require row.srcLine
    (after.averageScaled = afterAverage && after.counter = afterCounter)
    "RED after-state mismatch"

def checkRow (exempt : Exemption) (row : Row) : Except String Unit := do
  require row.srcLine (row.eventPhase = 0) "AQM enqueue certificate must have phase 0"
  require row.srcLine (row.packetSizeBytes > 0) "packet size must be positive"
  require row.srcLine
    (row.queuedBytesBefore ≤ Aqm.maxQueueBytes && row.packetSizeBytes ≤ Aqm.maxQueueBytes)
    "AQM byte operands exceed the u64 representation domain"
  require row.srcLine (row.capacity > 0) "AQM capacity must be positive"
  require row.srcLine
    (row.ecnAfter = expectedEcnAfter row.ecnBefore row.action)
    "ECN mark transition mismatch"
  match row.policy with
  | "threshold" => checkThreshold exempt row
  | "red" => checkRed exempt row
  | other => throw s!"line {row.srcLine}: invalid AQM policy: {other}"

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
  first.policy = second.policy &&
    first.depthUnit = second.depthUnit &&
    first.capacity = second.capacity &&
    first.threshold = second.threshold &&
    first.minThreshold = second.minThreshold &&
    first.maxThreshold = second.maxThreshold &&
    first.maxProbabilityNumerator = second.maxProbabilityNumerator &&
    first.maxProbabilityDenominator = second.maxProbabilityDenominator &&
    first.markEcn = second.markEcn

/-- Each queue's rows continue its configuration and RED state. One pass in canonical order keeps
each queue's most recent row in a hash map: the one row per queue that the list scan it replaces
kept (it dropped a queue's older row whenever a newer one arrived), so each row is compared with the
same prior row, with the same requirements and messages, in O(1) expected instead of O(queues). -/
def checkContinuity (rows : List Row) : Except String Unit := do
  let mut last : Std.HashMap (Nat × Nat) Row := ∅
  for row in rows do
    match last.get? (row.nodeId, row.queueId) with
    | none => pure ()
    | some prior =>
        require row.srcLine (sameConfig prior row)
          s!"AQM config does not continue the prior config for queue (node_id={row.nodeId}, queue_id={row.queueId})"
        if row.policy = "red" then
          require row.srcLine
            (row.beforeAverageScaled = prior.afterAverageScaled &&
              row.beforeCounter = prior.afterCounter)
            s!"RED before-state does not continue the prior state for queue (node_id={row.nodeId}, queue_id={row.queueId})"
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
    checkRow exemption row
  checkContinuity rows
  checkPacketKinds rows

end LeanGuard.P10c.AqmEventLog
