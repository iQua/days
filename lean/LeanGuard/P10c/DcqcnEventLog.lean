import DaysExecutor.Event
import LeanGuard.P10c.Dcqcn.Semantics
import LeanGuard.Shared.Check
import LeanGuard.Shared.Csv

namespace LeanGuard.P10c.DcqcnEventLog

open LeanGuard.Shared
open LeanGuard.P10c

inductive Kind
  | cnp
  | control
  | bytes
  deriving DecidableEq, Repr

def parseKind : String → Except String Kind
  | "cnp" => pure .cnp
  | "control" => pure .control
  | "bytes" => pure .bytes
  | other => throw s!"invalid DCQCN row kind: '{other}'"

def parseStage : String → Except String Dcqcn.Stage
  | "fast_recovery" => pure .fastRecovery
  | "additive" => pure .additive
  | "hyper" => pure .hyper
  | other => throw s!"invalid DCQCN stage: '{other}'"

def parseBit (value : String) : Except String Bool :=
  match value with
  | "0" => pure false
  | "1" => pure true
  | other => throw s!"invalid bit: '{other}'"

def parseU64 (value : String) : Except String Nat := do
  let parsed ← parseNat value
  if parsed ≤ Dcqcn.maxU64 then
    pure parsed
  else
    throw s!"value exceeds u64: '{value}'"

def parseOptU64 (value : String) : Except String (Option Nat) :=
  parseOpt parseU64 value

structure Row where
  key : DaysExecutor.EventKey
  nodeId : Nat
  flowId : Nat
  kind : Kind
  applied : Bool
  emittedBytes : Nat
  config : Dcqcn.Config
  before : Dcqcn.State
  after : Dcqcn.State
  srcLine : Nat
  deriving DecidableEq, Repr

def parseState
    (fieldPrefix : String)
    (idx : Std.HashMap String Nat)
    (fields : Array String) : Except String Dcqcn.State := do
  pure
    { alphaPpb := ← parseU64 (← getField idx fields s!"{fieldPrefix}_alpha_ppb")
      currentRateBps := ← parseU64 (← getField idx fields s!"{fieldPrefix}_current_rate_bps")
      targetRateBps := ← parseU64 (← getField idx fields s!"{fieldPrefix}_target_rate_bps")
      stage := ← parseStage (← getField idx fields s!"{fieldPrefix}_stage")
      stageSteps := ← parseU64 (← getField idx fields s!"{fieldPrefix}_stage_steps")
      bytesSinceIncrease :=
        ← parseU64 (← getField idx fields s!"{fieldPrefix}_bytes_since_increase")
      cnpSeen := ← parseBit (← getField idx fields s!"{fieldPrefix}_cnp_seen")
      lastCnpNs :=
        ← parseOptU64 (← getField idx fields s!"{fieldPrefix}_last_cnp_time_ns")
      nextControlTimeNs :=
        ← parseU64 (← getField idx fields s!"{fieldPrefix}_next_control_time_ns") }

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let result : Except String Row := do
    pure
      { key :=
          { timeNs := ← parseU64 (← getField idx fields "time_ns")
            phase := ← parseU64 (← getField idx fields "event_phase")
            originNode := ← parseU64 (← getField idx fields "event_origin_node")
            originSeq := ← parseU64 (← getField idx fields "event_origin_sequence") }
        nodeId := ← parseU64 (← getField idx fields "node_id")
        flowId := ← parseU64 (← getField idx fields "flow_id")
        kind := ← parseKind (← getField idx fields "kind")
        applied := ← parseBit (← getField idx fields "applied")
        emittedBytes := ← parseU64 (← getField idx fields "emitted_bytes")
        config :=
          { initialRateBps := ← parseU64 (← getField idx fields "initial_rate_bps")
            minimumRateBps := ← parseU64 (← getField idx fields "minimum_rate_bps")
            maximumRateBps := ← parseU64 (← getField idx fields "maximum_rate_bps")
            additiveRateBps := ← parseU64 (← getField idx fields "additive_rate_bps")
            hyperRateBps := ← parseU64 (← getField idx fields "hyper_rate_bps")
            gPpb := ← parseU64 (← getField idx fields "g_ppb")
            decreasePpb := ← parseU64 (← getField idx fields "decrease_ppb")
            cnpIntervalNs := ← parseU64 (← getField idx fields "cnp_interval_ns")
            controlIntervalNs := ← parseU64 (← getField idx fields "control_interval_ns")
            increaseByteThreshold :=
              ← parseU64 (← getField idx fields "increase_byte_threshold") }
        before := ← parseState "before" idx fields
        after := ← parseState "after" idx fields
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

def sameSource (first second : Row) : Bool :=
  first.nodeId = second.nodeId && first.flowId = second.flowId

def continuous (first second : Row) : Bool :=
  first.after = second.before

def checkKeyOrder : List Row → Except String Unit
  | [] | [_] => pure ()
  | first :: second :: rest => do
      require second.srcLine (first.key < second.key)
        "duplicate or backward canonical event key"
      checkKeyOrder (second :: rest)

def checkRow (row : Row) : Except String Unit := do
  require row.srcLine (Dcqcn.validConfig row.config) "invalid DCQCN configuration"
  require row.srcLine (Dcqcn.validState row.config row.before)
    "invalid DCQCN before-state"
  require row.srcLine (Dcqcn.validState row.config row.after)
    "invalid DCQCN after-state"
  let expected ←
    match row.kind with
    | .cnp => do
        require row.srcLine (row.key.phase = 0) "DCQCN CNP arrival must have phase 0"
        require row.srcLine (row.emittedBytes = 0) "CNP row has emitted bytes"
        match row.before.lastCnpNs with
        | some last =>
            require row.srcLine
              (last + row.config.cnpIntervalNs ≤ Dcqcn.maxU64)
              "CNP interval deadline exceeds u64"
        | none => pure ()
        pure (Dcqcn.onCnp row.config row.before row.key.timeNs)
    | .control => do
        require row.srcLine (row.key.phase = 1) "DCQCN control tick must have phase 1"
        require row.srcLine (row.emittedBytes = 0) "control row has emitted bytes"
        require row.srcLine (row.before.nextControlTimeNs = row.key.timeNs)
          "control event time does not match before-state deadline"
        require row.srcLine
          (row.key.timeNs + row.config.controlIntervalNs ≤ Dcqcn.maxU64)
          "next control deadline exceeds u64"
        pure (Dcqcn.onControl row.config row.before row.key.timeNs)
    | .bytes => do
        require row.srcLine (row.key.phase = 1) "DCQCN byte opportunity must have phase 1"
        require row.srcLine (row.emittedBytes > 0) "byte row must emit positive bytes"
        require row.srcLine
          (row.before.bytesSinceIncrease + row.emittedBytes ≤ Dcqcn.maxU64)
          "DCQCN byte counter exceeds u64"
        pure (Dcqcn.onBytes row.config row.before row.emittedBytes)
  require row.srcLine (row.applied = expected.acted) "DCQCN transition result mismatch"
  require row.srcLine (row.after = expected.state) "DCQCN after-state mismatch"

def checkContinuity (rows : List Row) : Except String Unit := do
  let rec go (previous : List Row) : List Row → Except String Unit
    | [] => pure ()
    | row :: rest => do
        match previous.find? (sameSource · row) with
        | none =>
            require row.srcLine (Dcqcn.initialCompatible row.config row.before)
              s!"DCQCN first state is not initial for source (node_id={row.nodeId}, flow_id={row.flowId})"
        | some prior =>
            require row.srcLine (prior.config = row.config)
              s!"DCQCN config discontinuity for source (node_id={row.nodeId}, flow_id={row.flowId})"
            require row.srcLine (continuous prior row)
              s!"DCQCN state discontinuity for source (node_id={row.nodeId}, flow_id={row.flowId})"
        go (row :: previous.filter (fun prior => !sameSource prior row)) rest
  go [] rows

def checkRows (rows : List Row) : Except String Unit := do
  require 1 (!rows.isEmpty) "empty DCQCN trace"
  checkKeyOrder rows
  checkContinuity rows
  for row in rows do checkRow row

end LeanGuard.P10c.DcqcnEventLog
