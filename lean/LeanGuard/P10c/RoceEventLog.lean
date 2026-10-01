import DaysExecutor.Event
import LeanGuard.P10c.Roce.Semantics
import LeanGuard.Shared.Check
import LeanGuard.Shared.Csv

namespace LeanGuard.P10c.RoceEventLog

open LeanGuard.Shared
open LeanGuard.P10c

/-! # Event-log checkers for RoCE queue pairs

The two CSVs of the pinned schema (`days-gpu/plans/briefs/p15/qp-schema.md`):
`roce_receiver_transitions_csv` (one row per data arrival at a queue pair's receiver) and
`roce_sender_transitions_csv` (one row per tick, ACK, NACK or timeout at its sender).

Every check is linear in the number of rows: per-flow state lives in hash maps keyed by
`(node_id, flow_id)` or `flow_id`, never in a list searched per row.
-/

def parseBit (value : String) : Except String Bool :=
  match value with
  | "0" => pure false
  | "1" => pure true
  | other => throw s!"invalid bit: '{other}'"

def parseBounded (kind : String) (maximum : Nat) (value : String) : Except String Nat := do
  let parsed ← parseNat value
  if parsed ≤ maximum then
    pure parsed
  else
    throw s!"value exceeds {kind}: '{value}'"

def parseU64 (value : String) : Except String Nat :=
  parseBounded "u64" Roce.maxU64 value

def parseU128 (value : String) : Except String Nat :=
  parseBounded "u128" Roce.maxU128 value

def parseOptU64 (value : String) : Except String (Option Nat) :=
  parseOpt parseU64 value

def parseKey (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String DaysExecutor.EventKey := do
  pure
    { timeNs := ← parseU64 (← getField idx fields "time_ns")
      phase := ← parseU64 (← getField idx fields "event_phase")
      originNode := ← parseU64 (← getField idx fields "event_origin_node")
      originSeq := ← parseU64 (← getField idx fields "event_origin_sequence") }

/-- Splits a CSV into its header index and its non-empty data lines, numbered from 2. -/
def parseLines {α : Type} (content : String)
    (parseRow : Nat → Std.HashMap String Nat → Array String → Except String α) :
    Except String (List α) := do
  let lines :=
    content.splitOn "\n" |>.map stripCR |>.map String.trim |>.filter (· != "")
  match lines with
  | [] => throw "empty CSV"
  | header :: data =>
      let idx := mkIndex (splitCsvLine header)
      let rec go (lineNo : Nat) (remaining : List String) (rows : List α) := do
        match remaining with
        | [] => pure rows.reverse
        | line :: rest =>
            let row ← parseRow lineNo idx (splitCsvLine line).toArray
            go (lineNo + 1) rest (row :: rows)
      go 2 data []

def withLine {α : Type} (lineNo : Nat) (result : Except String α) : Except String α :=
  match result with
  | .ok value => pure value
  | .error error => throw s!"line {lineNo}: {error}"

/-! ## Receiver rows -/

def parseAction : String → Except String Roce.Action
  | "ack" => pure .ack
  | "duplicate_ack" => pure .duplicateAck
  | "nack" => pure .nack
  | "nack_suppressed" => pure .nackSuppressed
  | "none" => pure .none
  | other => throw s!"invalid RoCE receiver action: '{other}'"

structure ReceiverRow where
  key : DaysExecutor.EventKey
  nodeId : Nat
  flowId : Nat
  config : Roce.ReceiverConfig
  packet : Roce.DataArrival
  packetSentTimeNs : Nat
  packetRetransmission : Bool
  action : Roce.Action
  feedbackAcknowledgment : Option Nat
  feedbackPayload : Option Nat
  cnpSent : Bool
  cnpPayload : Option Nat
  before : Roce.ReceiverState
  after : Roce.ReceiverState
  srcLine : Nat
  deriving DecidableEq, Repr

def parseReceiverState (fieldPrefix : String) (idx : Std.HashMap String Nat)
    (fields : Array String) : Except String Roce.ReceiverState := do
  let nackPsn ← parseOptU64 (← getField idx fields s!"{fieldPrefix}_last_nack_psn")
  let nackTime ← parseOptU64 (← getField idx fields s!"{fieldPrefix}_last_nack_time_ns")
  let lastNack ←
    match nackPsn, nackTime with
    | none, none => pure none
    | some psn, some time => pure (some { expectedPsn := psn, timeNs := time })
    | _, _ => throw s!"{fieldPrefix}_last_nack_psn and {fieldPrefix}_last_nack_time_ns must be both present or both blank"
  pure
    { expectedPsn := ← parseU64 (← getField idx fields s!"{fieldPrefix}_expected_psn")
      packetsSinceAck := ← parseU64 (← getField idx fields s!"{fieldPrefix}_packets_since_ack")
      lastNack := lastNack
      lastCnpNs := ← parseOptU64 (← getField idx fields s!"{fieldPrefix}_last_cnp_time_ns") }

def parseReceiverRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String ReceiverRow := withLine lineNo do
  pure
    { key := ← parseKey idx fields
      nodeId := ← parseU64 (← getField idx fields "node_id")
      flowId := ← parseU64 (← getField idx fields "flow_id")
      config :=
        { totalBytes := ← parseU64 (← getField idx fields "total_bytes")
          ackEveryPackets := ← parseU64 (← getField idx fields "ack_every_packets")
          nackIntervalNs := ← parseU64 (← getField idx fields "nack_interval_ns")
          duplicateAck := ← parseBit (← getField idx fields "duplicate_ack")
          ackSizeBytes := ← parseU64 (← getField idx fields "ack_size_bytes")
          cnpIntervalNs := ← parseU64 (← getField idx fields "cnp_interval_ns") }
      packet :=
        { psn := ← parseU64 (← getField idx fields "packet_psn")
          bytes := ← parseU64 (← getField idx fields "packet_bytes")
          ce := ← parseBit (← getField idx fields "packet_ce") }
      packetSentTimeNs := ← parseU64 (← getField idx fields "packet_sent_time_ns")
      packetRetransmission := ← parseBit (← getField idx fields "packet_retransmission")
      action := ← parseAction (← getField idx fields "action")
      feedbackAcknowledgment := ← parseOptU64 (← getField idx fields "feedback_acknowledgment")
      feedbackPayload := ← parseOptU64 (← getField idx fields "feedback_payload")
      cnpSent := ← parseBit (← getField idx fields "cnp_sent")
      cnpPayload := ← parseOptU64 (← getField idx fields "cnp_payload")
      before := ← parseReceiverState "before" idx fields
      after := ← parseReceiverState "after" idx fields
      srcLine := lineNo }

def parseReceiverCsv (content : String) : Except String (List ReceiverRow) :=
  parseLines content parseReceiverRow

def checkKeyOrder {α : Type} (key : α → DaysExecutor.EventKey) (line : α → Nat) :
    List α → Except String Unit
  | [] | [_] => pure ()
  | first :: second :: rest => do
      require (line second) (key first < key second) "duplicate or backward canonical event key"
      checkKeyOrder key line (second :: rest)

/-- RED stub: the receiver semantics are not checked yet. -/
def checkReceiverRow (_row : ReceiverRow) : Except String Unit := pure ()

/-- RED stub: no continuity yet. -/
def checkReceiverSequence (rows : List ReceiverRow) : Except String Unit := do
  for row in rows do checkReceiverRow row

def checkReceiverRows (rows : List ReceiverRow) : Except String Unit := do
  require 1 (!rows.isEmpty) "empty RoCE receiver trace"
  checkKeyOrder (·.key) (·.srcLine) rows
  checkReceiverSequence rows

end LeanGuard.P10c.RoceEventLog
