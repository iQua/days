import DaysExecutor.Event
import LeanGuard.P10c.DcqcnEventLog
import LeanGuard.P10c.MechanismEventLog
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

/-- One receiver row against §5 step 9, given only the row. -/
def checkReceiverRow (row : ReceiverRow) : Except String Unit := do
  let line := row.srcLine
  require line (Roce.validReceiverConfig row.config) "invalid RoCE receiver configuration"
  require line (Roce.validReceiverState row.config row.before) "invalid RoCE receiver before-state"
  require line (Roce.validReceiverState row.config row.after) "invalid RoCE receiver after-state"
  require line (row.key.phase = 0) "RoCE data arrival must have phase 0"
  require line (row.packet.bytes > 0) "RoCE data packet has no bytes"
  require line (row.packet.psn + row.packet.bytes ≤ row.config.totalBytes)
    "RoCE data packet extends beyond the queue pair's total bytes"
  require line (row.packetSentTimeNs ≤ row.key.timeNs) "RoCE data packet arrives before it was sent"
  let expected := Roce.onData row.config row.before row.key.timeNs row.packet
  require line (row.cnpSent = expected.cnpSent) "RoCE notification-point decision mismatch"
  require line (row.cnpPayload.isSome = row.cnpSent) "RoCE CNP payload present iff a CNP was sent"
  require line (row.action = expected.action) "RoCE receiver action mismatch"
  require line (row.feedbackAcknowledgment = expected.feedbackAcknowledgment)
    "RoCE feedback acknowledgment mismatch"
  require line (row.feedbackPayload.isSome = row.action.sendsFeedback)
    "RoCE feedback payload present iff an ACK or NACK was sent"
  require line (row.after = expected.state) "RoCE receiver after-state mismatch"

/--
One pass in event order: per-receiver continuity (each row starts where the receiver's previous row ended, the first one
from the initial state, under one configuration) and the target's payload order: payload
sequences are allocated in event order, CNP before ACK or NACK (S1), so every payload a receiver
host allocates exceeds the previous one. Each row is then checked against the semantics.
-/
def checkReceiverSequence (rows : List ReceiverRow) : Except String Unit := do
  let mut last : Std.HashMap (Nat × Nat) ReceiverRow := ∅
  let mut lastPayload : Std.HashMap Nat Nat := ∅
  for row in rows do
    let source := (row.nodeId, row.flowId)
    match last.get? source with
    | none =>
        require row.srcLine (row.before = Roce.initialReceiver)
          s!"RoCE receiver first state is not initial (node_id={row.nodeId}, flow_id={row.flowId})"
    | some prior =>
        require row.srcLine (prior.config = row.config)
          s!"RoCE receiver config discontinuity (node_id={row.nodeId}, flow_id={row.flowId})"
        require row.srcLine (prior.after = row.before)
          s!"RoCE receiver state discontinuity (node_id={row.nodeId}, flow_id={row.flowId})"
    checkReceiverRow row
    last := last.insert source row
    for payload in [row.cnpPayload, row.feedbackPayload].filterMap id do
      match lastPayload.get? row.nodeId with
      | some previous =>
          require row.srcLine (previous < payload)
            s!"RoCE receiver payloads out of allocation order (node_id={row.nodeId})"
      | none => pure ()
      lastPayload := lastPayload.insert row.nodeId payload

def checkReceiverRows (rows : List ReceiverRow) : Except String Unit := do
  require 1 (!rows.isEmpty) "empty RoCE receiver trace"
  checkKeyOrder (·.key) (·.srcLine) rows
  checkReceiverSequence rows

/-! ## Sender rows, joined with the DCQCN controller log -/

/-- Raises an error that names the log and the line it refers to. -/
def requireAt (role : String) (lineNo : Nat) (cond : Bool) (msg : String) : Except String Unit :=
  if cond then pure () else throw s!"{role}: line {lineNo}: {msg}"

def inRole {α : Type} (role : String) (result : Except String α) : Except String α :=
  match result with
  | .ok value => pure value
  | .error error => throw s!"{role}: {error}"

inductive SenderKind
  | tick
  | ack
  | nack
  | timeout
  /-- Amendment 2: a PFC RESUME restarted the pair's pause-parked pacer. -/
  | resume
  deriving DecidableEq, Repr

def parseSenderKind : String → Except String SenderKind
  | "tick" => pure .tick
  | "ack" => pure .ack
  | "nack" => pure .nack
  | "timeout" => pure .timeout
  | "resume" => pure .resume
  | other => throw s!"invalid RoCE sender kind: '{other}'"

def parsePacer : String → Except String Roce.Pacer
  | "armed" => pure .armed
  | "parked" => pure .parked
  | "stopped" => pure .stopped
  | other => throw s!"invalid RoCE pacer state: '{other}'"

def parseStatus : String → Except String Roce.Status
  | "scheduled" => pure .scheduled
  | "blocked" => pure .blocked
  | "finished" => pure .finished
  | "stopped" => pure .stopped
  | other => throw s!"invalid generator status: '{other}'"

structure SenderRow where
  key : DaysExecutor.EventKey
  nodeId : Nat
  flowId : Nat
  kind : SenderKind
  /-- Amendment 1: the tick found the pair's data class paused on its host's egress. -/
  classPaused : Bool
  /-- Amendment 3: the pair's data priority (0..=7); `none` in a log without the column. -/
  dataClass : Option Nat
  config : Roce.SenderConfig
  rateBps : Option Nat
  inputAcknowledgment : Option Nat
  emitted : Bool
  emission : Option Roce.Emission
  emittedPayload : Option Nat
  before : Roce.SenderState
  after : Roce.SenderState
  srcLine : Nat
  deriving DecidableEq, Repr

def parseSenderState (fieldPrefix : String) (idx : Std.HashMap String Nat)
    (fields : Array String) : Except String Roce.SenderState := do
  pure
    { nextPsn := ← parseU64 (← getField idx fields s!"{fieldPrefix}_next_psn")
      sndUna := ← parseU64 (← getField idx fields s!"{fieldPrefix}_snd_una")
      bytesEmitted := ← parseU64 (← getField idx fields s!"{fieldPrefix}_bytes_emitted")
      packetsEmitted := ← parseU64 (← getField idx fields s!"{fieldPrefix}_packets_emitted")
      creditQuanta := ← parseU128 (← getField idx fields s!"{fieldPrefix}_credit_quanta")
      rtoDeadlineNs := ← parseOptU64 (← getField idx fields s!"{fieldPrefix}_rto_deadline_ns")
      pacer := ← parsePacer (← getField idx fields s!"{fieldPrefix}_pacer")
      nextTickNs := ← parseOptU64 (← getField idx fields s!"{fieldPrefix}_next_tick_ns")
      status := ← parseStatus (← getField idx fields s!"{fieldPrefix}_status") }

def parseSenderRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String SenderRow := withLine lineNo do
  -- Amendment 1. A log from a writer before the amendment has no such column; such a writer
  -- cannot pause a class, so every row reads 0.
  let classPaused ←
    if idx.contains "class_paused" then parseBit (← getField idx fields "class_paused")
    else pure false
  let dataClass ←
    if idx.contains "data_class" then do
      let value ← parseNat (← getField idx fields "data_class")
      if value ≤ 7 then pure (some value) else throw s!"data_class exceeds 7: '{value}'"
    else pure none
  let emitted ← parseBit (← getField idx fields "emitted")
  let psn ← parseOptU64 (← getField idx fields "emitted_psn")
  let bytes ← parseOptU64 (← getField idx fields "emitted_bytes")
  let retransmission ← parseOpt parseBit (← getField idx fields "emitted_retransmission")
  let payload ← parseOptU64 (← getField idx fields "emitted_payload")
  let emission ←
    match emitted, psn, bytes, retransmission, payload with
    | true, some psn, some bytes, some retransmission, some _ =>
        pure (some { psn := psn, bytes := bytes, retransmission := retransmission })
    | false, none, none, none, none => pure none
    | _, _, _, _, _ => throw "emitted_* fields must be present iff emitted = 1"
  pure
    { key := ← parseKey idx fields
      nodeId := ← parseU64 (← getField idx fields "node_id")
      flowId := ← parseU64 (← getField idx fields "flow_id")
      kind := ← parseSenderKind (← getField idx fields "kind")
      classPaused := classPaused
      dataClass := dataClass
      config :=
        { mtuBytes := ← parseU64 (← getField idx fields "mtu_bytes")
          totalBytes := ← parseU64 (← getField idx fields "total_bytes")
          pacingIntervalNs := ← parseU64 (← getField idx fields "pacing_interval_ns")
          firstPacingTimeNs := ← parseU64 (← getField idx fields "first_pacing_time_ns")
          rtoNs := ← parseU64 (← getField idx fields "rto_ns") }
      rateBps := ← parseOptU64 (← getField idx fields "rate_bps")
      inputAcknowledgment := ← parseOptU64 (← getField idx fields "input_acknowledgment")
      emitted := emitted
      emission := emission
      emittedPayload := payload
      before := ← parseSenderState "before" idx fields
      after := ← parseSenderState "after" idx fields
      srcLine := lineNo }

def parseSenderCsv (content : String) : Except String (List SenderRow) :=
  parseLines content parseSenderRow

/-- One sender log item in event order: a sender row with the DCQCN byte opportunity that shares
its event key, or a DCQCN controller row of a queue pair on its own. -/
inductive SenderItem
  | sender (row : SenderRow) (bytes : Option DcqcnEventLog.Row)
  | dcqcn (row : DcqcnEventLog.Row)
  /-- Amendment 3: a PFC control record (PAUSE or RESUME) at a queue pair's host. -/
  | pfc (row : MechanismEventLog.PfcLog.Row)

def SenderItem.key : SenderItem → DaysExecutor.EventKey
  | .sender row _ => row.key
  | .dcqcn row => row.key
  | .pfc row => row.key

/-- Merges the host PFC records into the key-ordered sender items (linear). On an equal key the
PFC record comes first: a RESUME's `resume` rows carry the RESUME's own key. -/
def mergePfc : List MechanismEventLog.PfcLog.Row → List SenderItem → List SenderItem →
    List SenderItem
  | [], [], acc => acc.reverse
  | p :: ps, [], acc => mergePfc ps [] (.pfc p :: acc)
  | [], i :: is, acc => mergePfc [] is (i :: acc)
  | p :: ps, i :: is, acc =>
      if p.key ≤ i.key then mergePfc ps (i :: is) (.pfc p :: acc)
      else mergePfc (p :: ps) is (i :: acc)
termination_by ps is => ps.length + is.length

/-- Merges the two key-ordered logs (linear). -/
def mergeLogs : List SenderRow → List DcqcnEventLog.Row → List SenderItem → List SenderItem
  | [], [], acc => acc.reverse
  | s :: ss, [], acc => mergeLogs ss [] (.sender s none :: acc)
  | [], d :: ds, acc => mergeLogs [] ds (.dcqcn d :: acc)
  | s :: ss, d :: ds, acc =>
      if d.key < s.key then mergeLogs (s :: ss) ds (.dcqcn d :: acc)
      else if s.key < d.key then mergeLogs ss (d :: ds) (.sender s none :: acc)
      else mergeLogs ss ds (.sender s (some d) :: acc)
termination_by ss ds => ss.length + ds.length

/-- What the sender checker tracks per queue pair `(node_id, flow_id)`. -/
structure FlowTrack where
  config : Roce.SenderConfig
  /-- The state after the pair's last transition, including DCQCN status recomputations. -/
  state : Roce.SenderState
  /-- The controller's current rate after the pair's last controller transition. -/
  rateBps : Nat
  /-- A control tick ran on the completed pair: D2 forbids another. -/
  controlSpent : Bool
  /-- The controller's pending control time, from the pair's last DCQCN row (`none` before the
  pair's first DCQCN row). -/
  controlNs : Option Nat
  /-- Amendment 2: the pacer was parked by a paused tick and not restarted since, so a RESUME may
  restart it. -/
  pauseParked : Bool
  /-- Amendment 3: the pair's data class (constant per pair). -/
  dataClass : Option Nat

/--
The events a pair has pending, as `(time, rank, name)`: the armed pacing tick, the armed timeout,
and the control tick unless D2 has spent it (a control tick ran on the completed pair and was not
re-armed). Each must fire, as a row, before any later event of the pair: a full run executes
every event with time `≤ stop_time_ns` in `EventKey` order (`scalar.rs` run loop).
-/
def pendingEvents (flow : FlowTrack) : List (Nat × Nat × String) :=
  let tick :=
    if flow.state.pacer = .armed then flow.state.nextTickNs.map (fun t => (t, 0, "RoCE pacing tick"))
    else none
  let timeout := flow.state.rtoDeadlineNs.map (fun t => (t, 1, "RoCE timeout"))
  let control :=
    if flow.controlSpent then none else flow.controlNs.map (fun t => (t, 2, "DCQCN control tick"))
  [tick, timeout, control].filterMap id

/--
An event of a pair at `timeNs` may not lie after any of the pair's pending events. Equal times are
allowed: an arrival (phase 0) precedes a timer at the same instant (S2), and two timers of one host
at one instant are ordered by an `origin_seq` the logs do not show (S3).
-/
def checkPending (role : String) (lineNo : Nat) (source : Nat × Nat) (flow : FlowTrack)
    (timeNs : Nat) : Except String Unit := do
  for (pendingNs, _, name) in pendingEvents flow do
    requireAt role lineNo (timeNs ≤ pendingNs)
      s!"pending {name} at {pendingNs} did not fire before this row (node_id={source.1}, flow_id={source.2})"

/-- The stop-time decisions the log implies, when the stop time is not given. -/
structure StopBounds where
  latestWithin : Option (Nat × Nat) := none
  earliestBeyond : Option (Nat × Nat) := none

/-- Amendment 3: the pause-parked pairs a host RESUME must restart, awaiting their `resume` rows
at the RESUME's key. -/
structure ResumeExpectation where
  key : DaysExecutor.EventKey
  node : Nat
  dataClass : Nat
  pfcLine : Nat
  remaining : Std.HashSet Nat

structure SenderTrack where
  flows : Std.HashMap (Nat × Nat) FlowTrack := ∅
  lastPayload : Std.HashMap Nat Nat := ∅
  stop : StopBounds := {}
  /-- Amendment 3: `(node, controlled_link, priority)` whose host controller set is non-empty. -/
  hostAsserted : Std.HashMap (Nat × Nat × Nat) Bool := ∅
  /-- Amendment 3: per `(node, priority)`, how many host controlled links assert a pause. -/
  hostPausedLinks : Std.HashMap (Nat × Nat) Nat := ∅
  /-- Amendment 3: per `(node, data class)`, the flows whose pacer a paused tick parked. -/
  pauseParkedIndex : Std.HashMap (Nat × Nat) (Std.HashSet Nat) := ∅
  expectation : Option ResumeExpectation := none

/-- The row-local shape rules of a sender row: phase, which optional fields are present, and the
validity of its configuration and states. -/
def checkSenderShape (row : SenderRow) : Except String Unit := do
  let at_ := requireAt "sender" row.srcLine
  at_ (Roce.validSenderConfig row.config) "invalid RoCE sender configuration"
  at_ (Roce.validSenderState row.config row.before) "invalid RoCE sender before-state"
  at_ (Roce.validSenderState row.config row.after) "invalid RoCE sender after-state"
  at_ (!row.classPaused || row.kind = .tick) "RoCE class_paused set on a non-tick row"
  match row.kind with
  | .tick =>
      at_ (row.key.phase = 1) "RoCE pacing tick must have phase 1"
      at_ row.inputAcknowledgment.isNone "RoCE tick row carries an acknowledgment"
      if row.classPaused then
        at_ (row.rateBps.isNone && !row.emitted) "RoCE paused tick credits or emits"
      else
        at_ (row.rateBps.isSome = (row.before.nextPsn < row.config.totalBytes))
          "RoCE tick rate present iff the tick credits"
      at_ (row.before.pacer = .armed && row.before.nextTickNs = some row.key.timeNs)
        "RoCE tick of a pacer not armed for this time"
  | .ack | .nack =>
      at_ (row.key.phase = 0) "RoCE ACK or NACK arrival must have phase 0"
      at_ row.inputAcknowledgment.isSome "RoCE ACK or NACK row has no acknowledgment"
      at_ (row.rateBps.isNone && !row.emitted) "RoCE ACK or NACK row credits or emits"
      at_ (row.inputAcknowledgment.all (· ≤ row.before.bytesEmitted))
        "RoCE acknowledgment above the sender's high-water mark"
  | .resume =>
      at_ (row.key.phase = 0) "RoCE resume must have phase 0"
      at_ (row.rateBps.isNone && row.inputAcknowledgment.isNone && !row.emitted)
        "RoCE resume row credits, emits or carries an acknowledgment"
  | .timeout =>
      at_ (row.key.phase = 1) "RoCE timeout must have phase 1"
      at_ (row.rateBps.isNone && row.inputAcknowledgment.isNone && !row.emitted)
        "RoCE timeout row credits, emits or carries an acknowledgment"
      at_ (row.before.rtoDeadlineNs = some row.key.timeNs)
        "RoCE timeout fires without an armed deadline at this time"

/-- Records or checks the one stop-time comparison a transition made. -/
def checkStop (stopTimeNs : Option Nat) (row : SenderRow) (query : Option Nat) (withinStop : Bool)
    (bounds : StopBounds) : Except String StopBounds := do
  match query with
  | none => pure bounds
  | some tick =>
      match stopTimeNs with
      | some stop =>
          requireAt "sender" row.srcLine (withinStop = decide (tick ≤ stop))
            s!"RoCE pacer stop decision contradicts stop_time_ns={stop} (tick {tick})"
          pure bounds
      | none =>
          if withinStop then
            pure { bounds with
              latestWithin :=
                match bounds.latestWithin with
                | some (latest, line) => if tick > latest then some (tick, row.srcLine) else some (latest, line)
                | none => some (tick, row.srcLine) }
          else
            pure { bounds with
              earliestBeyond :=
                match bounds.earliestBeyond with
                | some (earliest, line) => if tick < earliest then some (tick, row.srcLine) else some (earliest, line)
                | none => some (tick, row.srcLine) }

/-- Checks one sender row against §5 given the tracked rate, and returns the updated track. -/
def checkSenderItem (stopTimeNs : Option Nat) (track : SenderTrack) (row : SenderRow)
    (bytes : Option DcqcnEventLog.Row) : Except String SenderTrack := do
  let at_ := requireAt "sender" row.srcLine
  let source := (row.nodeId, row.flowId)
  let prior := track.flows.get? source
  -- Continuity: the first row starts from the initial state, later rows where the pair stood.
  let rate ←
    match prior with
    | none => do
        let rate ←
          match row.rateBps with
          | some rate => pure rate
          | none => throw s!"sender: line {row.srcLine}: RoCE queue pair's first row is not a crediting tick (node_id={row.nodeId}, flow_id={row.flowId})"
        at_ (row.kind = .tick && row.before = Roce.initialSender row.config rate)
          s!"RoCE sender first state is not initial (node_id={row.nodeId}, flow_id={row.flowId})"
        pure rate
    | some flow => do
        at_ (flow.config = row.config)
          s!"RoCE sender config discontinuity (node_id={row.nodeId}, flow_id={row.flowId})"
        at_ (flow.dataClass = row.dataClass)
          s!"RoCE data_class discontinuity (node_id={row.nodeId}, flow_id={row.flowId})"
        at_ (flow.state = row.before)
          s!"RoCE sender state discontinuity (node_id={row.nodeId}, flow_id={row.flowId})"
        pure flow.rateBps
  checkSenderShape row
  -- The pair's pending events fired before this row (review H1). A byte opportunity at this key
  -- carries the controller's pending control time, also for the pair's first DCQCN row.
  let controlNs :=
    match bytes with
    | some d => some d.before.nextControlTimeNs
    | none => prior.bind (·.controlNs)
  match prior with
  | some flow => checkPending "sender" row.srcLine source { flow with controlNs := controlNs } row.key.timeNs
  | none =>
      checkPending "sender" row.srcLine source
        { config := row.config, state := row.before, rateBps := rate, controlSpent := false,
          controlNs := controlNs, pauseParked := false, dataClass := row.dataClass }
        row.key.timeNs
  let pauseParked := prior.map (·.pauseParked) |>.getD false
  if row.kind = .resume then
    at_ pauseParked
      s!"RoCE resume of a queue pair not parked by a pause (node_id={row.nodeId}, flow_id={row.flowId})"
  -- The joined controller log: a tick credits at the controller's current rate, and the DCQCN
  -- byte opportunity at the same key exists exactly when the tick sends, for the same bytes.
  at_ (row.rateBps.all (· = rate)) "RoCE tick rate differs from the DCQCN controller's current rate"
  let rateAfterBytes ←
    match bytes, row.emission with
    | none, none => pure rate
    | some d, some emission => do
        at_ (d.nodeId = row.nodeId && d.flowId = row.flowId && d.kind = .bytes)
          "RoCE tick shares its event key with a DCQCN row that is not its byte opportunity"
        at_ (d.emittedBytes = emission.bytes && d.before.currentRateBps = rate)
          "RoCE emission differs from its DCQCN byte opportunity"
        pure d.after.currentRateBps
    | some _, none => throw s!"sender: line {row.srcLine}: DCQCN byte opportunity without a RoCE emission"
    | none, some _ => throw s!"sender: line {row.srcLine}: RoCE emission without a DCQCN byte opportunity"
  let withinStop := row.after.pacer != .stopped
  let expected :=
    match row.kind, row.inputAcknowledgment with
    | .tick, _ =>
        if row.classPaused then Roce.onPausedTick row.config rate row.before
        else Roce.onTick row.config rate rateAfterBytes row.key.timeNs withinStop row.before
    | .ack, some value =>
        Roce.onFeedback row.config rate row.key.timeNs value false withinStop row.before
    | .nack, some value =>
        Roce.onFeedback row.config rate row.key.timeNs value true withinStop row.before
    | .timeout, _ => Roce.onTimeout row.config rate row.key.timeNs withinStop row.before
    | .resume, _ => Roce.onResume row.config rate row.key.timeNs withinStop row.before
    | _, none => { state := row.before, emission := none, stopQuery := none }
  -- The executor's fixed-width arithmetic: anything beyond it is an execution error, not a row.
  at_ (row.before.creditQuanta + Roce.tickCredit row.config (row.rateBps.getD 0) ≤ Roce.maxU128)
    "RoCE pacing credit exceeds u128"
  at_ (expected.stopQuery.all (· ≤ Roce.maxU64) &&
      row.key.timeNs + row.config.rtoNs ≤ Roce.maxU64)
    "RoCE timer exceeds u64"
  at_ (row.kind != .resume || expected.stopQuery.isSome) "RoCE resume of a pacer that cannot restart"
  at_ (row.emission = expected.emission) "RoCE emission mismatch"
  at_ (row.after = expected.state) "RoCE sender after-state mismatch"
  let stop ← checkStop stopTimeNs row expected.stopQuery withinStop track.stop
  -- Payloads are allocated in event order on each node.
  let lastPayload ←
    match row.emittedPayload with
    | none => pure track.lastPayload
    | some payload => do
        match track.lastPayload.get? row.nodeId with
        | some previous =>
            at_ (previous < payload)
              s!"RoCE sender payloads out of allocation order (node_id={row.nodeId})"
        | none => pure ()
        pure (track.lastPayload.insert row.nodeId payload)
  let controlSpent := prior.map (·.controlSpent) |>.getD false
  let nowPauseParked := row.classPaused || (pauseParked && row.after.pacer = .parked)
  let pauseParkedIndex :=
    match row.dataClass with
    | none => track.pauseParkedIndex
    | some dataClass =>
        if nowPauseParked == pauseParked then track.pauseParkedIndex
        else
          let slot := (row.nodeId, dataClass)
          let flows := track.pauseParkedIndex.getD slot ∅
          track.pauseParkedIndex.insert slot
            (if nowPauseParked then flows.insert row.flowId else flows.erase row.flowId)
  pure
    { track with
      pauseParkedIndex := pauseParkedIndex
      flows := track.flows.insert source
        { config := row.config, state := row.after, rateBps := rateAfterBytes,
          controlSpent := controlSpent,
          controlNs := match bytes with
            | some d => some d.after.nextControlTimeNs
            | none => controlNs
          -- Set by a paused tick; cleared by any restart (the pacer leaves `parked`).
          pauseParked := nowPauseParked
          dataClass := row.dataClass }
      lastPayload := lastPayload
      stop := stop }

/-- A DCQCN controller row of a queue pair between its RoCE rows: the rate it leaves, the status
recomputation (amended §5 step 2), and D2. -/
def checkDcqcnItem (track : SenderTrack) (d : DcqcnEventLog.Row) : Except String SenderTrack := do
  let at_ := requireAt "dcqcn" d.srcLine
  let source := (d.nodeId, d.flowId)
  match track.flows.get? source with
  | none =>
      throw s!"dcqcn: line {d.srcLine}: DCQCN row of a queue pair before its first RoCE tick (node_id={d.nodeId}, flow_id={d.flowId})"
  | some flow =>
      at_ (d.before.currentRateBps = flow.rateBps)
        "DCQCN rate differs from the queue pair's tracked controller rate"
      at_ (d.kind != .bytes) "DCQCN byte opportunity without a RoCE tick at its event key"
      checkPending "dcqcn" d.srcLine source
        { flow with controlNs := some d.before.nextControlTimeNs } d.key.timeNs
      let completed := flow.state.sndUna ≥ flow.config.totalBytes
      if d.kind = .control then
        at_ (!flow.controlSpent)
          s!"DCQCN control tick re-armed after queue-pair completion (D2) (node_id={d.nodeId}, flow_id={d.flowId})"
      let state := Roce.onRateChange flow.config d.after.currentRateBps flow.state
      pure { track with
        flows := track.flows.insert source
          { flow with
            state := state
            rateBps := d.after.currentRateBps
            controlSpent := flow.controlSpent || (d.kind = .control && completed)
            controlNs := some d.after.nextControlTimeNs } }

/-- `(time, node, flow, rank)` lexicographic order, to name the earliest unfired event whatever the
hash map's iteration order. -/
def earlierPending (a b : Nat × Nat × Nat × Nat × String) : Bool :=
  a.1 < b.1 || (a.1 = b.1 && (a.2.1 < b.2.1 || (a.2.1 = b.2.1 &&
    (a.2.2.1 < b.2.2.1 || (a.2.2.1 = b.2.2.1 && a.2.2.2.1 < b.2.2.2.1)))))

/--
At the end of the log, no pair may hold a pending event at or before the stop time: the run would
have executed it. Without a given stop time, the bound is the latest time the log shows to be at or
before the stop (any logged event, any armed tick).
-/
def checkPendingAtEnd (stopTimeNs : Option Nat) (rows : List SenderRow)
    (dcqcn : List DcqcnEventLog.Row) (track : SenderTrack) : Except String Unit := do
  let latestRow := rows.foldl (fun acc row => max acc row.key.timeNs) 0
  let latestDcqcn := dcqcn.foldl (fun acc d => max acc d.key.timeNs) 0
  let latestArmed := (track.stop.latestWithin.map (·.1)).getD 0
  let bound := stopTimeNs.getD (max latestRow (max latestDcqcn latestArmed))
  let mut earliest : Option (Nat × Nat × Nat × Nat × String) := none
  for (source, flow) in track.flows.toList do
    for (pendingNs, rank, name) in pendingEvents flow do
      if pendingNs ≤ bound then
        let candidate := (pendingNs, source.1, source.2, rank, name)
        earliest :=
          match earliest with
          | some current => if earlierPending candidate current then some candidate else some current
          | none => some candidate
  match earliest with
  | none => pure ()
  | some (pendingNs, node, flow, _, name) =>
      match stopTimeNs with
      | some stop =>
          throw s!"sender: pending {name} at {pendingNs} never fired by stop_time_ns={stop} (node_id={node}, flow_id={flow})"
      | none =>
          throw s!"sender: pending {name} at {pendingNs} never fired although the log implies stop_time_ns >= {bound} (node_id={node}, flow_id={flow})"

/-- The smallest flow id of a set, independent of the set's iteration order. -/
def minFlow (flows : Std.HashSet Nat) : Option Nat :=
  flows.fold (fun least flow => some (match least with | some l => min l flow | none => flow)) none

/-- Amendment 3 completeness: a RESUME's expected `resume` rows have all appeared once the log has
moved past its key. -/
def settleExpectation (track : SenderTrack) (key : Option DaysExecutor.EventKey) :
    Except String SenderTrack := do
  match track.expectation with
  | none => pure track
  | some expected =>
      if key = some expected.key then pure track
      else
        match minFlow expected.remaining with
        | some flow =>
            throw s!"pfc: line {expected.pfcLine}: host RESUME of data_class {expected.dataClass} at node {expected.node} did not restart pause-parked queue pair (flow_id={flow})"
        | none => pure { track with expectation := none }

/--
Amendment 3: a host PFC control record at a queue pair's host. The host's class `p` is paused while
any of its controlled links has a non-empty controller set. A record that ends the pause (a host
RESUME) expects a `resume` row, at its key, for every pair of class `p` at that node that a paused
tick parked and that the restart rule would restart (`restart_roce_pacer`: parked, `next_psn <
total`, `snd_una < total`).
-/
def checkPfcItem (track : SenderTrack) (row : MechanismEventLog.PfcLog.Row) :
    Except String SenderTrack := do
  let track ← settleExpectation track (some row.key)
  let slot := (row.nodeId, row.priority)
  let link := (row.nodeId, row.controlledLink, row.priority)
  let pausedBefore := track.hostPausedLinks.getD slot 0
  let assertedBefore := track.hostAsserted.getD link false
  let assertedAfter := !row.afterControllers.isEmpty
  let pausedAfter :=
    if assertedBefore == assertedAfter then pausedBefore
    else if assertedAfter then pausedBefore + 1
    else pausedBefore - 1
  let track :=
    { track with
      hostAsserted := track.hostAsserted.insert link assertedAfter
      hostPausedLinks := track.hostPausedLinks.insert slot pausedAfter }
  if pausedBefore > 0 && pausedAfter = 0 then
    let parked := track.pauseParkedIndex.getD slot ∅
    let restartable := parked.fold (fun acc flow =>
      match track.flows.get? (row.nodeId, flow) with
      | some pair =>
          if pair.state.pacer = .parked && pair.state.nextPsn < pair.config.totalBytes &&
              pair.state.sndUna < pair.config.totalBytes then acc.insert flow else acc
      | none => acc) (∅ : Std.HashSet Nat)
    pure { track with
      expectation := some
        { key := row.key, node := row.nodeId, dataClass := row.priority, pfcLine := row.srcLine,
          remaining := restartable } }
  else
    pure track

/-- Amendment 3: the warrants of a sender row against the host PFC state at its key. -/
def checkWarrant (track : SenderTrack) (row : SenderRow) : Except String SenderTrack := do
  let at_ := requireAt "sender" row.srcLine
  match row.dataClass with
  | none => pure track
  | some dataClass =>
      let paused := track.hostPausedLinks.getD (row.nodeId, dataClass) 0 > 0
      match row.kind with
      | .tick =>
          if row.classPaused then
            at_ paused s!"RoCE paused tick while data_class {dataClass} is not paused at node {row.nodeId}"
          else
            at_ (!paused) s!"RoCE unpaused tick while data_class {dataClass} is paused at node {row.nodeId}"
          pure track
      | .resume =>
          match track.expectation with
          | some expected =>
              at_ (expected.key = row.key && expected.node = row.nodeId &&
                  expected.dataClass = dataClass)
                s!"RoCE resume row without a host RESUME of data_class {dataClass} at node {row.nodeId} at this event key"
              at_ (expected.remaining.contains row.flowId)
                s!"RoCE resume row of a queue pair the RESUME did not find pause-parked (flow_id={row.flowId})"
              pure { track with
                expectation := some { expected with remaining := expected.remaining.erase row.flowId } }
          | none =>
              throw s!"sender: line {row.srcLine}: RoCE resume row without a host RESUME of data_class {dataClass} at node {row.nodeId} at this event key"
      | _ => pure track

/--
Canonical order of the sender log: strictly increasing event keys, except that the `resume` rows
of one RESUME share its key (Amendment 2), which is allowed only for rows of one node with
strictly increasing `flow_id`.
-/
def checkSenderKeyOrder : List SenderRow → Except String Unit
  | [] | [_] => pure ()
  | first :: second :: rest => do
      if first.key = second.key && (first.kind = .resume || second.kind = .resume) then
        requireAt "sender" second.srcLine
          (first.kind = .resume && second.kind = .resume && first.nodeId = second.nodeId &&
            first.flowId < second.flowId)
          "RoCE resume rows sharing an event key must be of one node in strictly increasing flow_id order"
      else
        requireAt "sender" second.srcLine (first.key < second.key)
          "duplicate or backward canonical event key"
      checkSenderKeyOrder (second :: rest)

/--
The sender log against §5, joined with the DCQCN controller log of the same run. `stopTimeNs` is
the image's stop time when known; without it, one stop time must separate every armed tick from
every stopped one.
-/
def checkSenderRows (stopTimeNs : Option Nat) (rows : List SenderRow)
    (dcqcn : List DcqcnEventLog.Row) (pfc : Option (List MechanismEventLog.PfcLog.Row)) :
    Except String Unit := do
  requireAt "sender" 1 (!rows.isEmpty) "empty RoCE sender trace"
  checkSenderKeyOrder rows
  -- Amendment 3 input rules: an amended log is checked against its PFC log, and pause and resume
  -- rows are only accepted with their warrants.
  let amended := rows.any (·.dataClass.isSome)
  if !amended then
    for row in rows do
      requireAt "sender" row.srcLine (!row.classPaused && row.kind != .resume)
        "class_paused and resume rows need the data_class column and the PFC log (Amendment 3)"
    if pfc.isSome then
      throw "sender: --pfc needs a sender log with the data_class column (Amendment 3)"
  else if pfc.isNone then
    throw "sender: the log carries data_class (Amendment 3); pass its PFC log with --pfc"
  let pfcRows ← match pfc with
    | none => pure []
    | some pfcRows => do
        inRole "pfc" (MechanismEventLog.PfcLog.checkRows pfcRows)
        inRole "pfc" (MechanismEventLog.PfcLog.canonicalize pfcRows)
  -- The controller log is checked on its own terms first (an empty one only when no queue pair
  -- ever sent a byte and no controller event happened).
  if !dcqcn.isEmpty then inRole "dcqcn" (DcqcnEventLog.checkRows dcqcn)
  let mut pairs : Std.HashSet (Nat × Nat) := ∅
  for row in rows do pairs := pairs.insert (row.nodeId, row.flowId)
  let pairRows := dcqcn.filter (fun d => pairs.contains (d.nodeId, d.flowId))
  -- A run executes no event after its stop time.
  if let some stop := stopTimeNs then
    for row in rows do
      requireAt "sender" row.srcLine (row.key.timeNs ≤ stop) s!"event after stop_time_ns={stop}"
    for d in dcqcn do
      requireAt "dcqcn" d.srcLine (d.key.timeNs ≤ stop) s!"event after stop_time_ns={stop}"
  let hosts := pairs.fold (fun acc (node, _) => acc.insert node) (∅ : Std.HashSet Nat)
  let hostControl := pfcRows.filter (fun p => p.kind = .control && hosts.contains p.nodeId)
  let mut track : SenderTrack := {}
  for item in mergePfc hostControl (mergeLogs rows pairRows []) [] do
    match item with
    | .sender row bytes =>
        -- A resume row is checked against the expectation of its own key before anything else
        -- settles it; any other row first settles the expectation of an earlier RESUME.
        if row.kind != .resume then track ← settleExpectation track (some row.key)
        track ← checkSenderItem stopTimeNs track row bytes
        track ← checkWarrant track row
    | .dcqcn d =>
        track ← settleExpectation track (some d.key)
        track ← checkDcqcnItem track d
    | .pfc p => track ← checkPfcItem track p
  track ← settleExpectation track none
  match track.stop.latestWithin, track.stop.earliestBeyond with
  | some (within, withinLine), some (beyond, beyondLine) =>
      if within < beyond then pure ()
      else throw s!"sender: no single stop time fits the pacer decisions: tick {within} (line {withinLine}) is armed and tick {beyond} (line {beyondLine}) is stopped"
  | _, _ => pure ()
  checkPendingAtEnd stopTimeNs rows dcqcn track

/-! ## Cross-role invariants (both logs and the controller log of one run) -/

/-- Merges two time-ordered logs; on equal times the left (sending) side comes first, so a packet
sent and consumed at the same instant counts as sent before it is consumed. Linear. -/
def mergeByTime {α β : Type} (ta : α → Nat) (tb : β → Nat) :
    List α → List β → List (α ⊕ β) → List (α ⊕ β)
  | [], [], acc => acc.reverse
  | a :: as, [], acc => mergeByTime ta tb as [] (.inl a :: acc)
  | [], b :: bs, acc => mergeByTime ta tb [] bs (.inr b :: acc)
  | a :: as, b :: bs, acc =>
      if ta a ≤ tb b then mergeByTime ta tb as (b :: bs) (.inl a :: acc)
      else mergeByTime ta tb (a :: as) bs (.inr b :: acc)
termination_by as bs => as.length + bs.length

/-- Consumes one unit of `key` from a multiset of sent packets. -/
def consume {κ : Type} [BEq κ] [Hashable κ] (sent : Std.HashMap κ Nat) (key : κ) :
    Option (Std.HashMap κ Nat) :=
  match sent.get? key with
  | some (count + 1) => some (sent.insert key count)
  | _ => none

/--
The invariants only the joined logs show (every check is a hash-map step per row):

* every data arrival at a receiver is a packet its sender emitted at `packet_sent_time_ns`, with
  the same PSN, size and retransmission bit, and no emission arrives twice;
* every ACK and NACK the sender applies was sent earlier by the pair's receiver with that value
  (so `snd_una` advances only to values a receiver's frontier carried), each at most once;
* every CNP the controller applies for a pair was sent earlier by its receiver, each at most once;
* the receiver's total equals the sender's, and a receiver drops duplicates silently only when
  the sender's timeout is off (D7).
-/
def checkCrossRole (sender : List SenderRow) (receiver : List ReceiverRow)
    (dcqcn : List DcqcnEventLog.Row) : Except String Unit := do
  -- Configuration agreement per flow.
  let mut senderConfig : Std.HashMap Nat Roce.SenderConfig := ∅
  for row in sender do senderConfig := senderConfig.insert row.flowId row.config
  let mut emissions : Std.HashMap (Nat × Nat) Roce.Emission := ∅
  for row in sender do
    if let some emission := row.emission then
      emissions := emissions.insert (row.flowId, row.key.timeNs) emission
  let mut delivered : Std.HashSet (Nat × Nat) := ∅
  for row in receiver do
    let at_ := requireAt "receiver" row.srcLine
    match senderConfig.get? row.flowId with
    | none => throw s!"receiver: line {row.srcLine}: RoCE receiver of a flow with no sender rows (flow_id={row.flowId})"
    | some config =>
        at_ (row.config.totalBytes = config.totalBytes)
          s!"RoCE receiver total_bytes differs from the sender's (flow_id={row.flowId})"
        at_ (row.config.duplicateAck || config.rtoNs = 0)
          s!"RoCE receiver drops duplicates silently while the sender's timeout is on (D7) (flow_id={row.flowId})"
    let sent := (row.flowId, row.packetSentTimeNs)
    match emissions.get? sent with
    | none =>
        throw s!"receiver: line {row.srcLine}: RoCE data arrival matches no sender emission (flow_id={row.flowId}, sent_time_ns={row.packetSentTimeNs})"
    | some emission =>
        at_ (emission.psn = row.packet.psn && emission.bytes = row.packet.bytes &&
            emission.retransmission = row.packetRetransmission)
          s!"RoCE data arrival differs from the sender's emission (flow_id={row.flowId}, sent_time_ns={row.packetSentTimeNs})"
        at_ (!delivered.contains sent)
          s!"RoCE emission delivered twice (flow_id={row.flowId}, sent_time_ns={row.packetSentTimeNs})"
    delivered := delivered.insert sent
  -- Feedback: (flow, is NACK, value) sent by receivers before the sender consumes it.
  let feedback := sender.filter (fun row => row.kind = .ack || row.kind = .nack)
  let mut acks : Std.HashMap (Nat × Bool × Nat) Nat := ∅
  for item in mergeByTime (·.key.timeNs) (·.key.timeNs) receiver feedback [] do
    match item with
    | .inl row =>
        if let some value := row.feedbackAcknowledgment then
          let key := (row.flowId, decide (row.action = .nack), value)
          acks := acks.insert key (acks.getD key 0 + 1)
    | .inr row =>
        let value := row.inputAcknowledgment.getD 0
        let nack := decide (row.kind = .nack)
        match consume acks (row.flowId, nack, value) with
        | some rest => acks := rest
        | none =>
            throw s!"sender: line {row.srcLine}: RoCE {if nack then "NACK" else "ACK"} carries a value no receiver sent before it (flow_id={row.flowId}, acknowledgment={value})"
  -- CNPs of queue pairs: sent by the receiver's notification point before the controller applies them.
  let mut pairs : Std.HashSet (Nat × Nat) := ∅
  for row in sender do pairs := pairs.insert (row.nodeId, row.flowId)
  let applied := dcqcn.filter (fun d => d.kind = .cnp && pairs.contains (d.nodeId, d.flowId))
  let mut cnps : Std.HashMap Nat Nat := ∅
  for item in mergeByTime (·.key.timeNs) (·.key.timeNs) receiver applied [] do
    match item with
    | .inl row =>
        if row.cnpSent then cnps := cnps.insert row.flowId (cnps.getD row.flowId 0 + 1)
    | .inr d =>
        match consume cnps d.flowId with
        | some rest => cnps := rest
        | none =>
            throw s!"dcqcn: line {d.srcLine}: DCQCN CNP of a queue pair that no receiver sent before it (flow_id={d.flowId})"

/-- The three logs of one run: each on its own terms, then the cross-role invariants. -/
def checkTrace (stopTimeNs : Option Nat) (sender : List SenderRow) (receiver : List ReceiverRow)
    (dcqcn : List DcqcnEventLog.Row) (pfc : Option (List MechanismEventLog.PfcLog.Row)) :
    Except String Unit := do
  inRole "receiver" (checkReceiverRows receiver)
  checkSenderRows stopTimeNs sender dcqcn pfc
  checkCrossRole sender receiver dcqcn

end LeanGuard.P10c.RoceEventLog
