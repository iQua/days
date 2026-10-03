import DaysExecutor.Event
import LeanGuard.P10c.Dcqcn.Semantics
import LeanGuard.Shared.Check
import LeanGuard.Shared.Csv

namespace LeanGuard.P10c.DcqcnEventLog

/-! Replay checker of `dcqcn_transitions_csv` (pinned schema
`days-gpu/plans/briefs/p16/dcqcn-schema.md`). Every row is recomputed from its `before` state:
a `feedback` row is `onFeedback(before, time)`, any other row `settle(before, bound)` when it froze
its flow and `materialize(before, bound)` otherwise; the bound is the event time for an arrival
(phase 0) and the next nanosecond for a timer (phase 1). Rows are in (event key, flow) order, each
source's first row starts from the pristine controller, each later row continues the previous one,
and a frozen source has no later row. -/

open LeanGuard.Shared
open LeanGuard.P10c

inductive Kind
  | feedback
  | tick
  | advance
  deriving DecidableEq, Repr

def parseKind : String → Except String Kind
  | "feedback" => pure .feedback
  | "tick" => pure .tick
  | "advance" => pure .advance
  | other => throw s!"invalid DCQCN row kind: '{other}'"

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

structure Row where
  key : DaysExecutor.EventKey
  nodeId : Nat
  flowId : Nat
  kind : Kind
  boundNs : Nat
  frozen : Bool
  counts : Dcqcn.Counts
  config : Dcqcn.Config
  before : Dcqcn.State
  after : Dcqcn.State
  srcLine : Nat
  deriving Repr

def parseState
    (fieldPrefix : String)
    (idx : Std.HashMap String Nat)
    (fields : Array String) : Except String Dcqcn.State := do
  let field := fun name => getField idx fields s!"{fieldPrefix}_{name}"
  pure
    { alphaQ63 := ← parseU64 (← field "alpha_q63")
      currentRateBps := ← parseU64 (← field "current_rate_bps")
      targetRateBps := ← parseU64 (← field "target_rate_bps")
      nextAlphaNs := ← parseU64 (← field "next_alpha_ns")
      nextDecreaseNs := ← parseU64 (← field "next_decrease_ns")
      nextIncreaseNs := ← parseU64 (← field "next_increase_ns")
      stage := ← parseU64 (← field "stage")
      armed := ← parseBit (← field "armed")
      alphaPending := ← parseBit (← field "alpha_pending")
      decreasePending := ← parseBit (← field "decrease_pending")
      increaseArmed := ← parseBit (← field "increase_armed") }

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let field := getField idx fields
  let result : Except String Row := do
    pure
      { key :=
          { timeNs := ← parseU64 (← field "time_ns")
            phase := ← parseU64 (← field "event_phase")
            originNode := ← parseU64 (← field "event_origin_node")
            originSeq := ← parseU64 (← field "event_origin_sequence") }
        nodeId := ← parseU64 (← field "node_id")
        flowId := ← parseU64 (← field "flow_id")
        kind := ← parseKind (← field "kind")
        boundNs := ← parseU64 (← field "bound_ns")
        frozen := ← parseBit (← field "frozen")
        counts :=
          { alphaTicks := ← parseU64 (← field "alpha_ticks")
            increaseFires := ← parseU64 (← field "increase_fires")
            decreaseCuts := ← parseU64 (← field "decrease_cuts") }
        config :=
          { initialRateBps := ← parseU64 (← field "initial_rate_bps")
            minimumRateBps := ← parseU64 (← field "minimum_rate_bps")
            maximumRateBps := ← parseU64 (← field "maximum_rate_bps")
            additiveRateBps := ← parseU64 (← field "additive_rate_bps")
            hyperRateBps := ← parseU64 (← field "hyper_rate_bps")
            gQ63 := ← parseU64 (← field "g_q63")
            alphaIntervalNs := ← parseU64 (← field "alpha_interval_ns")
            decreaseIntervalNs := ← parseU64 (← field "decrease_interval_ns")
            increaseIntervalNs := ← parseU64 (← field "increase_interval_ns")
            fastRecoverySteps := ← parseU64 (← field "fast_recovery_steps")
            clampTargetRate := ← parseBit (← field "clamp_target_rate") }
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

/-- Rows are ordered by (event key, flow); one event yields at most one row per flow. -/
def checkOrder : List Row → Except String Unit
  | [] | [_] => pure ()
  | first :: second :: rest => do
      require second.srcLine
        (first.key < second.key || (first.key = second.key && first.flowId < second.flowId))
        "duplicate or backward (event key, flow)"
      checkOrder (second :: rest)

/-- The transition a row must show, recomputed from its `before` state. -/
def expected (row : Row) : Dcqcn.State × Dcqcn.Counts :=
  match row.kind with
  | .feedback => Dcqcn.onFeedback row.config row.before row.key.timeNs
  | .tick | .advance =>
      if row.frozen then Dcqcn.settle row.config row.before row.boundNs
      else Dcqcn.materialize row.config row.before row.boundNs

def checkRow (row : Row) : Except String Unit := do
  require row.srcLine (Dcqcn.validConfig row.config) "invalid DCQCN configuration"
  require row.srcLine (Dcqcn.validState row.config row.before) "invalid DCQCN before-state"
  require row.srcLine (Dcqcn.validState row.config row.after) "invalid DCQCN after-state"
  require row.srcLine (row.key.phase ≤ 1) "DCQCN row from a phase-2 event"
  require row.srcLine (row.boundNs = row.key.timeNs + row.key.phase)
    "DCQCN bound is not the event time (arrival) or the next nanosecond (timer)"
  match row.kind with
  | .feedback =>
      require row.srcLine (row.key.phase = 0) "DCQCN feedback must be an arrival (phase 0)"
      require row.srcLine (!row.frozen) "DCQCN feedback on a frozen controller"
  | .tick =>
      require row.srcLine (row.key.phase = 1) "DCQCN pacing tick must have phase 1"
  | .advance => pure ()
  let (state, counts) := expected row
  require row.srcLine (row.after = state) "DCQCN after-state mismatch"
  require row.srcLine (row.counts = counts) "DCQCN transition counts mismatch"
  if row.kind = .advance then
    require row.srcLine (row.frozen || 0 < counts.increaseFires + counts.decreaseCuts)
      "DCQCN advance row applied no rate instant and froze nothing"

structure SourceState where
  config : Dcqcn.Config
  after : Dcqcn.State
  frozen : Bool

/-- One pass in row order, keyed by (node, flow): a row first continues its source (the pristine
controller for the first row, the previous `after` otherwise, never after a freeze), then obeys
its own transition rule, so the first rejected line is the earliest violation. -/
def checkRows (rows : List Row) : Except String Unit := do
  require 1 (!rows.isEmpty) "empty DCQCN trace"
  checkOrder rows
  let mut sources : Std.HashMap (Nat × Nat) SourceState := {}
  for row in rows do
    let source := (row.nodeId, row.flowId)
    match sources.get? source with
    | none =>
        require row.srcLine (row.before = Dcqcn.pristine row.config)
          s!"DCQCN first state is not pristine for source (node_id={row.nodeId}, flow_id={row.flowId})"
    | some prior =>
        require row.srcLine (!prior.frozen)
          s!"DCQCN row after the freeze of source (node_id={row.nodeId}, flow_id={row.flowId})"
        require row.srcLine (prior.config = row.config)
          s!"DCQCN config discontinuity for source (node_id={row.nodeId}, flow_id={row.flowId})"
        require row.srcLine (prior.after = row.before)
          s!"DCQCN state discontinuity for source (node_id={row.nodeId}, flow_id={row.flowId})"
    checkRow row
    sources := sources.insert source { config := row.config, after := row.after, frozen := row.frozen }

/-! ## The CNP join for unreliable flows (P16 D1 fix round 1)

`dcqcn_cnp_arrivals_csv` lists every CNP the executor delivered to a reaction point
(`time_ns,flow_id,payload`). An unreliable flow (one with `tick` rows, ruling D17) applies a CNP
that arrives at or before its freeze, the `frozen` row of its finishing or stopping tick: a CNP is a
phase-0 arrival, so at the freezing tick's own instant it still applies. Each such CNP is exactly
one `feedback` row of its flow at its time, and every `feedback` row of an unreliable flow is one
such CNP. A CNP after the freeze is ignored (ruling D11); the row checks above already reject any
row after a freeze. Queue pairs get no CNP (ruling D4): their feedback is the ECN echo, joined by
the RoCE checker. -/

structure CnpArrival where
  timeNs : Nat
  flowId : Nat
  payload : Nat
  srcLine : Nat
  deriving Repr

def parseCnpCsv (content : String) : Except String (List CnpArrival) := do
  let lines :=
    content.splitOn "\n" |>.map stripCR |>.map String.trim |>.filter (· != "")
  match lines with
  | [] => throw "empty CNP arrivals CSV"
  | header :: data =>
      let idx := mkIndex (splitCsvLine header)
      let rec go (lineNo : Nat) (remaining : List String) (rows : List CnpArrival) := do
        match remaining with
        | [] => pure rows.reverse
        | line :: rest =>
            let fields := (splitCsvLine line).toArray
            let row : Except String CnpArrival := do
              pure
                { timeNs := ← parseU64 (← getField idx fields "time_ns")
                  flowId := ← parseU64 (← getField idx fields "flow_id")
                  payload := ← parseU64 (← getField idx fields "payload")
                  srcLine := lineNo }
            match row with
            | .ok row => go (lineNo + 1) rest (row :: rows)
            | .error error => throw s!"line {lineNo}: {error}"
      go 2 data []

/-- `(time, flow, payload)` order, strictly increasing (a payload arrives once). -/
def checkCnpOrder : List CnpArrival → Except String Unit
  | [] | [_] => pure ()
  | first :: second :: rest => do
      let lt := first.timeNs < second.timeNs ||
        (first.timeNs = second.timeNs && (first.flowId < second.flowId ||
          (first.flowId = second.flowId && first.payload < second.payload)))
      require second.srcLine lt "duplicate or backward (time, flow, payload)"
      checkCnpOrder (second :: rest)

def inRole {α : Type} (role : String) (result : Except String α) : Except String α :=
  match result with
  | .ok value => pure value
  | .error error => throw s!"{role}: {error}"

def checkCnpJoin (rows : List Row) (cnps : List CnpArrival) : Except String Unit := do
  inRole "cnp" (checkCnpOrder cnps)
  let mut unreliable : Std.HashSet Nat := ∅
  let mut freeze : Std.HashMap Nat Nat := ∅
  for row in rows do
    if row.kind = .tick then unreliable := unreliable.insert row.flowId
    if row.frozen then freeze := freeze.insert row.flowId row.key.timeNs
  -- The unreliable flows' feedback rows, as a multiset over (time, flow).
  let mut feedback : Std.HashMap (Nat × Nat) Nat := ∅
  for row in rows do
    if row.kind = .feedback && unreliable.contains row.flowId then
      let key := (row.key.timeNs, row.flowId)
      feedback := feedback.insert key (feedback.getD key 0 + 1)
  for cnp in cnps do
    if !unreliable.contains cnp.flowId then
      throw s!"cnp: line {cnp.srcLine}: CNP arrival for flow {cnp.flowId}, which has no DCQCN tick rows (not an unreliable DCQCN flow)"
    let live := (freeze.get? cnp.flowId).all (cnp.timeNs ≤ ·)
    if live then
      let key := (cnp.timeNs, cnp.flowId)
      match feedback.getD key 0 with
      | 0 =>
          throw s!"cnp: line {cnp.srcLine}: CNP arrival at {cnp.timeNs} before the freeze of flow {cnp.flowId} has no DCQCN feedback row"
      | count + 1 => feedback := feedback.insert key count
  for row in rows do
    if row.kind = .feedback && unreliable.contains row.flowId &&
        feedback.getD (row.key.timeNs, row.flowId) 0 > 0 then
      throw s!"dcqcn: line {row.srcLine}: DCQCN feedback row of unreliable flow {row.flowId} with no CNP arrival at its time"

end LeanGuard.P10c.DcqcnEventLog
