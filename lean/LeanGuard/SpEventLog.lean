import Std

import DaysExecutor.Event
import LeanGuard.Shared.Check
import LeanGuard.Shared.Coverage
import LeanGuard.Shared.Csv
import LeanGuard.Shared.TraceSpec
import LeanGuard.Sp.Semantics

namespace LeanGuard.SpEventLog

open LeanGuard.Shared
open LeanGuard.Sp.Semantics

/-- Stable executor SP certificate row carrying the full canonical event key. -/
structure Row where
    timeNs : Nat
    eventPhase : Nat
    originNode : Nat
    originSeq : Nat
    kind : Kind
    schedulerId : Nat
    classCount : Nat
    packetId : Nat
    flowId : Nat
    classId : Nat
    priority : Nat
    sizeBytes : Nat
    departureTimeNs : Option Nat
    srcLine : Nat
deriving DecidableEq, Repr

def key (r : Row) : DaysExecutor.EventKey :=
    { timeNs := r.timeNs
      phase := r.eventPhase
      originNode := r.originNode
      originSeq := r.originSeq }

def keyLt (a b : DaysExecutor.EventKey) : Bool :=
    decide (a < b)

def parseKind (s : String) : Except String Kind :=
    match s with
    | "enqueue" => pure .enqueue
    | "schedule" => pure .schedule
    | "depart" => pure .depart
    | other => throw s!"invalid kind: {other}"

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
    let res : Except String Row := do
        let timeNs ← parseNat (← getField idx fields "time_ns")
        let eventPhase ← parseNat (← getField idx fields "event_phase")
        let originNode ← parseNat (← getField idx fields "origin_node")
        let originSeq ← parseNat (← getField idx fields "origin_seq")
        let kind ← parseKind (← getField idx fields "kind")
        let schedulerId ← parseNat (← getField idx fields "scheduler_id")
        let classCount ← parseNat (← getField idx fields "class_count")
        let packetId ← parseNat (← getField idx fields "packet_id")
        let flowId ← parseNat (← getField idx fields "flow_id")
        let classId ← parseNat (← getField idx fields "class_id")
        let priority ← parseNat (← getField idx fields "priority")
        let sizeBytes ← parseNat (← getField idx fields "size_bytes")
        let departureTimeNs ← parseOpt parseNat (← getField idx fields "departure_time_ns")
        pure
            { timeNs
              eventPhase
              originNode
              originSeq
              kind
              schedulerId
              classCount
              packetId
              flowId
              classId
              priority
              sizeBytes
              departureTimeNs
              srcLine := lineNo }
    match res with
    | .ok row => pure row
    | .error error => throw s!"line {lineNo}: {error}"

def parseCsv (content : String) : Except String (List Row) := do
    let lines :=
        content.splitOn "\n" |>.map stripCR |>.map (fun line => line.trim) |>.filter (· != "")
    match lines with
    | [] => throw "empty CSV"
    | header :: data =>
        let idx := mkIndex (splitCsvLine header)
        let rec go (lineNo : Nat) (remaining : List String) (acc : List Row) :
            Except String (List Row) := do
            match remaining with
            | [] => pure acc.reverse
            | line :: rest => do
                let row ← parseRow lineNo idx (splitCsvLine line).toArray
                go (lineNo + 1) rest (row :: acc)
        go 2 data []

def toEvent (r : Row) : Event :=
    { key := key r
      kind := r.kind
      schedulerId := r.schedulerId
      classCount := r.classCount
      packetId := r.packetId
      flowId := r.flowId
      classId := r.classId
      priority := r.priority
      sizeBytes := r.sizeBytes
      departureTimeNs := r.departureTimeNs }

def canonicalizeRows (rows : List Row) : Except String (List Row) := do
    let sorted := rows.toArray.qsort (fun a b => keyLt (key a) (key b)) |>.toList
    let rec checkKeys : List Row → Except String Unit
        | [] => pure ()
        | [_] => pure ()
        | first :: second :: rest => do
            if key first = key second then
                throw
                    s!"duplicate key at lines {first.srcLine} and {second.srcLine}: (time_ns={(key first).timeNs}, event_phase={(key first).phase}, origin_node={(key first).originNode}, origin_seq={(key first).originSeq})"
            require second.srcLine (keyLt (key first) (key second))
                "canonical key order violated"
            checkKeys (second :: rest)
    checkKeys sorted
    pure sorted

def hasLowerPriority (priority : Nat) : List QueuedPacket → Bool
    | [] => false
    | packet :: rest => packet.priority < priority || hasLowerPriority priority rest

def hasEqualPriorityAfter (selected : Nat × Nat) (priority : Nat) : List QueuedPacket → Bool
    | [] => false
    | packet :: rest =>
        if packet.key = selected then
            rest.any (fun later => later.priority = priority)
        else
            hasEqualPriorityAfter selected priority rest

def recordCover (coverage : CoverageState) (g : Global) (r : Row) : CoverageState :=
    let st := g.schedulers.getD r.schedulerId {}
    let coverage := covSeen (covTick coverage) r.classId
    let coverage := if r.classCount ≥ 2 then covHit coverage "class_count_ge_2" else coverage
    match r.kind with
    | .enqueue => covHit coverage "enqueue"
    | .schedule =>
        let coverage := if st.queue.isEmpty then coverage else covHit coverage "schedule_nonempty"
        let coverage :=
            if hasLowerPriority r.priority st.queue then
                covHit coverage "higher_priority_preempts"
            else
                coverage
        let coverage :=
            if hasEqualPriorityAfter (packetKey r.flowId r.packetId) r.priority st.queue then
                covHit coverage "equal_priority_fifo_tie"
            else
                coverage
        if st.queue.length = 1 then covHit coverage "queue_drained" else coverage
    | .depart => covHit coverage "depart"

structure ReplayState where
    g : Global := {}
    lastKey : Option DaysExecutor.EventKey := none
deriving Repr

def stepRow (state : ReplayState) (row : Row) : Except String ReplayState := do
    match state.lastKey with
    | none => pure ()
    | some previous => require row.srcLine (keyLt previous (key row)) "global key went backwards"
    let g' ← step row.srcLine state.g (toEvent row)
    pure { g := g', lastKey := some (key row) }

def traceSpec : TraceSpec :=
    { Row := Row
      State := ReplayState
      init := {}
      step := stepRow }

def observeCoverage (coverage : CoverageState) (state : ReplayState) (row : Row) : CoverageState :=
    recordCover coverage state.g row

def replayCanonicalRowsWithCoverage (rows : List Row) (coverage : CoverageState) :
    Except (String × CoverageState) (ReplayState × CoverageState) :=
    TraceSpec.replayWithObserverM traceSpec observeCoverage traceSpec.init rows coverage

theorem replayCanonicalRowsWithCoverage_sound {rows : List Row} {coverage : CoverageState}
    {state : ReplayState} {coverage' : CoverageState} :
    replayCanonicalRowsWithCoverage rows coverage = .ok (state, coverage') →
      TraceSpec.Replay traceSpec traceSpec.init rows state := by
    intro h
    exact TraceSpec.replayWithObserverM_sound traceSpec observeCoverage h

def checkRowsWithCoverage (rows : List Row) : CheckOutcome :=
    match canonicalizeRows rows with
    | .error error => .error (error, {})
    | .ok sorted =>
        match replayCanonicalRowsWithCoverage sorted {} with
        | .error error => .error error
        | .ok (_, coverage) => .ok coverage

theorem checkRowsWithCoverage_sound {rows : List Row} {coverage : CoverageState} :
    checkRowsWithCoverage rows = .ok coverage →
      ∃ sorted state,
        canonicalizeRows rows = .ok sorted ∧
        TraceSpec.Replay traceSpec traceSpec.init sorted state := by
    intro h
    unfold checkRowsWithCoverage at h
    cases hcanon : canonicalizeRows rows with
    | error error => simp [hcanon] at h
    | ok sorted =>
        simp [hcanon] at h
        cases hrun : replayCanonicalRowsWithCoverage sorted {} with
        | error error => simp [hrun] at h
        | ok pair =>
            rcases pair with ⟨state, coverage'⟩
            simp [hrun] at h
            exact ⟨sorted, state, by simp, replayCanonicalRowsWithCoverage_sound hrun⟩

end LeanGuard.SpEventLog
