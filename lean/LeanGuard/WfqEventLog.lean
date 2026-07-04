import Std

import LeanGuard.Shared.Check
import LeanGuard.Shared.Csv
import LeanGuard.Shared.Key
import LeanGuard.Shared.Coverage
import LeanGuard.Shared.TraceSpec
import LeanGuard.Wfq.Semantics

namespace LeanGuard.WfqEventLog

open LeanGuard.Shared
open LeanGuard.Wfq.Semantics

/-- 1:1 with a single row in `wfq_events.csv` emitted by Days under `--features lean`. -/
structure Row where
    timeNs : Nat
    eventId : Nat
    kind : Kind
    schedulerId : Nat
    packetId : Nat
    flowId : Nat
    classId : Nat
    sizeBytes : Nat
    weight : Nat
    rateBps : Nat
    vtimeNs : Nat
    finishTimeNs : Nat
    departureTimeNs : Option Nat
    srcLine : Nat
deriving DecidableEq, Repr

def key (r : Row) : Nat × Nat :=
    (r.timeNs, r.eventId)

def parseKind (s : String) : Except String Kind :=
    match s with
    | "enqueue" => pure Kind.enqueue
    | "schedule" => pure Kind.schedule
    | "depart" => pure Kind.depart
    | other => throw s!"invalid kind: {other}"

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
    let res : Except String Row := do
        let timeNs ← parseNat (← getField idx fields "time_ns")
        let eventId ← parseNat (← getField idx fields "event_id")
        let kind ← parseKind (← getField idx fields "kind")
        let schedulerId ← parseNat (← getField idx fields "scheduler_id")
        let packetId ← parseNat (← getField idx fields "packet_id")
        let flowId ← parseNat (← getField idx fields "flow_id")
        let classId ← parseNat (← getField idx fields "class_id")
        let sizeBytes ← parseNat (← getField idx fields "size_bytes")
        let weight ← parseNat (← getField idx fields "weight")
        let rateBps ← parseNat (← getField idx fields "rate_bps")
        let vtimeNs ← parseNat (← getField idx fields "vtime_ns")
        let finishTimeNs ← parseNat (← getField idx fields "finish_time_ns")
        let departureTimeNs ← parseOpt parseNat (← getField idx fields "departure_time_ns")
        pure
            { timeNs
              eventId
              kind
              schedulerId
              packetId
              flowId
              classId
              sizeBytes
              weight
              rateBps
              vtimeNs
              finishTimeNs
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
        let rec go (lineNo : Nat) (data : List String) (acc : List Row) :
            Except String (List Row) := do
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
      packetId := r.packetId
      flowId := r.flowId
      classId := r.classId
      sizeBytes := r.sizeBytes
      weight := r.weight
      rateBps := r.rateBps
      vtimeNs := r.vtimeNs
      finishTimeNs := r.finishTimeNs
      departureTimeNs := r.departureTimeNs }

def recordCover (cov : CoverageState) (g : Global) (r : Row) : CoverageState :=
    let st := g.schedulers.getD r.schedulerId {}
    let cov := covSeen cov r.classId
    let cov :=
        if cov.seen.size >= 2 then
            covHit cov "class_count_ge_2"
        else
            cov
    let cov :=
        match r.kind with
        | Kind.enqueue =>
            if !hasActive st then
                covHit cov "enqueue_when_empty"
            else
                cov
        | Kind.schedule =>
            let queueList := st.queue.toList
            let cov :=
                match queueList with
                | [] => cov
                | _ => covHit cov "schedule_nonempty"
            let cov :=
                match queueList with
                | [] => cov
                | (_, first) :: rest =>
                    let rec minFinish (best : Nat) (xs : List ((Nat × Nat) × QueuedPacket)) : Nat :=
                        match xs with
                        | [] => best
                        | (_, q) :: xs' =>
                            let best' := if q.finishTimeNs < best then q.finishTimeNs else best
                            minFinish best' xs'
                    let minVal := minFinish first.finishTimeNs rest
                    let rec countMin (target : Nat) (count : Nat)
                        (xs : List ((Nat × Nat) × QueuedPacket)) : Nat :=
                        match xs with
                        | [] => count
                        | (_, q) :: xs' =>
                            let count' := if q.finishTimeNs = target then count + 1 else count
                            countMin target count' xs'
                    let count := countMin minVal 0 queueList
                    if count >= 2 then
                        covHit cov "finish_time_tie_observed"
                    else
                        cov
            cov
        | Kind.depart =>
            let count := st.flowCounts.get? r.classId |>.getD 0
            if count = 0 then
                cov
            else
                let flowCounts := st.flowCounts.insert r.classId (count - 1)
                let stTemp := { st with flowCounts := flowCounts }
                if !hasActive stTemp then
                    covHit cov "queue_becomes_empty_after_depart"
                else
                    cov
    cov

structure ReplayState where
  g : Global := {}
  lastKey : Option (Nat × Nat) := none
deriving Repr

def stepRow (s : ReplayState) (r : Row) : Except String ReplayState := do
  match s.lastKey with
  | none => pure ()
  | some pk => require r.srcLine (keyLt pk (key r)) "global key went backwards"
  let g' ← step r.srcLine s.g (toEvent r)
  pure { g := g', lastKey := some (key r) }

def traceSpec : TraceSpec :=
  { Row := Row
    State := ReplayState
    init := {}
    step := stepRow }

def observeCoverage (cov : CoverageState) (s : ReplayState) (r : Row) : CoverageState :=
  recordCover (covTick cov) s.g r

def replayCanonicalRowsWithCoverage (rows : List Row) (cov : CoverageState) :
    Except (String × CoverageState) (ReplayState × CoverageState) :=
  TraceSpec.replayWithObserverM traceSpec observeCoverage traceSpec.init rows cov

theorem replayCanonicalRowsWithCoverage_sound {rows : List Row} {cov : CoverageState}
    {s : ReplayState} {cov' : CoverageState} :
    replayCanonicalRowsWithCoverage rows cov = .ok (s, cov') →
      TraceSpec.Replay traceSpec traceSpec.init rows s := by
  intro h
  exact TraceSpec.replayWithObserverM_sound traceSpec observeCoverage h

theorem replayCanonicalRows_preserves
    {Inv : ReplayState → Prop}
    (hstep : ∀ {s r s'}, Inv s → traceSpec.step s r = .ok s' → Inv s')
    {rows : List Row} {s : ReplayState} :
    TraceSpec.replayM traceSpec traceSpec.init rows = .ok s →
      Inv traceSpec.init → Inv s := by
  intro h hs
  exact TraceSpec.replayM_preserves traceSpec hstep h hs

def checkRowsWithCoverage (rows : List Row) : CheckOutcome :=
  match canonicalizeRows rows key (fun r => r.srcLine) with
  | .error e => .error (e, {})
  | .ok rowsSorted =>
      match replayCanonicalRowsWithCoverage rowsSorted {} with
      | .error err => .error err
      | .ok (_, cov) => .ok cov

theorem checkRowsWithCoverage_sound {rows : List Row} {cov : CoverageState} :
    checkRowsWithCoverage rows = .ok cov →
      ∃ rowsSorted s,
        canonicalizeRows rows key (fun r => r.srcLine) = .ok rowsSorted ∧
        TraceSpec.Replay traceSpec traceSpec.init rowsSorted s := by
  intro h
  unfold checkRowsWithCoverage at h
  cases hcanon : canonicalizeRows rows key (fun r => r.srcLine) with
  | error e =>
      simp [hcanon] at h
  | ok rowsSorted =>
      simp [hcanon] at h
      cases hrun : replayCanonicalRowsWithCoverage rowsSorted {} with
      | error err =>
          simp [hrun] at h
      | ok pair =>
          rcases pair with ⟨s, cov'⟩
          simp [hrun] at h
          exact
            ⟨ rowsSorted
            , s
            , by simp
            , replayCanonicalRowsWithCoverage_sound hrun ⟩

def checkRows (rows : List Row) : Except String Unit := do
    match checkRowsWithCoverage rows with
    | .ok _ => pure ()
    | .error (e, _) => throw e

end LeanGuard.WfqEventLog
