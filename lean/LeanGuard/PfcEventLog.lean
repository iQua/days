import Std

import LeanGuard.Shared.Check
import LeanGuard.Shared.Csv
import LeanGuard.Shared.Key
import LeanGuard.Shared.Coverage
import LeanGuard.Shared.TraceSpec
import LeanGuard.Pfc.Semantics

namespace LeanGuard.PfcEventLog

open LeanGuard.Shared
open LeanGuard.Pfc.Semantics

/-- 1:1 with a single row in `pfc_events.csv` emitted by Days under `--features l2_pfc,lean`. -/
structure Row where
  timeNs : Nat
  eventId : Nat
  kind : Kind
  senderId : Nat
  receiverId : Nat
  priority : Nat
  pfcFrameId : Nat
  classEnable : Nat
  pauseQuanta : Nat
  queueOccupancyBytes : Option Nat
  xoffThresholdBytes : Option Nat
  xonThresholdBytes : Option Nat
  bufferCapacityBytes : Option Nat
  refreshIntervalNs : Option Nat
  drainIntervalNs : Option Nat
  srcLine : Nat
deriving DecidableEq, Repr

def key (r : Row) : Nat × Nat :=
  (r.timeNs, r.eventId)

def parseKind (s : String) : Except String Kind :=
  match s with
  | "pfc_sent" => pure Kind.pfcSent
  | "pfc_recv" => pure Kind.pfcRecv
  | other => throw s!"invalid kind: {other}"

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let res : Except String Row := do
    let timeNs ← parseNat (← getField idx fields "time_ns")
    let eventId ← parseNat (← getField idx fields "event_id")
    let kind ← parseKind (← getField idx fields "kind")
    let senderId ← parseNat (← getField idx fields "sender_id")
    let receiverId ← parseNat (← getField idx fields "receiver_id")
    let priority ← parseNat (← getField idx fields "priority")
    let pfcFrameId ← parseNat (← getField idx fields "pfc_frame_id")
    let classEnable ← parseNat (← getField idx fields "class_enable")
    let pauseQuanta ← parseNat (← getField idx fields "pause_quanta")
    let queueOccupancyBytes ← parseOpt parseNat (← getField idx fields "queue_occupancy_bytes")
    let xoffThresholdBytes ← parseOpt parseNat (← getField idx fields "xoff_threshold_bytes")
    let xonThresholdBytes ← parseOpt parseNat (← getField idx fields "xon_threshold_bytes")
    let bufferCapacityBytes ← parseOpt parseNat (← getField idx fields "buffer_capacity_bytes")
    let refreshIntervalNs ← parseOpt parseNat (← getField idx fields "refresh_interval_ns")
    let drainIntervalNs ← parseOpt parseNat (← getField idx fields "drain_interval_ns")
    pure
      { timeNs
        eventId
        kind
        senderId
        receiverId
        priority
        pfcFrameId
        classEnable
        pauseQuanta
        queueOccupancyBytes
        xoffThresholdBytes
        xonThresholdBytes
        bufferCapacityBytes
        refreshIntervalNs
        drainIntervalNs
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
    senderId := r.senderId
    receiverId := r.receiverId
    priority := r.priority
    pfcFrameId := r.pfcFrameId
    classEnable := r.classEnable
    pauseQuanta := r.pauseQuanta
    queueOccupancyBytes := r.queueOccupancyBytes
    xoffThresholdBytes := r.xoffThresholdBytes
    xonThresholdBytes := r.xonThresholdBytes
    bufferCapacityBytes := r.bufferCapacityBytes
    srcLine := r.srcLine }

def recordCover (cov : CoverageState) (g : Global) (r : Row) : CoverageState :=
  let cov :=
    match r.priority with
    | 0 => covHit cov "prio_0_seen"
    | 1 => covHit cov "prio_1_seen"
    | 2 => covHit cov "prio_2_seen"
    | 3 => covHit cov "prio_3_seen"
    | 4 => covHit cov "prio_4_seen"
    | 5 => covHit cov "prio_5_seen"
    | 6 => covHit cov "prio_6_seen"
    | 7 => covHit cov "prio_7_seen"
    | _ => cov
  let cov :=
    match r.kind with
    | Kind.pfcSent =>
        let wasPaused := isPaused g r.senderId r.priority
        let cov :=
          if r.pauseQuanta = 0 then
            covHit cov "resume"
          else if wasPaused then
            covHit cov "pause_refresh"
          else
            covHit cov "pause_assert"
        let cov :=
          match r.queueOccupancyBytes, r.xoffThresholdBytes with
          | some occ, some xoff =>
              if occ = xoff then
                covHit cov "occ_eq_xoff"
              else if occ > xoff then
                covHit cov "occ_gt_xoff"
              else
                cov
          | _, _ => cov
        match r.queueOccupancyBytes, r.xonThresholdBytes with
        | some occ, some xon =>
            if occ = xon then
              covHit cov "occ_eq_xon"
            else if occ < xon then
              covHit cov "occ_lt_xon"
            else
              cov
        | _, _ => cov
    | Kind.pfcRecv => cov
  cov

def traceSpec : TraceSpec :=
  { Row := Row
    State := Global
    init := {}
    step := fun g r => step r.srcLine g (toEvent r) }

def observeCoverage (cov : CoverageState) (g : Global) (r : Row) : CoverageState :=
  recordCover (covTick cov) g r

def replayCanonicalRowsWithCoverage (rows : List Row) (cov : CoverageState) :
    Except (String × CoverageState) (Global × CoverageState) :=
  TraceSpec.replayWithObserverM traceSpec observeCoverage traceSpec.init rows cov

theorem replayCanonicalRowsWithCoverage_sound {rows : List Row} {cov : CoverageState}
    {g : Global} {cov' : CoverageState} :
    replayCanonicalRowsWithCoverage rows cov = .ok (g, cov') →
      TraceSpec.Replay traceSpec traceSpec.init rows g := by
  intro h
  exact TraceSpec.replayWithObserverM_sound traceSpec observeCoverage h

theorem replayCanonicalRows_preserves
    {Inv : Global → Prop}
    (hstep : ∀ {g r g'}, Inv g → traceSpec.step g r = .ok g' → Inv g')
    {rows : List Row} {g : Global} :
    TraceSpec.replayM traceSpec traceSpec.init rows = .ok g →
      Inv traceSpec.init → Inv g := by
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
      ∃ rowsSorted g,
        canonicalizeRows rows key (fun r => r.srcLine) = .ok rowsSorted ∧
        TraceSpec.Replay traceSpec traceSpec.init rowsSorted g := by
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
          rcases pair with ⟨g, cov'⟩
          simp [hrun] at h
          exact
            ⟨ rowsSorted
            , g
            , by simp
            , replayCanonicalRowsWithCoverage_sound hrun ⟩

def checkRows (rows : List Row) : Except String Unit := do
  match checkRowsWithCoverage rows with
  | .ok _ => pure ()
  | .error (e, _) => throw e

end LeanGuard.PfcEventLog
