import Std

import LeanGuard.Shared.Check
import LeanGuard.Shared.Csv
import LeanGuard.Shared.Key
import LeanGuard.Shared.Numeric
import LeanGuard.Shared.Coverage
import LeanGuard.Cubic.Semantics

namespace LeanGuard.CubicEventLog

open LeanGuard.Shared
open LeanGuard.Cubic.Semantics

inductive Kind
  | ack
  | congestion
  | timeout
deriving DecidableEq, Repr

/-- 1:1 with a single row in `cubic_events.csv` emitted by Days under a Lean trace feature. -/
structure Row where
  timeNs : Nat
  eventId : Nat
  kind : Kind
  endpointId : Nat
  flowId : Nat
  ackedSegs : Option Nat
  rttNs : Option Nat
  mssBytes : Nat
  betaPpb : Nat
  cPpb : Nat
  tcpFriendly : Bool
  fastConvergence : Bool
  initCwndBytes : Nat
  initSsthreshBytes : Nat
  cwndBytes : Nat
  ssthreshBytes : Nat
  wMaxBytes : Nat
  wLastMaxBytes : Nat
  epochStartNs : Option Nat
  srcLine : Nat
deriving DecidableEq, Repr

def key (r : Row) : Nat × Nat :=
  (r.timeNs, r.eventId)

def parseKind (s : String) : Except String Kind :=
  match s with
  | "ack" => pure Kind.ack
  | "congestion" => pure Kind.congestion
  | "timeout" => pure Kind.timeout
  | other => throw s!"invalid kind: {other}"

def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let res : Except String Row := do
    let timeNs ← parseNat (← getField idx fields "time_ns")
    let eventId ← parseNat (← getField idx fields "event_id")
    let kind ← parseKind (← getField idx fields "kind")
    let endpointId ← parseNat (← getField idx fields "endpoint_id")
    let flowId ← parseNat (← getField idx fields "flow_id")
    let ackedSegs ← parseOpt parseNat (← getField idx fields "acked_segs")
    let rttNs ← parseOpt parseNat (← getField idx fields "rtt_ns")
    let mssBytes ← parseNat (← getField idx fields "mss_bytes")
    let betaPpb ← parseNat (← getField idx fields "beta_ppb")
    let cPpb ← parseNat (← getField idx fields "c_ppb")
    let tcpFriendly ← parseBool (← getField idx fields "tcp_friendly")
    let fastConvergence ← parseBool (← getField idx fields "fast_convergence")
    let initCwndBytes ← parseNat (← getField idx fields "init_cwnd_bytes")
    let initSsthreshBytes ← parseNat (← getField idx fields "init_ssthresh_bytes")
    let cwndBytes ← parseNat (← getField idx fields "cwnd_bytes")
    let ssthreshBytes ← parseNat (← getField idx fields "ssthresh_bytes")
    let wMaxBytes ← parseNat (← getField idx fields "w_max_bytes")
    let wLastMaxBytes ← parseNat (← getField idx fields "w_last_max_bytes")
    let epochStartNs ← parseOpt parseNat (← getField idx fields "epoch_start_ns")
    pure
      { timeNs
        eventId
        kind
        endpointId
        flowId
        ackedSegs
        rttNs
        mssBytes
        betaPpb
        cPpb
        tcpFriendly
        fastConvergence
        initCwndBytes
        initSsthreshBytes
        cwndBytes
        ssthreshBytes
        wMaxBytes
        wLastMaxBytes
        epochStartNs
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

structure Global where
  flows : Std.HashMap Nat State := ∅
deriving Repr

def rowParams (r : Row) : Params :=
  { flowId := r.flowId
    mssBytes := r.mssBytes
    betaPpb := r.betaPpb
    cPpb := r.cPpb
    tcpFriendly := r.tcpFriendly
    fastConvergence := r.fastConvergence
    initCwndBytes := r.initCwndBytes
    initSsthreshBytes := r.initSsthreshBytes }

def checkUnits (lineNo : Nat) (r : Row) : Except String Unit := do
  require lineNo (r.mssBytes > 0) "mss_bytes must be > 0"

def checkSnapshot (lineNo : Nat) (st : State) (r : Row) : Except String Unit := do
  let mss := st.p.mssBytes
  let expCwndBytes := encodeBytes st.cwndSegs mss
  let expSsthreshBytes := encodeBytes st.ssthreshSegs mss
  let expWMaxBytes := encodeBytes st.wMaxSegs mss
  let expWLastMaxBytes := encodeBytes st.wLastMaxSegs mss
  require lineNo (r.cwndBytes = expCwndBytes)
    s!"cwnd_bytes mismatch: got {r.cwndBytes}, expected {expCwndBytes}"
  require lineNo (r.ssthreshBytes = expSsthreshBytes)
    s!"ssthresh_bytes mismatch: got {r.ssthreshBytes}, expected {expSsthreshBytes}"
  require lineNo (r.wMaxBytes = expWMaxBytes)
    s!"w_max_bytes mismatch: got {r.wMaxBytes}, expected {expWMaxBytes}"
  require lineNo (r.wLastMaxBytes = expWLastMaxBytes)
    s!"w_last_max_bytes mismatch: got {r.wLastMaxBytes}, expected {expWLastMaxBytes}"
  require lineNo (r.epochStartNs = st.epochStartNs) "epoch_start_ns mismatch"

def step (lineNo : Nat) (g : Global) (r : Row) : Except String Global := do
  let p := rowParams r
  require lineNo (okParams p) "invalid CUBIC parameters"
  checkUnits lineNo r

  let initCwndSegs := bytesToSegs p.initCwndBytes p.mssBytes
  let initSsthreshSegs := bytesToSegs p.initSsthreshBytes p.mssBytes

  let st :=
    match g.flows.get? r.endpointId with
    | some s => s
    | none =>
        { p := p
          cwndSegs := initCwndSegs
          ssthreshSegs := initSsthreshSegs
          wMaxSegs := 0.0
          wLastMaxSegs := 0.0
          srtt := 0.0
          epochStartNs := none
          kZero := true
          lastTimeNs := none }

  require lineNo (st.p = p) "CUBIC parameters changed for endpoint"

  match st.lastTimeNs with
  | none => pure ()
  | some prev => require lineNo (prev <= r.timeNs) s!"time went backwards: {prev} > {r.timeNs}"

  let st' ←
    match r.kind with
    | Kind.ack => do
        let ackedSegs ← requireSome lineNo "acked_segs" r.ackedSegs
        let rttNs ← requireSome lineNo "rtt_ns" r.rttNs
        require lineNo (ackedSegs > 0) "acked_segs must be > 0"
        require lineNo (rttNs > 0) "rtt_ns must be > 0"
        match st.epochStartNs with
        | some t0 => require lineNo (t0 <= r.timeNs) "epoch_start_ns is in the future"
        | none => pure ()

        let rtt := nsToSeconds rttNs
        let srtt := updateSrtt st.srtt rtt
        let cwnd' :=
          if st.cwndSegs < st.ssthreshSegs then
            let cwndNext := clampCwnd (st.cwndSegs + Float.ofNat ackedSegs)
            let enterCa := decide (cwndNext >= st.ssthreshSegs)
            let epochStartNs :=
              if enterCa then some r.timeNs else st.epochStartNs
            let wMaxSegs :=
              if enterCa && st.wMaxSegs == 0.0 then cwndNext else st.wMaxSegs
            let kZero :=
              if enterCa && st.wMaxSegs == 0.0 then true else st.kZero
            { st with
              cwndSegs := cwndNext
              srtt := srtt
              epochStartNs := epochStartNs
              wMaxSegs := wMaxSegs
              kZero := kZero }
          else
            cubicUpdate { st with srtt := srtt } r.timeNs rttNs

        pure { cwnd' with lastTimeNs := some r.timeNs }
    | Kind.congestion =>
        let st'' := onCongestion st r.timeNs
        pure { st'' with lastTimeNs := some r.timeNs }
    | Kind.timeout =>
        let st'' := onTimeout st
        pure { st'' with lastTimeNs := some r.timeNs }

  checkSnapshot lineNo st' r

  pure { g with flows := g.flows.insert r.endpointId st' }

def recordCover (cov : CoverageState) (r : Row) : CoverageState :=
  match r.kind with
  | Kind.ack => covHit cov "saw_ack"
  | Kind.congestion => covHit cov "saw_congestion"
  | Kind.timeout => covHit cov "saw_timeout"

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
        let cov := recordCover cov r
        match prevKey with
        | none => pure ()
        | some pk =>
            match require r.srcLine (keyLt pk (key r)) "global key went backwards" with
            | .ok _ => pure ()
            | .error e => throw (e, cov)
        let g' ←
          match step r.srcLine g r with
          | .ok g' => pure g'
          | .error e => throw (e, cov)
        go g' (some (key r)) rs cov
  go {} none rowsSorted {}

def checkRows (rows : List Row) : Except String Unit := do
  match checkRowsWithCoverage rows with
  | .ok _ => pure ()
  | .error (e, _) => throw e

end LeanGuard.CubicEventLog
