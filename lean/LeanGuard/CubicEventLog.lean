import Std

namespace LeanGuard.CubicEventLog

def PPB : Nat := 1_000_000_000

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

def keyLt (a b : Nat × Nat) : Bool :=
  decide (a.1 < b.1 ∨ (a.1 = b.1 ∧ a.2 < b.2))

def stripCR (s : String) : String :=
  if s.endsWith "\r" then
    s.dropRight 1
  else
    s

/-- Simple CSV splitter that preserves empty fields. Assumes no quoted commas. -/
def splitCsvLine (s : String) : List String :=
  s.splitOn ","

def mkIndex (cols : List String) : Std.HashMap String Nat :=
  let rec go (i : Nat) (cols : List String) (m : Std.HashMap String Nat) : Std.HashMap String Nat :=
    match cols with
    | [] => m
    | c :: cs => go (i + 1) cs (m.insert c i)
  go 0 cols ∅

def getField (idx : Std.HashMap String Nat) (fields : Array String) (name : String) :
    Except String String := do
  match idx.get? name with
  | none => throw s!"missing required column: {name}"
  | some i =>
      match fields[i]? with
      | none => throw s!"row has no column index {i} for {name}"
      | some v => pure v.trim

def parseKind (s : String) : Except String Kind :=
  match s with
  | "ack" => pure Kind.ack
  | "congestion" => pure Kind.congestion
  | "timeout" => pure Kind.timeout
  | other => throw s!"invalid kind: {other}"

def parseNat (s : String) : Except String Nat :=
  match s.toNat? with
  | some n => pure n
  | none => throw s!"invalid Nat: '{s}'"

def parseBool (s : String) : Except String Bool :=
  match s with
  | "true" => pure true
  | "false" => pure false
  | other => throw s!"invalid Bool: '{other}'"

def parseOpt {α : Type} (p : String → Except String α) (s : String) : Except String (Option α) :=
  if s.isEmpty then
    pure none
  else
    some <$> p s

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

structure Params where
  flowId : Nat
  mssBytes : Nat
  betaPpb : Nat
  cPpb : Nat
  tcpFriendly : Bool
  fastConvergence : Bool
  initCwndBytes : Nat
  initSsthreshBytes : Nat
deriving DecidableEq, Repr

structure State where
  p : Params
  cwndSegs : Float
  ssthreshSegs : Float
  wMaxSegs : Float
  wLastMaxSegs : Float
  srtt : Float := 0.0
  epochStartNs : Option Nat := none
  kZero : Bool := true
  lastTimeNs : Option Nat := none
deriving Repr

structure Global where
  flows : Std.HashMap Nat State := ∅
deriving Repr

def require (lineNo : Nat) (cond : Bool) (msg : String) : Except String Unit :=
  if cond then
    pure ()
  else
    throw s!"line {lineNo}: {msg}"

def requireSome {α : Type} (lineNo : Nat) (name : String) : Option α → Except String α
  | none => throw s!"line {lineNo}: missing required field: {name}"
  | some v => pure v

def okParams (p : Params) : Bool :=
  p.mssBytes > 0
    && p.betaPpb > 0
    && p.betaPpb < PPB
    && p.cPpb > 0
    && p.initCwndBytes > 0
    && p.initSsthreshBytes > 0

def rowParams (r : Row) : Params :=
  { flowId := r.flowId
    mssBytes := r.mssBytes
    betaPpb := r.betaPpb
    cPpb := r.cPpb
    tcpFriendly := r.tcpFriendly
    fastConvergence := r.fastConvergence
    initCwndBytes := r.initCwndBytes
    initSsthreshBytes := r.initSsthreshBytes }

def max0 (a : Float) : Float :=
  if a < 0.0 then 0.0 else a

def ppbToFloat (ppb : Nat) : Float :=
  (Float.ofNat ppb) / (Float.ofNat PPB)

def nsToSeconds (ns : Nat) : Float :=
  (Float.ofNat ns) / 1.0e9

def updateSrtt (srtt rtt : Float) : Float :=
  if srtt == 0.0 then
    rtt
  else
    (1.0 - 0.125) * srtt + 0.125 * rtt

def toNatFloor (v : Float) : Nat :=
  ((Float.floor (max0 v)).toUInt64).toNat

def bytesToSegs (bytes mss : Nat) : Float :=
  (Float.ofNat bytes) / (Float.ofNat mss)

def encodeBytes (segs : Float) (mss : Nat) : Nat :=
  toNatFloor (segs * Float.ofNat mss)

def checkUnits (lineNo : Nat) (r : Row) : Except String Unit := do
  require lineNo (r.mssBytes > 0) "mss_bytes must be > 0"

def maxCwndSegs : Float := 2.0e6

def cubicK (st : State) (beta c : Float) : Float :=
  if st.kZero || decide (st.wMaxSegs <= 0.0) then
    0.0
  else
    Float.cbrt (st.wMaxSegs * (1.0 - beta) / c)

def cubicWindow (st : State) (beta c : Float) (t : Float) : Float :=
  c * (Float.pow (t - cubicK st beta c) 3.0) + st.wMaxSegs

def clampCwnd (cwnd : Float) : Float :=
  let c := if cwnd < 1.0 then 1.0 else cwnd
  if c > maxCwndSegs then maxCwndSegs else c

def cubicUpdate (st : State) (nowNs rttNs : Nat) : State :=
  let p := st.p
  let beta := ppbToFloat p.betaPpb
  let c := ppbToFloat p.cPpb
  let rtt := nsToSeconds rttNs
  let srtt := if st.srtt > 0.0 then st.srtt else rtt
  let epochStartNs := st.epochStartNs.getD nowNs
  let wMaxSegs := if st.wMaxSegs == 0.0 then st.cwndSegs else st.wMaxSegs
  let kZero := if st.wMaxSegs == 0.0 then true else st.kZero
  let t := nsToSeconds (nowNs - epochStartNs)
  let st' := { st with wMaxSegs := wMaxSegs, kZero := kZero }
  let wCubicT := cubicWindow st' beta c t
  let wEst :=
    wMaxSegs * beta + (3.0 * (1.0 - beta) / (1.0 + beta)) * (t / srtt)
  let cwndNext :=
    if p.tcpFriendly && wCubicT < wEst then
      wEst
    else
      let wTarget := cubicWindow st' beta c (t + srtt)
      let denom := if st.cwndSegs < 1.0 then 1.0 else st.cwndSegs
      st.cwndSegs + (wTarget - st.cwndSegs) / denom
  { st' with
    cwndSegs := clampCwnd cwndNext
    srtt := srtt
    epochStartNs := some epochStartNs }

def onCongestion (st : State) (nowNs : Nat) : State :=
  let p := st.p
  let beta := ppbToFloat p.betaPpb
  let wMaxCur := st.cwndSegs
  let (wMax', wLast') :=
    if p.fastConvergence && st.wLastMaxSegs > 0.0 && wMaxCur < st.wLastMaxSegs then
      (wMaxCur * (1.0 + beta) / 2.0, wMaxCur)
    else
      (wMaxCur, wMaxCur)
  let reduced := wMaxCur * beta
  let ssthresh := if reduced < 2.0 then 2.0 else reduced
  let cwnd' := clampCwnd reduced
  { st with
    cwndSegs := cwnd'
    ssthreshSegs := ssthresh
    wMaxSegs := wMax'
    wLastMaxSegs := wLast'
    srtt := st.srtt
    epochStartNs := some nowNs
    kZero := false }

def onTimeout (st : State) : State :=
  let p := st.p
  let beta := ppbToFloat p.betaPpb
  let reduced := st.cwndSegs * beta
  let ssthresh := if reduced < 2.0 then 2.0 else reduced
  { st with
    cwndSegs := 1.0
    ssthreshSegs := ssthresh
    wMaxSegs := 0.0
    wLastMaxSegs := 0.0
    epochStartNs := none
    kZero := true }

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

def canonicalizeRows (rows : List Row) : Except String (List Row) := do
  let rowsSorted :=
    rows.toArray
      |>.qsort (fun a b => keyLt (key a) (key b))
      |>.toList

  let rec checkKeys : List Row → Except String Unit
    | [] => pure ()
    | [_] => pure ()
    | a :: b :: rest => do
        if decide (key a = key b) then
          throw
            s!"duplicate key at lines {a.srcLine} and {b.srcLine}: (time_ns={a.timeNs}, event_id={a.eventId})"
        require b.srcLine (keyLt (key a) (key b)) "canonical key order violated"
        checkKeys (b :: rest)

  checkKeys rowsSorted
  pure rowsSorted

def checkRows (rows : List Row) : Except String Unit := do
  let rowsSorted ← canonicalizeRows rows

  let rec go (g : Global) (prevKey : Option (Nat × Nat)) (rows : List Row) : Except String Unit := do
    match rows with
    | [] => pure ()
    | r :: rs => do
        match prevKey with
        | none => pure ()
        | some pk =>
            require r.srcLine (keyLt pk (key r)) "global key went backwards"
        let g' ← step r.srcLine g r
        go g' (some (key r)) rs
  go {} none rowsSorted

end LeanGuard.CubicEventLog
