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
  cwndSegs : Nat
  ssthreshSegs : Nat
  wMaxSegs : Nat
  wLastMaxSegs : Nat
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

def toNatFloor (v : Float) : Nat :=
  ((Float.floor (max0 v)).toUInt64).toNat

def segsToBytes (segs mss : Nat) : Nat :=
  segs * mss

def bytesToSegs (lineNo : Nat) (mss bytes : Nat) (name : String) : Except String Nat := do
  require lineNo (mss > 0) "mss_bytes must be > 0"
  require lineNo (bytes % mss = 0) s!"{name} must be a multiple of mss_bytes"
  pure (bytes / mss)

def checkUnits (lineNo : Nat) (r : Row) : Except String Unit := do
  let _ ← bytesToSegs lineNo r.mssBytes r.initCwndBytes "init_cwnd_bytes"
  let _ ← bytesToSegs lineNo r.mssBytes r.initSsthreshBytes "init_ssthresh_bytes"
  let _ ← bytesToSegs lineNo r.mssBytes r.cwndBytes "cwnd_bytes"
  let _ ← bytesToSegs lineNo r.mssBytes r.ssthreshBytes "ssthresh_bytes"
  let _ ← bytesToSegs lineNo r.mssBytes r.wMaxBytes "w_max_bytes"
  let _ ← bytesToSegs lineNo r.mssBytes r.wLastMaxBytes "w_last_max_bytes"
  pure ()

def ackOnce (st : State) (rttNs nowNs : Nat) : State :=
  let p := st.p
  let beta := ppbToFloat p.betaPpb
  let c := ppbToFloat p.cPpb
  let cwnd := st.cwndSegs
  let ssthresh := st.ssthreshSegs
  if cwnd ≤ ssthresh then
    let cwnd' := cwnd + 1
    let entering := cwnd' > ssthresh
    let wMax' := if entering && st.wMaxSegs = 0 then cwnd' else st.wMaxSegs
    let epochStart' := if entering then some nowNs else st.epochStartNs
    let kZero' := if entering && st.wMaxSegs = 0 then true else st.kZero
    { st with cwndSegs := cwnd', wMaxSegs := wMax', epochStartNs := epochStart', kZero := kZero' }
  else
    let epochStartNs := st.epochStartNs.getD nowNs
    let wMaxSegs := if st.wMaxSegs = 0 then cwnd else st.wMaxSegs
    let kZero := if st.wMaxSegs = 0 then true else st.kZero
    let t := nsToSeconds (nowNs - epochStartNs)
    let rtt := nsToSeconds rttNs
    let wMaxF := Float.ofNat wMaxSegs
    let k := if kZero then 0.0 else Float.cbrt (wMaxF * (1.0 - beta) / c)
    let wCubic (x : Float) : Float :=
      c * (Float.pow (x - k) 3.0) + wMaxF
    let wCubicT := wCubic t
    let wEst :=
      wMaxF * beta + (3.0 * (1.0 - beta) / (1.0 + beta)) * (t / rtt)
    let cwndF := Float.ofNat cwnd
    let wTarget := wCubic (t + rtt)
    let cwndNextF :=
      if p.tcpFriendly && wCubicT < wEst then
        wEst
      else
        cwndF + (wTarget - cwndF) / cwndF
    let cwndNext := Nat.max 1 (toNatFloor cwndNextF)
    { st with cwndSegs := cwndNext, wMaxSegs := wMaxSegs, epochStartNs := some epochStartNs, kZero := kZero }

def ackMany (n : Nat) (st : State) (rttNs nowNs : Nat) : State :=
  let rec go (n : Nat) (s : State) : State :=
    match n with
    | 0 => s
    | n + 1 => go n (ackOnce s rttNs nowNs)
  go n st

def onCongestion (st : State) (nowNs : Nat) : State :=
  let p := st.p
  let beta := ppbToFloat p.betaPpb
  let cwnd := st.cwndSegs
  let wMaxCur := cwnd
  let wLast := st.wLastMaxSegs
  let (wMax', wLast') :=
    if p.fastConvergence && wLast > 0 && wMaxCur < wLast then
      let wMaxAdj :=
        toNatFloor ((Float.ofNat wMaxCur) * (1.0 + beta) / 2.0)
      (Nat.max 1 wMaxAdj, wMaxCur)
    else
      (wMaxCur, wMaxCur)
  let ssthresh := Nat.max 2 (toNatFloor ((Float.ofNat cwnd) * beta))
  let cwnd' := Nat.max 1 (toNatFloor ((Float.ofNat cwnd) * beta))
  { st with
    cwndSegs := cwnd'
    ssthreshSegs := ssthresh
    wMaxSegs := wMax'
    wLastMaxSegs := wLast'
    epochStartNs := some nowNs
    kZero := false }

def onTimeout (st : State) : State :=
  let p := st.p
  let beta := ppbToFloat p.betaPpb
  let cwnd := st.cwndSegs
  let ssthresh := Nat.max 2 (toNatFloor ((Float.ofNat cwnd) * beta))
  { st with
    cwndSegs := 1
    ssthreshSegs := ssthresh
    wMaxSegs := 0
    epochStartNs := none
    kZero := true }

def checkSnapshot (lineNo : Nat) (st : State) (r : Row) : Except String Unit := do
  let mss := st.p.mssBytes
  let expCwndBytes := segsToBytes st.cwndSegs mss
  let expSsthreshBytes := segsToBytes st.ssthreshSegs mss
  let expWMaxBytes := segsToBytes st.wMaxSegs mss
  let expWLastMaxBytes := segsToBytes st.wLastMaxSegs mss
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

  let initCwndSegs ← bytesToSegs lineNo p.mssBytes p.initCwndBytes "init_cwnd_bytes"
  let initSsthreshSegs ← bytesToSegs lineNo p.mssBytes p.initSsthreshBytes "init_ssthresh_bytes"

  let st :=
    match g.flows.get? r.endpointId with
    | some s => s
    | none =>
        { p := p
          cwndSegs := initCwndSegs
          ssthreshSegs := initSsthreshSegs
          wMaxSegs := 0
          wLastMaxSegs := 0
          epochStartNs := none
          kZero := true
          lastTimeNs := none }

  require lineNo (st.p = p) "CUBIC parameters changed for endpoint"

  match st.lastTimeNs with
  | none => pure ()
  | some prev => require lineNo (prev ≤ r.timeNs) s!"time went backwards: {prev} > {r.timeNs}"

  let st' ←
    match r.kind with
    | Kind.ack => do
        let ackedSegs ← requireSome lineNo "acked_segs" r.ackedSegs
        let rttNs ← requireSome lineNo "rtt_ns" r.rttNs
        require lineNo (ackedSegs > 0) "acked_segs must be > 0"
        require lineNo (rttNs > 0) "rtt_ns must be > 0"
        match st.epochStartNs with
        | some t0 => require lineNo (t0 ≤ r.timeNs) "epoch_start_ns is in the future"
        | none => pure ()
        let st'' := ackMany ackedSegs st rttNs r.timeNs
        pure { st'' with lastTimeNs := some r.timeNs }
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
