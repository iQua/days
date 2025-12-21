import Std

import LeanGuard.Shared.Check
import LeanGuard.Shared.Csv
import LeanGuard.Shared.Key
import LeanGuard.Shared.Numeric
import LeanGuard.Dcqcn.Semantics

namespace LeanGuard.DcqcnEventLog

open LeanGuard.Shared
open LeanGuard.Dcqcn.Semantics

inductive Kind
  | cnpSent
  | cnpRecv
  | timerTick
deriving DecidableEq, Repr

inductive Ecn
  | NotEct
  | Ect0
  | Ect1
  | Ce
deriving DecidableEq, Repr

/-- 1:1 with a single row in `dcqcn_events.csv` emitted by Days under `--features dcqcn,lean`. -/
structure Row where
  timeNs : Nat
  eventId : Nat
  kind : Kind
  endpointId : Nat
  flowId : Nat
  pktId : Option Nat
  pktFlowId : Option Nat
  triggerEcn : Option Ecn
  cnpPriority : Option Nat
  cnpSizeB : Option Nat
  cnpEcn : Option Ecn
  cnpCwr : Option Bool
  cnpLastPacket : Option Bool
  cnpIntervalNs : Nat
  gPpb : Nat
  miPpb : Nat
  initRateBps : Nat
  minRateBps : Nat
  maxRateBps : Nat
  aiRateBps : Nat
  haiRateBps : Nat
  alphaPpb : Option Nat
  rateBps : Option Nat
  cnpSeen : Option Bool
  lastCnpNs : Option Nat
  srcLine : Nat
deriving DecidableEq, Repr

def key (r : Row) : Nat × Nat :=
  (r.timeNs, r.eventId)

def parseKind (s : String) : Except String Kind :=
  match s with
  | "cnp_sent" => pure Kind.cnpSent
  | "cnp_recv" => pure Kind.cnpRecv
  | "timer_tick" => pure Kind.timerTick
  | other => throw s!"invalid kind: {other}"

def parseEcn (s : String) : Except String Ecn :=
  match s with
  | "NotEct" => pure Ecn.NotEct
  | "Ect0" => pure Ecn.Ect0
  | "Ect1" => pure Ecn.Ect1
  | "Ce" => pure Ecn.Ce
  | other => throw s!"invalid ECN field: {other}"


def parseRow (lineNo : Nat) (idx : Std.HashMap String Nat) (fields : Array String) :
    Except String Row := do
  let res : Except String Row := do
    let timeNs ← parseNat (← getField idx fields "time_ns")
    let eventId ← parseNat (← getField idx fields "event_id")
    let kind ← parseKind (← getField idx fields "kind")
    let endpointId ← parseNat (← getField idx fields "endpoint_id")
    let flowId ← parseNat (← getField idx fields "flow_id")
    let pktId ← parseOpt parseNat (← getField idx fields "pkt_id")
    let pktFlowId ← parseOpt parseNat (← getField idx fields "pkt_flow_id")
    let triggerEcn ← parseOpt parseEcn (← getField idx fields "trigger_ecn")
    let cnpPriority ← parseOpt parseNat (← getField idx fields "cnp_priority")
    let cnpSizeB ← parseOpt parseNat (← getField idx fields "cnp_size_b")
    let cnpEcn ← parseOpt parseEcn (← getField idx fields "cnp_ecn")
    let cnpCwr ← parseOpt parseBool (← getField idx fields "cnp_cwr")
    let cnpLastPacket ← parseOpt parseBool (← getField idx fields "cnp_last_packet")
    let cnpIntervalNs ← parseNat (← getField idx fields "cnp_interval_ns")
    let gPpb ← parseNat (← getField idx fields "g_ppb")
    let miPpb ← parseNat (← getField idx fields "mi_ppb")
    let initRateBps ← parseNat (← getField idx fields "init_rate_bps")
    let minRateBps ← parseNat (← getField idx fields "min_rate_bps")
    let maxRateBps ← parseNat (← getField idx fields "max_rate_bps")
    let aiRateBps ← parseNat (← getField idx fields "ai_rate_bps")
    let haiRateBps ← parseNat (← getField idx fields "hai_rate_bps")
    let alphaPpb ← parseOpt parseNat (← getField idx fields "alpha_ppb")
    let rateBps ← parseOpt parseNat (← getField idx fields "rate_bps")
    let cnpSeen ← parseOpt parseBool (← getField idx fields "cnp_seen")
    let lastCnpNs ← parseOpt parseNat (← getField idx fields "last_cnp_ns")
    pure
      { timeNs
        eventId
        kind
        endpointId
        flowId
        pktId
        pktFlowId
        triggerEcn
        cnpPriority
        cnpSizeB
        cnpEcn
        cnpCwr
        cnpLastPacket
        cnpIntervalNs
        gPpb
        miPpb
        initRateBps
        minRateBps
        maxRateBps
        aiRateBps
        haiRateBps
        alphaPpb
        rateBps
        cnpSeen
        lastCnpNs
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

structure SinkParams where
  flowId : Nat
  cnpIntervalNs : Nat
  cnpPriority : Nat
deriving DecidableEq, Repr

structure SinkState where
  p : SinkParams
  lastCnpNs : Option Nat := none
  lastTimeNs : Option Nat := none
deriving Repr

structure PendingInfo where
  sentTimeNs : Nat
  sentEventId : Nat
  sentLine : Nat
deriving Repr

structure Global where
  src : Std.HashMap Nat SrcState := ∅
  sink : Std.HashMap Nat SinkState := ∅
  pending : Std.HashMap (Nat × Nat) PendingInfo := ∅
deriving Repr

def rowSrcParams (r : Row) : SrcParams :=
  { flowId := r.flowId
    cnpIntervalNs := r.cnpIntervalNs
    gPpb := r.gPpb
    miPpb := r.miPpb
    initRateBps := r.initRateBps
    minRateBps := r.minRateBps
    maxRateBps := r.maxRateBps
    aiRateBps := r.aiRateBps
    haiRateBps := r.haiRateBps }

def checkCnpPacket (lineNo : Nat) (r : Row) : Except String (Nat × Nat) := do
  let pktId ← requireSome lineNo "pkt_id" r.pktId
  let pktFlowId ← requireSome lineNo "pkt_flow_id" r.pktFlowId
  require lineNo (pktFlowId = r.flowId) s!"pkt_flow_id {pktFlowId} ≠ flow_id {r.flowId}"

  let sizeB ← requireSome lineNo "cnp_size_b" r.cnpSizeB
  let ecn ← requireSome lineNo "cnp_ecn" r.cnpEcn
  let cwr ← requireSome lineNo "cnp_cwr" r.cnpCwr
  let last ← requireSome lineNo "cnp_last_packet" r.cnpLastPacket
  require lineNo (sizeB = 64) s!"CNP size must be 64, got {sizeB}"
  require lineNo (ecn = Ecn.NotEct) "CNP ECN must be NotEct"
  require lineNo (cwr = false) "CNP cwr must be false"
  require lineNo (last = false) "CNP last_packet must be false"

  pure (pktFlowId, pktId)

def step (lineNo : Nat) (g : Global) (r : Row) : Except String Global := do
  match r.kind with
  | Kind.cnpSent => do
      let (_pktFlowId, pktId) ← checkCnpPacket lineNo r
      let trig ← requireSome lineNo "trigger_ecn" r.triggerEcn
      require lineNo (trig = Ecn.Ce) "trigger_ecn must be Ce"
      let prio ← requireSome lineNo "cnp_priority" r.cnpPriority

      let sinkSt :=
        match g.sink.get? r.endpointId with
        | some s => s
        | none =>
            { p := { flowId := r.flowId, cnpIntervalNs := r.cnpIntervalNs, cnpPriority := prio }
              lastCnpNs := none
              lastTimeNs := none }

      require lineNo (sinkSt.p.flowId = r.flowId) "sink flow_id mismatch"
      require lineNo (sinkSt.p.cnpIntervalNs = r.cnpIntervalNs) "sink cnp_interval_ns mismatch"
      require lineNo (sinkSt.p.cnpPriority = prio) "sink cnp_priority mismatch"

      match sinkSt.lastTimeNs with
      | none => pure ()
      | some prev => require lineNo (prev ≤ r.timeNs) s!"time went backwards: {prev} > {r.timeNs}"

      match sinkSt.lastCnpNs with
      | none => pure ()
      | some last =>
          require lineNo (last + sinkSt.p.cnpIntervalNs ≤ r.timeNs) "CNP interval violated"

      require lineNo (r.lastCnpNs = some r.timeNs) "last_cnp_ns must equal time_ns for cnp_sent"

      require lineNo ((g.pending.get? (r.flowId, pktId)).isNone) "duplicate pending CNP"

      let sinkSt' := { sinkSt with lastCnpNs := some r.timeNs, lastTimeNs := some r.timeNs }
      pure
        { g with
          sink := g.sink.insert r.endpointId sinkSt'
          pending :=
            g.pending.insert (r.flowId, pktId)
              { sentTimeNs := r.timeNs, sentEventId := r.eventId, sentLine := r.srcLine } }

  | Kind.cnpRecv => do
      let (_pktFlowId, pktId) ← checkCnpPacket lineNo r
      let pendingKey := (r.flowId, pktId)
      let pinfo ←
        match g.pending.get? pendingKey with
        | none => throw s!"line {lineNo}: cnp_recv without prior cnp_sent"
        | some p => pure p

      require lineNo (keyLt (pinfo.sentTimeNs, pinfo.sentEventId) (r.timeNs, r.eventId))
        s!"cnp_recv precedes cnp_sent (sent at line {pinfo.sentLine})"

      let p := rowSrcParams r
      require lineNo (okParams p) "invalid source parameters"

      let srcSt :=
        match g.src.get? r.endpointId with
        | some s => s
        | none =>
            initialSrcState p

      require lineNo (srcSt.p = p) "source parameters changed for endpoint"

      match srcSt.lastTimeNs with
      | none => pure ()
      | some prev => require lineNo (prev ≤ r.timeNs) s!"time went backwards: {prev} > {r.timeNs}"

      let srcSt' := srcAfterCnp srcSt r.timeNs

      let α ← requireSome lineNo "alpha_ppb" r.alphaPpb
      let rb ← requireSome lineNo "rate_bps" r.rateBps
      let seen ← requireSome lineNo "cnp_seen" r.cnpSeen
      let expAlpha := toPpb srcSt'.alpha
      let expRate := toBps srcSt'.rateBps
      require lineNo (α = expAlpha) s!"alpha_ppb mismatch: got {α}, expected {expAlpha}"
      require lineNo (rb = expRate) s!"rate_bps mismatch: got {rb}, expected {expRate}"
      require lineNo (seen = srcSt'.cnpSeen) s!"cnp_seen mismatch: got {seen}, expected {srcSt'.cnpSeen}"
      require lineNo (r.lastCnpNs = srcSt'.lastCnpNs) "last_cnp_ns mismatch"

      pure
        { g with
          src := g.src.insert r.endpointId srcSt'
          pending := g.pending.erase pendingKey }

  | Kind.timerTick => do
      let p := rowSrcParams r
      require lineNo (okParams p) "invalid source parameters"

      let srcSt :=
        match g.src.get? r.endpointId with
        | some s => s
        | none =>
            initialSrcState p

      require lineNo (srcSt.p = p) "source parameters changed for endpoint"

      match srcSt.lastTimeNs with
      | none => pure ()
      | some prev => require lineNo (prev ≤ r.timeNs) s!"time went backwards: {prev} > {r.timeNs}"

      let srcSt' := srcAfterTimer srcSt r.timeNs

      let α ← requireSome lineNo "alpha_ppb" r.alphaPpb
      let rb ← requireSome lineNo "rate_bps" r.rateBps
      let seen ← requireSome lineNo "cnp_seen" r.cnpSeen
      let expAlpha := toPpb srcSt'.alpha
      let expRate := toBps srcSt'.rateBps
      require lineNo (α = expAlpha) s!"alpha_ppb mismatch: got {α}, expected {expAlpha}"
      require lineNo (rb = expRate) s!"rate_bps mismatch: got {rb}, expected {expRate}"
      require lineNo (seen = srcSt'.cnpSeen) s!"cnp_seen mismatch: got {seen}, expected {srcSt'.cnpSeen}"
      require lineNo (r.lastCnpNs = srcSt'.lastCnpNs) "last_cnp_ns mismatch"

      pure { g with src := g.src.insert r.endpointId srcSt' }

def checkRows (rows : List Row) : Except String Unit := do
  let rowsSorted ← canonicalizeRows rows key (fun r => r.srcLine)

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

def dummyRow (timeNs eventId srcLine : Nat) : Row :=
  { timeNs
    eventId
    kind := Kind.timerTick
    endpointId := 0
    flowId := 0
    pktId := none
    pktFlowId := none
    triggerEcn := none
    cnpPriority := none
    cnpSizeB := none
    cnpEcn := none
    cnpCwr := none
    cnpLastPacket := none
    cnpIntervalNs := 0
    gPpb := 0
    miPpb := 0
    initRateBps := 0
    minRateBps := 0
    maxRateBps := 0
    aiRateBps := 0
    haiRateBps := 0
    alphaPpb := none
    rateBps := none
    cnpSeen := none
    lastCnpNs := none
    srcLine }

example :
    (match canonicalizeRows [dummyRow 2 1 10, dummyRow 1 5 11, dummyRow 2 0 12] key (fun r => r.srcLine) with
      | .ok v => some v
      | .error _ => none) =
      some [dummyRow 1 5 11, dummyRow 2 0 12, dummyRow 2 1 10] := by
  native_decide

end LeanGuard.DcqcnEventLog
