import Std

import LeanGuard.Shared.Check
import LeanGuard.Shared.Csv
import LeanGuard.Shared.Key

namespace LeanGuard.PfcEventLog

open LeanGuard.Shared

inductive Kind
  | pfcSent
  | pfcRecv
deriving DecidableEq, Repr

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

def oneHot (prio : Nat) : Nat :=
  Nat.shiftLeft 1 prio

structure PendingInfo where
  sentTimeNs : Nat
  sentEventId : Nat
  sentLine : Nat
  senderId : Nat
  receiverId : Nat
  priority : Nat
  classEnable : Nat
  pauseQuanta : Nat
deriving Repr

structure Global where
  pending : Std.HashMap (Nat × Nat) PendingInfo := ∅
  pauseActive : Std.HashMap (Nat × Nat) Bool := ∅
deriving Repr

def isPaused (g : Global) (senderId priority : Nat) : Bool :=
  match g.pauseActive.get? (senderId, priority) with
  | none => false
  | some b => b

def setPaused (g : Global) (senderId priority : Nat) (paused : Bool) : Global :=
  { g with pauseActive := g.pauseActive.insert (senderId, priority) paused }

def step (lineNo : Nat) (g : Global) (r : Row) : Except String Global := do
  require lineNo (r.priority < 8) s!"invalid priority (expected 0..7): {r.priority}"
  require lineNo (r.pauseQuanta ≤ 65535) s!"pause_quanta out of range: {r.pauseQuanta}"
  require lineNo (r.classEnable = oneHot r.priority)
    s!"class_enable must be 1<<priority: got {r.classEnable}, expected {oneHot r.priority}"

  let pendingKey := (r.pfcFrameId, r.priority)

  match r.kind with
  | Kind.pfcSent => do
      let occ ← requireSome lineNo "queue_occupancy_bytes" r.queueOccupancyBytes
      let xoff ← requireSome lineNo "xoff_threshold_bytes" r.xoffThresholdBytes
      let xon ← requireSome lineNo "xon_threshold_bytes" r.xonThresholdBytes
      let cap ← requireSome lineNo "buffer_capacity_bytes" r.bufferCapacityBytes

      require lineNo (xon ≤ xoff) s!"threshold ordering violated: xon={xon} > xoff={xoff}"
      if cap > 0 then
        require lineNo (occ ≤ cap) s!"occupancy exceeds buffer capacity: occ={occ} cap={cap}"

      require lineNo ((g.pending.get? pendingKey).isNone)
        s!"duplicate pending PFC frame: (pfc_frame_id={r.pfcFrameId}, priority={r.priority})"

      let wasPaused := isPaused g r.senderId r.priority
      if r.pauseQuanta = 0 then
        require lineNo wasPaused "resume sent while not paused"
        require lineNo (occ ≤ xon) s!"resume requires occupancy ≤ xon: occ={occ} xon={xon}"
      else
        if wasPaused then
          require lineNo (occ > xon) s!"pause refresh requires occupancy > xon: occ={occ} xon={xon}"
        else
          require lineNo (occ ≥ xoff) s!"pause assert requires occupancy ≥ xoff: occ={occ} xoff={xoff}"

      let g' :=
        if r.pauseQuanta = 0 then
          setPaused g r.senderId r.priority false
        else
          setPaused g r.senderId r.priority true

      pure
        { g' with
          pending :=
            g'.pending.insert pendingKey
              { sentTimeNs := r.timeNs
                sentEventId := r.eventId
                sentLine := r.srcLine
                senderId := r.senderId
                receiverId := r.receiverId
                priority := r.priority
                classEnable := r.classEnable
                pauseQuanta := r.pauseQuanta } }

  | Kind.pfcRecv => do
      let pinfo ←
        match g.pending.get? pendingKey with
        | none => throw s!"line {lineNo}: pfc_recv without prior pfc_sent"
        | some p => pure p

      require lineNo (keyLt (pinfo.sentTimeNs, pinfo.sentEventId) (r.timeNs, r.eventId))
        s!"pfc_recv precedes pfc_sent (sent at line {pinfo.sentLine})"

      require lineNo (r.senderId = pinfo.senderId) "sender_id mismatch"
      require lineNo (r.receiverId = pinfo.receiverId) "receiver_id mismatch"
      require lineNo (r.classEnable = pinfo.classEnable) "class_enable mismatch"
      require lineNo (r.pauseQuanta = pinfo.pauseQuanta) "pause_quanta mismatch"

      pure { g with pending := g.pending.erase pendingKey }

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

end LeanGuard.PfcEventLog
