import Std

import LeanGuard.Shared.Check
import LeanGuard.Shared.Key

namespace LeanGuard.Pfc.Semantics

open LeanGuard.Shared

inductive Kind
  | pfcSent
  | pfcRecv
deriving DecidableEq, Repr

structure Event where
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
  srcLine : Nat
deriving Repr

def key (e : Event) : Nat × Nat :=
  (e.timeNs, e.eventId)

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

def step (lineNo : Nat) (g : Global) (e : Event) : Except String Global := do
  require lineNo (e.priority < 8) s!"invalid priority (expected 0..7): {e.priority}"
  require lineNo (e.pauseQuanta ≤ 65535) s!"pause_quanta out of range: {e.pauseQuanta}"
  require lineNo (e.classEnable = oneHot e.priority)
    s!"class_enable must be 1<<priority: got {e.classEnable}, expected {oneHot e.priority}"

  let pendingKey := (e.pfcFrameId, e.priority)

  match e.kind with
  | Kind.pfcSent => do
      let occ ← requireSome lineNo "queue_occupancy_bytes" e.queueOccupancyBytes
      let xoff ← requireSome lineNo "xoff_threshold_bytes" e.xoffThresholdBytes
      let xon ← requireSome lineNo "xon_threshold_bytes" e.xonThresholdBytes
      let cap ← requireSome lineNo "buffer_capacity_bytes" e.bufferCapacityBytes

      require lineNo (xon ≤ xoff) s!"threshold ordering violated: xon={xon} > xoff={xoff}"
      if cap > 0 then
        require lineNo (occ ≤ cap) s!"occupancy exceeds buffer capacity: occ={occ} cap={cap}"

      require lineNo ((g.pending.get? pendingKey).isNone)
        s!"duplicate pending PFC frame: (pfc_frame_id={e.pfcFrameId}, priority={e.priority})"

      let wasPaused := isPaused g e.senderId e.priority
      if e.pauseQuanta = 0 then
        require lineNo wasPaused "resume sent while not paused"
        require lineNo (occ ≤ xon) s!"resume requires occupancy ≤ xon: occ={occ} xon={xon}"
      else
        if wasPaused then
          require lineNo (occ > xon) s!"pause refresh requires occupancy > xon: occ={occ} xon={xon}"
        else
          require lineNo (occ ≥ xoff) s!"pause assert requires occupancy ≥ xoff: occ={occ} xoff={xoff}"

      let g' :=
        if e.pauseQuanta = 0 then
          setPaused g e.senderId e.priority false
        else
          setPaused g e.senderId e.priority true

      pure
        { g' with
          pending :=
            g'.pending.insert pendingKey
              { sentTimeNs := e.timeNs
                sentEventId := e.eventId
                sentLine := e.srcLine
                senderId := e.senderId
                receiverId := e.receiverId
                priority := e.priority
                classEnable := e.classEnable
                pauseQuanta := e.pauseQuanta } }

  | Kind.pfcRecv => do
      let pinfo ←
        match g.pending.get? pendingKey with
        | none => throw s!"line {lineNo}: pfc_recv without prior pfc_sent"
        | some p => pure p

      require lineNo (keyLt (pinfo.sentTimeNs, pinfo.sentEventId) (e.timeNs, e.eventId))
        s!"pfc_recv precedes pfc_sent (sent at line {pinfo.sentLine})"

      require lineNo (e.senderId = pinfo.senderId) "sender_id mismatch"
      require lineNo (e.receiverId = pinfo.receiverId) "receiver_id mismatch"
      require lineNo (e.classEnable = pinfo.classEnable) "class_enable mismatch"
      require lineNo (e.pauseQuanta = pinfo.pauseQuanta) "pause_quanta mismatch"

      pure { g with pending := g.pending.erase pendingKey }

end LeanGuard.Pfc.Semantics

