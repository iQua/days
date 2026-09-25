import Std

import DaysExecutor.Event
import LeanGuard.Shared.Check

namespace LeanGuard.Sp.Semantics

open LeanGuard.Shared

/-- Executor SP certificate transitions. -/
inductive Kind
    | enqueue
    | schedule
    | depart
deriving DecidableEq, Repr

/-- One fully observed executor SP transition. -/
structure Event where
    key : DaysExecutor.EventKey
    kind : Kind
    schedulerId : Nat
    classCount : Nat
    packetId : Nat
    flowId : Nat
    classId : Nat
    priority : Nat
    sizeBytes : Nat
    departureTimeNs : Option Nat
deriving Repr

def packetKey (flowId packetId : Nat) : Nat × Nat :=
    (flowId, packetId)

/-- A queued packet retains its canonical enqueue key for stable FIFO tie breaking. -/
structure QueuedPacket where
    key : Nat × Nat
    classId : Nat
    priority : Nat
    sizeBytes : Nat
    enqueueKey : DaysExecutor.EventKey
deriving Repr

structure PendingPacket where
    key : Nat × Nat
    classId : Nat
    priority : Nat
    sizeBytes : Nat
    departureTimeNs : Nat
    scheduleLine : Nat
deriving Repr

structure SchedulerState where
    classCount : Nat := 0
    priorities : Std.HashMap Nat Nat := ∅
    queue : List QueuedPacket := []
    pending : Option PendingPacket := none
deriving Repr

structure Global where
    schedulers : Std.HashMap Nat SchedulerState := ∅
deriving Repr

def expectedPhase : Kind → Nat
    | .enqueue => 0
    | .depart => 1
    | .schedule => 2

def ensureClassConfig (lineNo : Nat) (st : SchedulerState) (e : Event) :
    Except String SchedulerState := do
    require lineNo (e.classCount > 0) "class_count must be > 0"
    require lineNo (e.classId < e.classCount)
        s!"class_id {e.classId} is outside class_count {e.classCount}"
    require lineNo (e.classId = e.flowId % e.classCount)
        s!"class_id mismatch: got {e.classId}, expected flow_id % class_count = {e.flowId % e.classCount}"
    let st ←
        if st.classCount = 0 then
            pure { st with classCount := e.classCount }
        else do
            require lineNo (st.classCount = e.classCount)
                s!"class_count changed: got {e.classCount}, expected {st.classCount}"
            pure st
    match st.priorities.get? e.classId with
    | none => pure { st with priorities := st.priorities.insert e.classId e.priority }
    | some priority => do
        require lineNo (priority = e.priority)
            s!"priority changed for class {e.classId}: got {e.priority}, expected {priority}"
        pure st

def queueContains (key : Nat × Nat) : List QueuedPacket → Bool
    | [] => false
    | packet :: rest => packet.key = key || queueContains key rest

def eraseQueued (key : Nat × Nat) : List QueuedPacket → List QueuedPacket
    | [] => []
    | packet :: rest =>
        if packet.key = key then rest else packet :: eraseQueued key rest

/--
Select the greatest integer priority. Equal priorities retain the first enqueue in canonical order.
-/
def bestQueued (lineNo : Nat) : List QueuedPacket → Except String QueuedPacket
    | [] => throw s!"line {lineNo}: schedule on empty queue"
    | first :: rest =>
        let rec go (best : QueuedPacket) : List QueuedPacket → QueuedPacket
            | [] => best
            | packet :: tail =>
                let best' := if best.priority < packet.priority then packet else best
                go best' tail
        pure (go first rest)

def stepEnqueue (lineNo : Nat) (st : SchedulerState) (e : Event) :
    Except String SchedulerState := do
    require lineNo (e.sizeBytes > 0) "size_bytes must be > 0"
    require lineNo (e.departureTimeNs.isNone) "departure_time_ns must be empty for enqueue"
    let key := packetKey e.flowId e.packetId
    require lineNo (!queueContains key st.queue) "duplicate packet enqueue"
    match st.pending with
    | none => pure ()
    | some pending => require lineNo (pending.key != key) "packet already pending"
    let packet : QueuedPacket :=
        { key
          classId := e.classId
          priority := e.priority
          sizeBytes := e.sizeBytes
          enqueueKey := e.key }
    pure { st with queue := st.queue ++ [packet] }

def stepSchedule (lineNo : Nat) (st : SchedulerState) (e : Event) :
    Except String SchedulerState := do
    let departure ← requireSome lineNo "departure_time_ns" e.departureTimeNs
    require lineNo (e.key.timeNs ≤ departure) "departure_time_ns precedes schedule time"
    match st.pending with
    | some _ => throw s!"line {lineNo}: schedule while another packet is pending"
    | none => pure ()
    let expected ← bestQueued lineNo st.queue
    let key := packetKey e.flowId e.packetId
    require lineNo (expected.key = key)
        s!"scheduled packet violates SP order: got flow {e.flowId}/packet {e.packetId}, expected flow {expected.key.1}/packet {expected.key.2}"
    require lineNo (expected.classId = e.classId) "class_id mismatch"
    require lineNo (expected.priority = e.priority) "priority mismatch"
    require lineNo (expected.sizeBytes = e.sizeBytes) "size_bytes mismatch"
    let pending : PendingPacket :=
        { key
          classId := e.classId
          priority := e.priority
          sizeBytes := e.sizeBytes
          departureTimeNs := departure
          scheduleLine := lineNo }
    pure { st with queue := eraseQueued key st.queue, pending := some pending }

def stepDepart (lineNo : Nat) (st : SchedulerState) (e : Event) :
    Except String SchedulerState := do
    let departure ← requireSome lineNo "departure_time_ns" e.departureTimeNs
    require lineNo (departure = e.key.timeNs)
        "departure_time_ns must equal time_ns on depart"
    let pending ←
        match st.pending with
        | none => throw s!"line {lineNo}: depart without pending schedule"
        | some packet => pure packet
    let key := packetKey e.flowId e.packetId
    require lineNo (pending.key = key)
        s!"depart packet mismatch (scheduled at line {pending.scheduleLine})"
    require lineNo (pending.classId = e.classId) "class_id mismatch"
    require lineNo (pending.priority = e.priority) "priority mismatch"
    require lineNo (pending.sizeBytes = e.sizeBytes) "size_bytes mismatch"
    require lineNo (pending.departureTimeNs = departure)
        s!"departure_time_ns mismatch (scheduled at line {pending.scheduleLine})"
    pure { st with pending := none }

def step (lineNo : Nat) (g : Global) (e : Event) : Except String Global := do
    require lineNo (e.key.phase = expectedPhase e.kind)
        s!"event_phase mismatch: got {e.key.phase}, expected {expectedPhase e.kind}"
    let st0 := g.schedulers.getD e.schedulerId {}
    let st1 ← ensureClassConfig lineNo st0 e
    let st' ←
        match e.kind with
        | .enqueue => stepEnqueue lineNo st1 e
        | .schedule => stepSchedule lineNo st1 e
        | .depart => stepDepart lineNo st1 e
    pure { g with schedulers := g.schedulers.insert e.schedulerId st' }

end LeanGuard.Sp.Semantics
