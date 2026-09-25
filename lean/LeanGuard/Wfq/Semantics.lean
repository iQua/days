import Std

import LeanGuard.Shared.Check

namespace LeanGuard.Wfq.Semantics

open LeanGuard.Shared

/-- Executable WFQ semantics based on Demers-Keshav-Shenker (1989/1990). -/
inductive Kind
    | enqueue
    | schedule
    | depart
deriving DecidableEq, Repr

structure Event where
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
deriving Repr

def packetKey (flowId packetId : Nat) : Nat × Nat :=
    (flowId, packetId)

structure QueuedPacket where
    key : Nat × Nat
    classId : Nat
    sizeBytes : Nat
    finishTime : Rat
    finishTimeNs : Nat
    enqueueTimeNs : Nat
    enqueueEventId : Nat
deriving Repr

structure PendingPacket where
    key : Nat × Nat
    classId : Nat
    sizeBytes : Nat
    finishTimeNs : Nat
    departureTimeNs : Nat
    scheduleLine : Nat
deriving Repr

structure SchedulerState where
    rateBps : Nat := 0
    weights : Std.HashMap Nat Nat := ∅
    finishTimes : Std.HashMap Nat Rat := ∅
    flowCounts : Std.HashMap Nat Nat := ∅
    vtime : Rat := 0
    lastUpdatedNs : Nat := 0
    queue : Std.HashMap (Nat × Nat) QueuedPacket := ∅
    pending : Option PendingPacket := none
    lastTimeNs : Option Nat := none
deriving Repr

structure Global where
    schedulers : Std.HashMap Nat SchedulerState := ∅
deriving Repr

/-- Exact conversion from an integer nanosecond timestamp to rational seconds. -/
def nsToSeconds (ns : Nat) : Rat :=
    (ns : Rat) / (1_000_000_000 : Rat)

/--
Round a nonnegative rational to the nearest natural number, with exact half values rounded upward.
All WFQ values are nonnegative; the defensive negative branch matches the old logging projection.
-/
def roundNonnegative (v : Rat) : Nat :=
    if v < 0 then
        0
    else
        (v.num.natAbs * 2 + v.den) / (v.den * 2)

/-- Exact rational-seconds projection to the integer-nanosecond CSV contract. -/
def toNs (v : Rat) : Nat :=
    roundNonnegative (v * (1_000_000_000 : Rat))

def ensureRate (lineNo : Nat) (st : SchedulerState) (rateBps : Nat) :
    Except String SchedulerState := do
    require lineNo (rateBps > 0) "rate_bps must be > 0"
    if st.rateBps = 0 then
        pure { st with rateBps := rateBps }
    else
        require lineNo (st.rateBps = rateBps)
            s!"rate_bps changed: got {rateBps}, expected {st.rateBps}"
        pure st

def ensureWeight (lineNo : Nat) (st : SchedulerState) (classId weight : Nat) :
    Except String SchedulerState := do
    require lineNo (weight > 0) s!"weight must be > 0 for class {classId}"
    match st.weights.get? classId with
    | none =>
        pure { st with weights := st.weights.insert classId weight }
    | some w => do
        require lineNo (w = weight)
            s!"weight changed for class {classId}: got {weight}, expected {w}"
        pure st

def updateLastTime (lineNo : Nat) (st : SchedulerState) (timeNs : Nat) :
    Except String SchedulerState := do
    match st.lastTimeNs with
    | none => pure { st with lastTimeNs := some timeNs }
    | some prev => do
        require lineNo (prev <= timeNs) s!"time went backwards: {prev} > {timeNs}"
        pure { st with lastTimeNs := some timeNs }

def activeWeightSum (lineNo : Nat) (st : SchedulerState) : Except String Nat := do
    let rec go (xs : List (Nat × Nat)) (acc : Nat) : Except String Nat :=
        match xs with
        | [] => pure acc
        | (classId, count) :: rest =>
            if count = 0 then
                go rest acc
            else
                match st.weights.get? classId with
                | none => throw s!"line {lineNo}: missing weight for class {classId}"
                | some w => go rest (acc + w)
    go st.flowCounts.toList 0

def hasActive (st : SchedulerState) : Bool :=
    let rec go (xs : List (Nat × Nat)) : Bool :=
        match xs with
        | [] => false
        | (_, count) :: rest => if count > 0 then true else go rest
    go st.flowCounts.toList

def minQueued (lineNo : Nat) (st : SchedulerState) : Except String QueuedPacket := do
    match st.queue.toList with
    | [] => throw s!"line {lineNo}: schedule on empty queue"
    | (_, first) :: rest =>
        let rec go (best : QueuedPacket) (xs : List ((Nat × Nat) × QueuedPacket)) : QueuedPacket :=
            match xs with
            | [] => best
            | (_, q) :: xs' =>
                let best' :=
                    if q.finishTime < best.finishTime then
                        q
                    else if q.finishTime = best.finishTime &&
                        decide (q.enqueueTimeNs < best.enqueueTimeNs ∨
                            (q.enqueueTimeNs = best.enqueueTimeNs ∧
                                q.enqueueEventId < best.enqueueEventId)) then
                        q
                    else
                        best
                go best' xs'
        pure (go first rest)

def stepEnqueue (lineNo : Nat) (st : SchedulerState) (e : Event) :
    Except String SchedulerState := do
    require lineNo (e.sizeBytes > 0) "size_bytes must be > 0"
    require lineNo (e.departureTimeNs.isNone) "departure_time_ns must be empty for enqueue"

    let key := packetKey e.flowId e.packetId
    require lineNo ((st.queue.get? key).isNone) "duplicate packet enqueue"
    match st.pending with
    | none => pure ()
    | some p =>
        require lineNo (p.key != key) "packet already pending"

    let weightSum ← activeWeightSum lineNo st
    let arrivalTime := nsToSeconds e.timeNs
    let lastUpdated := nsToSeconds st.lastUpdatedNs

    let (vtimeBase, finishTimesBase) :=
        if weightSum = 0 then
            (0, (∅ : Std.HashMap Nat Rat))
        else
            (st.vtime + (arrivalTime - lastUpdated) / (weightSum : Rat), st.finishTimes)

    let prevFinish := finishTimesBase.get? e.classId |>.getD 0
    let virtualStart := if vtimeBase > prevFinish then vtimeBase else prevFinish
    let serviceTime :=
        ((e.sizeBytes * 8 : Nat) : Rat)
            / ((e.rateBps * e.weight : Nat) : Rat)
    let finishTime := virtualStart + serviceTime
    let expFinishNs := toNs finishTime
    require lineNo (expFinishNs = e.finishTimeNs)
        s!"finish_time_ns mismatch: got {e.finishTimeNs}, expected {expFinishNs}"

    let expVtimeNs := toNs vtimeBase
    require lineNo (expVtimeNs = e.vtimeNs)
        s!"vtime_ns mismatch: got {e.vtimeNs}, expected {expVtimeNs}"

    let count := st.flowCounts.get? e.classId |>.getD 0
    let flowCounts := st.flowCounts.insert e.classId (count + 1)
    let finishTimes := finishTimesBase.insert e.classId finishTime
    let queue :=
        st.queue.insert key
            { key
              classId := e.classId
              sizeBytes := e.sizeBytes
              finishTime := finishTime
              finishTimeNs := e.finishTimeNs
              enqueueTimeNs := e.timeNs
              enqueueEventId := e.eventId }

    pure
        { st with
          flowCounts := flowCounts
          finishTimes := finishTimes
          queue := queue
          vtime := vtimeBase
          lastUpdatedNs := e.timeNs }

def stepSchedule (lineNo : Nat) (st : SchedulerState) (e : Event) :
    Except String SchedulerState := do
    let dep ← requireSome lineNo "departure_time_ns" e.departureTimeNs
    require lineNo (dep >= e.timeNs) "departure_time_ns precedes schedule time"
    match st.pending with
    | some _ => throw s!"line {lineNo}: schedule while another packet pending"
    | none => pure ()

    let expVtimeNs := toNs st.vtime
    require lineNo (expVtimeNs = e.vtimeNs)
        s!"vtime_ns mismatch: got {e.vtimeNs}, expected {expVtimeNs}"

    let key := packetKey e.flowId e.packetId
    let queued ←
        match st.queue.get? key with
        | none => throw s!"line {lineNo}: schedule for unknown packet"
        | some q => pure q

    require lineNo (queued.classId = e.classId) "class_id mismatch"
    require lineNo (queued.sizeBytes = e.sizeBytes) "size_bytes mismatch"
    require lineNo (queued.finishTimeNs = e.finishTimeNs) "finish_time_ns mismatch"

    let minQ ← minQueued lineNo st
    require lineNo (minQ.key = key)
        s!"scheduled packet is not the exact minimum finish tag: got flow {e.flowId}/packet {e.packetId}, expected flow {minQ.key.1}/packet {minQ.key.2}"

    let queue := st.queue.erase key
    let pending :=
        { key := key
          classId := e.classId
          sizeBytes := e.sizeBytes
          finishTimeNs := e.finishTimeNs
          departureTimeNs := dep
          scheduleLine := lineNo }

    pure { st with queue := queue, pending := some pending }

def stepDepart (lineNo : Nat) (st : SchedulerState) (e : Event) :
    Except String SchedulerState := do
    let dep ← requireSome lineNo "departure_time_ns" e.departureTimeNs
    require lineNo (dep = e.timeNs) "departure_time_ns must equal time_ns on depart"
    let pending ←
        match st.pending with
        | none => throw s!"line {lineNo}: depart without pending schedule"
        | some p => pure p

    let key := packetKey e.flowId e.packetId
    require lineNo (pending.key = key)
        s!"depart packet mismatch (scheduled at line {pending.scheduleLine})"
    require lineNo (pending.classId = e.classId) "class_id mismatch"
    require lineNo (pending.sizeBytes = e.sizeBytes) "size_bytes mismatch"
    require lineNo (pending.finishTimeNs = e.finishTimeNs) "finish_time_ns mismatch"
    require lineNo (pending.departureTimeNs = dep)
        s!"departure_time_ns mismatch (scheduled at line {pending.scheduleLine})"

    let weightSum ← activeWeightSum lineNo st
    require lineNo (weightSum > 0) "depart with empty active set"
    let departTime := nsToSeconds dep
    let lastUpdated := nsToSeconds st.lastUpdatedNs
    let vtimeNext := st.vtime + (departTime - lastUpdated) / (weightSum : Rat)

    let count := st.flowCounts.get? e.classId |>.getD 0
    require lineNo (count > 0) "flow_queue_count underflow"
    let flowCounts := st.flowCounts.insert e.classId (count - 1)
    let stTemp := { st with flowCounts := flowCounts }
    let (vtimeFinal, finishTimesFinal) :=
        if hasActive stTemp then
            (vtimeNext, st.finishTimes)
        else
            (0, st.finishTimes.insert e.classId 0)

    let expVtimeNs := toNs vtimeFinal
    require lineNo (expVtimeNs = e.vtimeNs)
        s!"vtime_ns mismatch: got {e.vtimeNs}, expected {expVtimeNs}"

    pure
        { st with
          flowCounts := flowCounts
          finishTimes := finishTimesFinal
          vtime := vtimeFinal
          lastUpdatedNs := dep
          pending := none }

def step (lineNo : Nat) (g : Global) (e : Event) : Except String Global := do
    let st0 := g.schedulers.getD e.schedulerId {}
    let st1 ← updateLastTime lineNo st0 e.timeNs
    let st2 ← ensureRate lineNo st1 e.rateBps
    let st3 ← ensureWeight lineNo st2 e.classId e.weight
    let st' ←
        match e.kind with
        | Kind.enqueue => stepEnqueue lineNo st3 e
        | Kind.schedule => stepSchedule lineNo st3 e
        | Kind.depart => stepDepart lineNo st3 e
    pure { g with schedulers := g.schedulers.insert e.schedulerId st' }

end LeanGuard.Wfq.Semantics
