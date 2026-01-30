------------------------------ MODULE DrrTrace ------------------------------
\* Trace validation baseline for Days DRR (Deficit Round Robin) scheduling.
\*
\* Intended to mirror:
\* - `lean/LeanGuard/Drr/Semantics.lean`
\* - `lean/LeanGuard/DrrEventLog.lean`
\*
\* This module expects a companion module named `TraceData` defining `Trace`.
\*
\* IMPORTANT: `TraceData` is rescaled to fit TLC's 32-bit integers:
\* - `*_ns` fields are stored in microseconds
\* - `*_ppb` fields are stored in permille (unused here)
\* - `*_bps` fields are stored in units of 10 Mbps (only checked for constancy)

EXTENDS Naturals, Integers, Sequences, TLC, FiniteSets, TraceData

LenTrace == Len(Trace)

Key(rec) == <<rec.time_ns, rec.event_id>>
KeyLt(k1, k2) ==
  (k1[1] < k2[1]) \/ (k1[1] = k2[1] /\ k1[2] < k2[2])

TraceOk ==
  IF LenTrace <= 1 THEN
    TRUE
  ELSE
    \A i \in 1..(LenTrace - 1) : KeyLt(Key(Trace[i]), Key(Trace[i + 1]))

Has(ll, field) == field \in DOMAIN ll

SchedulerIds == { Trace[i].scheduler_id : i \in 1..LenTrace }

MaxOfSet(S) == CHOOSE m \in S : \A x \in S : x <= m

MaxClassCount ==
  IF LenTrace = 0 THEN 0 ELSE MaxOfSet({ Trace[i].class_count : i \in 1..LenTrace })

AllClassIds ==
  IF MaxClassCount = 0 THEN {0} ELSE 0..(MaxClassCount - 1)

DefaultPacket == [ packet_id |-> 0, flow_id |-> 0, size_bytes |-> 0 ]

DefaultSched ==
  [ class_count |-> 0,
    rate_bps |-> 0,
    quantum |-> [cid \in AllClassIds |-> 0],
    deficit |-> [cid \in AllClassIds |-> 0],
    queues |-> [cid \in AllClassIds |-> <<>>],
    current_queue |-> 0,
    packets_waiting |-> 0,
    batch_has |-> FALSE,
    batch_id |-> 0,
    batch_class_has |-> FALSE,
    batch_class |-> 0,
    batch_next_has |-> FALSE,
    batch_next_start_ns |-> 0,
    last_time_has |-> FALSE,
    last_time_ns |-> 0 ]

QueueEmpty(st, cid) == Len(st.queues[cid]) = 0

WrapOk(st) ==
  \A cid \in 0..(st.class_count - 1) :
    QueueEmpty(st, cid) \/ st.quantum[cid] > 0

UpdateDeficits(st) ==
  [cid \in AllClassIds |->
    IF cid < st.class_count THEN
      IF QueueEmpty(st, cid) THEN
        0
      ELSE
        st.deficit[cid] + st.quantum[cid]
    ELSE
      st.deficit[cid]]

NextQueueState(st) ==
  IF st.current_queue + 1 < st.class_count THEN
    [st EXCEPT !.current_queue = st.current_queue + 1]
  ELSE
    [st EXCEPT !.current_queue = 0, !.deficit = UpdateDeficits(st)]

RECURSIVE AdvanceQueueOk(_, _), AdvanceQueueState(_, _)

AdvanceQueueOk(st, steps) ==
  IF steps = 0 THEN
    TRUE
  ELSE
    IF st.current_queue + 1 < st.class_count THEN
      AdvanceQueueOk([st EXCEPT !.current_queue = st.current_queue + 1], steps - 1)
    ELSE
      /\ WrapOk(st)
      /\ AdvanceQueueOk([st EXCEPT !.current_queue = 0, !.deficit = UpdateDeficits(st)], steps - 1)

AdvanceQueueState(st, steps) ==
  IF steps = 0 THEN
    st
  ELSE
    IF st.current_queue + 1 < st.class_count THEN
      AdvanceQueueState([st EXCEPT !.current_queue = st.current_queue + 1], steps - 1)
    ELSE
      AdvanceQueueState([st EXCEPT !.current_queue = 0, !.deficit = UpdateDeficits(st)], steps - 1)

LastTimeOk(st, t) == ~st.last_time_has \/ st.last_time_ns <= t

UpdateLastTimeState(st, t) ==
  [st EXCEPT !.last_time_has = TRUE, !.last_time_ns = t]

EnsureClassCountOk(st, n) == n > 0 /\ (st.class_count = 0 \/ st.class_count = n)

EnsureClassCountState(st, n) ==
  IF st.class_count = 0 THEN
    [st EXCEPT !.class_count = n]
  ELSE
    st

EnsureRateOk(st, r) == r > 0 /\ (st.rate_bps = 0 \/ st.rate_bps = r)

EnsureRateState(st, r) ==
  IF st.rate_bps = 0 THEN
    [st EXCEPT !.rate_bps = r]
  ELSE
    st

EnsureQuantumOk(st, cid, q) == q > 0 /\ (st.quantum[cid] = 0 \/ st.quantum[cid] = q)

EnsureQuantumState(st, cid, q) ==
  IF st.quantum[cid] = 0 THEN
    [st EXCEPT !.quantum[cid] = q]
  ELSE
    st

ApplyBatchOk(st, batchId, timeNs) ==
  /\ ~st.batch_has \/ batchId >= st.batch_id
  /\ LET st0 ==
       IF ~st.batch_has \/ batchId # st.batch_id THEN
         [st EXCEPT
           !.batch_has = TRUE,
           !.batch_id = batchId,
           !.batch_class_has = FALSE,
           !.batch_next_has = FALSE]
       ELSE
         [st EXCEPT !.batch_has = TRUE, !.batch_id = batchId]
     IN
       IF ~st0.batch_next_has THEN
         TRUE
       ELSE
         st0.batch_next_start_ns = timeNs

ApplyBatchState(st, batchId, timeNs) ==
  LET st0 ==
       IF ~st.batch_has \/ batchId # st.batch_id THEN
         [st EXCEPT
           !.batch_has = TRUE,
           !.batch_id = batchId,
           !.batch_class_has = FALSE,
           !.batch_next_has = FALSE]
       ELSE
         [st EXCEPT !.batch_has = TRUE, !.batch_id = batchId]
  IN
    IF ~st0.batch_next_has THEN
      [st0 EXCEPT !.batch_next_has = TRUE, !.batch_next_start_ns = timeNs]
    ELSE
      st0

VARIABLES l, sched

Vars == <<l, sched>>

Init ==
  /\ l = 1
  /\ TraceOk
  /\ sched = [sid \in SchedulerIds |-> DefaultSched]

Enqueue ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         sid == ll.scheduler_id
         st0 == sched[sid]
         stT == UpdateLastTimeState(st0, ll.time_ns)
         st1 == EnsureClassCountState(stT, ll.class_count)
         st2 == EnsureRateState(st1, ll.rate_bps)
         st3 == EnsureQuantumState(st2, ll.class_id, ll.quantum_bytes)
         pkt == [packet_id |-> ll.packet_id, flow_id |-> ll.flow_id, size_bytes |-> ll.size_bytes]
         q0 == st3.queues[ll.class_id]
         q1 == Append(q0, pkt)
         st4 == [st3 EXCEPT
           !.queues[ll.class_id] = q1,
           !.packets_waiting = st3.packets_waiting + 1]
     IN
       /\ ll.kind = "enqueue"
       /\ LastTimeOk(st0, ll.time_ns)
       /\ EnsureClassCountOk(stT, ll.class_count)
       /\ EnsureRateOk(st1, ll.rate_bps)
       /\ EnsureQuantumOk(st2, ll.class_id, ll.quantum_bytes)
       /\ ~Has(ll, "batch_id")
       /\ ~Has(ll, "departure_time_ns")
       /\ ll.scan_steps = 0
       /\ ll.size_bytes > 0
       /\ ll.class_id < st3.class_count
       /\ ll.current_queue < st3.class_count
       /\ ll.current_queue = st3.current_queue
       /\ ll.deficit_bytes = st4.deficit[ll.class_id]
       /\ l' = l + 1
       /\ sched' = [sched EXCEPT ![sid] = st4]

Schedule ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         sid == ll.scheduler_id
         st0 == sched[sid]
         stT == UpdateLastTimeState(st0, ll.time_ns)
         st1 == EnsureClassCountState(stT, ll.class_count)
         st2 == EnsureRateState(st1, ll.rate_bps)
         st3 == EnsureQuantumState(st2, ll.class_id, ll.quantum_bytes)
         batchId == ll.batch_id
         dep == ll.departure_time_ns
         stA == ApplyBatchState(st3, batchId, ll.time_ns)
         stB == AdvanceQueueState(stA, ll.scan_steps)
         q == stB.queues[ll.class_id]
         headPkt == Head(q)
         deficit0 == stB.deficit[ll.class_id]
         qTail == Tail(q)
         stC0 ==
           IF stB.batch_class_has THEN
             stB
           ELSE
             [stB EXCEPT !.batch_class_has = TRUE, !.batch_class = ll.class_id]
         stC ==
           [stC0 EXCEPT
             !.queues[ll.class_id] = qTail,
             !.packets_waiting = stC0.packets_waiting - 1,
             !.deficit[ll.class_id] = deficit0 - ll.size_bytes,
             !.batch_next_has = TRUE,
             !.batch_next_start_ns = dep]
     IN
       /\ ll.kind = "schedule"
       /\ LastTimeOk(st0, ll.time_ns)
       /\ EnsureClassCountOk(stT, ll.class_count)
       /\ EnsureRateOk(st1, ll.rate_bps)
       /\ EnsureQuantumOk(st2, ll.class_id, ll.quantum_bytes)
       /\ Has(ll, "batch_id")
       /\ Has(ll, "departure_time_ns")
       /\ dep >= ll.time_ns
       /\ ll.size_bytes > 0
       /\ ll.class_id < st3.class_count
       /\ ll.current_queue < st3.class_count
       /\ st3.packets_waiting > 0
       /\ ApplyBatchOk(st3, batchId, ll.time_ns)
       /\ AdvanceQueueOk(stA, ll.scan_steps)
       /\ stB.current_queue = ll.class_id
       /\ ~stB.batch_class_has \/ stB.batch_class = ll.class_id
       /\ Len(q) > 0
       /\ headPkt.packet_id = ll.packet_id
       /\ headPkt.flow_id = ll.flow_id
       /\ headPkt.size_bytes = ll.size_bytes
       /\ deficit0 > 0
       /\ ll.size_bytes <= deficit0
       /\ ll.current_queue = stC.current_queue
       /\ ll.deficit_bytes = stC.deficit[ll.class_id]
       /\ l' = l + 1
       /\ sched' = [sched EXCEPT ![sid] = stC]

Next == Enqueue \/ Schedule

TraceSpec == Init /\ [][Next]_Vars

ProgressOk == IF l <= LenTrace THEN ENABLED Next ELSE TRUE

=============================================================================
