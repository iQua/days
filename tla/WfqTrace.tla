------------------------------ MODULE WfqTrace ------------------------------
\* Trace validation baseline for Days WFQ (Weighted Fair Queueing).
\*
\* Intended to mirror:
\* - `lean/LeanGuard/Wfq/Semantics.lean`
\* - `lean/LeanGuard/WfqEventLog.lean`
\* - the corresponding implementation in `src/schedulers/wfq.rs` (under `--features lean`).
\*
\* This module expects a companion module named `TraceData` defining `Trace`.
\*
\* IMPORTANT: `TraceData` is rescaled to fit TLC's 32-bit integers:
\* - `*_ns` fields are stored in microseconds (rounded)
\* - `*_bps` fields are stored in units of 10 Mbps (rounded)
\*
\* This spec mirrors the Float-based implementation using **integer microseconds**:
\* - virtual time `vtime_ns` is interpreted as microseconds of virtual time
\* - finish/departure times are interpreted as microseconds
\* - service time is computed in microseconds using the scaled rate:
\*     10 Mbps == 10 bits/us, so service_us ~= bits / (rate_bps_scaled * 10 * weight)
\*
\* Because scaling introduces rounding, we accept a small tolerance on reported `vtime_ns`
\* and `finish_time_ns` (currently ±1 microsecond).

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

RoundDiv(n, d) == (n + d \div 2) \div d

AbsDiff(a, b) == IF a >= b THEN a - b ELSE b - a
ApproxEq(a, b, eps) == AbsDiff(a, b) <= eps

SchedulerIds == { Trace[i].scheduler_id : i \in 1..LenTrace }
ClassIds == { Trace[i].class_id : i \in 1..LenTrace }

PktKeys == { <<Trace[i].scheduler_id, Trace[i].flow_id, Trace[i].packet_id>> : i \in 1..LenTrace }

DefaultPkt ==
  [ present |-> FALSE,
    class_id |-> 0,
    size_bytes |-> 0,
    finish_time_ns |-> 0 ]

DefaultPending ==
  [ present |-> FALSE,
    key |-> <<0, 0, 0>>,
    class_id |-> 0,
    size_bytes |-> 0,
    finish_time_ns |-> 0,
    departure_time_ns |-> 0,
    schedule_idx |-> 0 ]

DefaultSched ==
  [ rate_bps |-> 0,
    vtime_ns |-> 0,
    last_updated_ns |-> 0,
    active_weight_sum |-> 0,
    last_time_has |-> FALSE,
    last_time_ns |-> 0,
    weights |-> [c \in ClassIds |-> 0],
    flow_counts |-> [c \in ClassIds |-> 0],
    finish_times |-> [c \in ClassIds |-> 0] ]

QueuedKeysForSid(queue, sid) ==
  { k \in PktKeys : k[1] = sid /\ queue[k].present }

MinOfSet(S) ==
  IF S = {} THEN
    0
  ELSE
    CHOOSE m \in S : \A x \in S : m <= x

MinFinish(queue, sid) ==
  LET S == { queue[k].finish_time_ns : k \in QueuedKeysForSid(queue, sid) }
  IN MinOfSet(S)

ServiceTimeUs(sizeBytes, rateBpsScaled, weight) ==
  RoundDiv(sizeBytes * 8, rateBpsScaled * 10 * weight)

LastTimeOk(st, t) == ~st.last_time_has \/ st.last_time_ns <= t

UpdateLastTime(st, t) ==
  [st EXCEPT !.last_time_has = TRUE, !.last_time_ns = t]

EnsureRateOk(st, r) == r > 0 /\ (st.rate_bps = 0 \/ st.rate_bps = r)

EnsureRateState(st, r) ==
  IF st.rate_bps = 0 THEN
    [st EXCEPT !.rate_bps = r]
  ELSE
    st

EnsureWeightOk(st, cid, w) == w > 0 /\ (st.weights[cid] = 0 \/ st.weights[cid] = w)

EnsureWeightState(st, cid, w) ==
  IF st.weights[cid] = 0 THEN
    [st EXCEPT !.weights[cid] = w]
  ELSE
    st

VARIABLES l, sched, queue, pending

Vars == <<l, sched, queue, pending>>

Init ==
  /\ l = 1
  /\ TraceOk
  /\ sched = [sid \in SchedulerIds |-> DefaultSched]
  /\ queue = [k \in PktKeys |-> DefaultPkt]
  /\ pending = [sid \in SchedulerIds |-> DefaultPending]

Enqueue ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         sid == ll.scheduler_id
         key == <<sid, ll.flow_id, ll.packet_id>>
         st0 == sched[sid]
         stT == UpdateLastTime(st0, ll.time_ns)
         st1 == EnsureRateState(stT, ll.rate_bps)
         st2 == EnsureWeightState(st1, ll.class_id, ll.weight)
         pk0 == pending[sid]
         q0 == queue[key]
         wsum == st2.active_weight_sum
         ftBase == IF wsum = 0 THEN [c \in ClassIds |-> 0] ELSE st2.finish_times
         vBase ==
           IF wsum = 0 THEN
             0
           ELSE
             st2.vtime_ns + RoundDiv(ll.time_ns - st2.last_updated_ns, wsum)
         prevFinish == ftBase[ll.class_id]
         vStart == IF vBase > prevFinish THEN vBase ELSE prevFinish
         serviceUs == ServiceTimeUs(ll.size_bytes, st2.rate_bps, ll.weight)
         finishUs == vStart + serviceUs
         count0 == st2.flow_counts[ll.class_id]
         count1 == count0 + 1
         wsum1 == IF count0 = 0 THEN wsum + ll.weight ELSE wsum
         st3 ==
           [st2 EXCEPT
             !.vtime_ns = vBase,
             !.last_updated_ns = ll.time_ns,
             !.active_weight_sum = wsum1,
             !.flow_counts[ll.class_id] = count1,
             !.finish_times = [ftBase EXCEPT ![ll.class_id] = finishUs]]
     IN
       /\ ll.kind = "enqueue"
       /\ LastTimeOk(st0, ll.time_ns)
       /\ EnsureRateOk(stT, ll.rate_bps)
       /\ EnsureWeightOk(st1, ll.class_id, ll.weight)
       /\ ll.size_bytes > 0
       /\ ~Has(ll, "departure_time_ns")
       /\ ~q0.present
       /\ ~pk0.present \/ pk0.key # key
       /\ ApproxEq(ll.vtime_ns, vBase, 1)
       /\ ApproxEq(ll.finish_time_ns, finishUs, 1)
       /\ l' = l + 1
       /\ sched' = [sched EXCEPT ![sid] = st3]
       /\ pending' = pending
       /\ queue' =
            [queue EXCEPT ![key] =
              [q0 EXCEPT
                !.present = TRUE,
                !.class_id = ll.class_id,
                !.size_bytes = ll.size_bytes,
                !.finish_time_ns = ll.finish_time_ns]]

Schedule ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         sid == ll.scheduler_id
         dep == ll.departure_time_ns
         key == <<sid, ll.flow_id, ll.packet_id>>
         st0 == sched[sid]
         stT == UpdateLastTime(st0, ll.time_ns)
         st1 == EnsureRateState(stT, ll.rate_bps)
         st2 == EnsureWeightState(st1, ll.class_id, ll.weight)
         pk0 == pending[sid]
         q0 == queue[key]
         minFinish == MinFinish(queue, sid)
     IN
       /\ ll.kind = "schedule"
       /\ LastTimeOk(st0, ll.time_ns)
       /\ EnsureRateOk(stT, ll.rate_bps)
       /\ EnsureWeightOk(st1, ll.class_id, ll.weight)
       /\ Has(ll, "departure_time_ns")
       /\ dep >= ll.time_ns
       /\ ~pk0.present
       /\ ApproxEq(ll.vtime_ns, st2.vtime_ns, 1)
       /\ q0.present
       /\ q0.class_id = ll.class_id
       /\ q0.size_bytes = ll.size_bytes
       /\ q0.finish_time_ns = ll.finish_time_ns
       /\ minFinish = ll.finish_time_ns
       /\ l' = l + 1
       /\ sched' = [sched EXCEPT ![sid] = st2]
       /\ queue' = [queue EXCEPT ![key] = [q0 EXCEPT !.present = FALSE]]
       /\ pending' =
            [pending EXCEPT ![sid] =
              [pk0 EXCEPT
                !.present = TRUE,
                !.key = key,
                !.class_id = ll.class_id,
                !.size_bytes = ll.size_bytes,
                !.finish_time_ns = ll.finish_time_ns,
                !.departure_time_ns = dep,
                !.schedule_idx = l]]

Depart ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         sid == ll.scheduler_id
         dep == ll.departure_time_ns
         key == <<sid, ll.flow_id, ll.packet_id>>
         st0 == sched[sid]
         stT == UpdateLastTime(st0, ll.time_ns)
         st1 == EnsureRateState(stT, ll.rate_bps)
         st2 == EnsureWeightState(st1, ll.class_id, ll.weight)
         pk0 == pending[sid]
         wsum == st2.active_weight_sum
         vNext ==
           IF wsum = 0 THEN
             st2.vtime_ns
           ELSE
             st2.vtime_ns + RoundDiv(dep - st2.last_updated_ns, wsum)
         count0 == st2.flow_counts[ll.class_id]
         count1 == count0 - 1
         wsum1 == IF count1 = 0 THEN wsum - st2.weights[ll.class_id] ELSE wsum
         vFinal == IF wsum1 = 0 THEN 0 ELSE vNext
         ftFinal ==
           IF wsum1 = 0 THEN
             [st2.finish_times EXCEPT ![ll.class_id] = 0]
           ELSE
             st2.finish_times
         st3 ==
           [st2 EXCEPT
             !.vtime_ns = vFinal,
             !.last_updated_ns = dep,
             !.active_weight_sum = wsum1,
             !.flow_counts[ll.class_id] = count1,
             !.finish_times = ftFinal]
     IN
       /\ ll.kind = "depart"
       /\ LastTimeOk(st0, ll.time_ns)
       /\ EnsureRateOk(stT, ll.rate_bps)
       /\ EnsureWeightOk(st1, ll.class_id, ll.weight)
       /\ Has(ll, "departure_time_ns")
       /\ dep = ll.time_ns
       /\ pk0.present
       /\ pk0.key = key
       /\ pk0.class_id = ll.class_id
       /\ pk0.size_bytes = ll.size_bytes
       /\ pk0.finish_time_ns = ll.finish_time_ns
       /\ pk0.departure_time_ns = dep
       /\ wsum > 0
       /\ count0 > 0
       /\ ApproxEq(ll.vtime_ns, vFinal, 1)
       /\ l' = l + 1
       /\ sched' = [sched EXCEPT ![sid] = st3]
       /\ queue' = queue
       /\ pending' = [pending EXCEPT ![sid] = [pk0 EXCEPT !.present = FALSE]]

Next == Enqueue \/ Schedule \/ Depart

TraceSpec == Init /\ [][Next]_Vars

ProgressOk == IF l <= LenTrace THEN ENABLED Next ELSE TRUE

=============================================================================
