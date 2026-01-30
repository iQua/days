------------------------------ MODULE PfcTrace ------------------------------
\* Trace validation baseline for Days PFC (Priority Flow Control).
\*
\* Intended to mirror:
\* - `lean/LeanGuard/Pfc/Semantics.lean`
\* - `lean/LeanGuard/PfcEventLog.lean`
\*
\* This module expects a companion module named `TraceData` defining `Trace`.
\*
\* IMPORTANT: `TraceData` is rescaled to fit TLC's 32-bit integers:
\* - `*_ns` fields are stored in microseconds
\* - `*_ppb` fields are stored in permille (unused here)
\* - `*_bps` fields are stored in units of 10 Mbps (unused here)

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

OneHot(prio) == 2^prio

PendingKeys == { <<Trace[i].pfc_frame_id, Trace[i].priority>> : i \in 1..LenTrace }
PauseKeys == { <<Trace[i].sender_id, Trace[i].priority>> : i \in 1..LenTrace }

DefaultPending ==
  [ present |-> FALSE,
    sent_time_ns |-> 0,
    sent_event_id |-> 0,
    sender_id |-> 0,
    receiver_id |-> 0,
    class_enable |-> 0,
    pause_quanta |-> 0 ]

VARIABLES l, pending, paused

Vars == <<l, pending, paused>>

Init ==
  /\ l = 1
  /\ TraceOk
  /\ pending = [k \in PendingKeys |-> DefaultPending]
  /\ paused = [k \in PauseKeys |-> FALSE]

CheckCommon(ll) ==
  /\ ll.priority < 8
  /\ ll.pause_quanta <= 65535
  /\ ll.class_enable = OneHot(ll.priority)

PfcSent ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         pkey == <<ll.pfc_frame_id, ll.priority>>
         wasPaused == paused[<<ll.sender_id, ll.priority>>]
         pk0 == pending[pkey]
         occ == ll.queue_occupancy_bytes
         xoff == ll.xoff_threshold_bytes
         xon == ll.xon_threshold_bytes
         cap == ll.buffer_capacity_bytes
     IN
       /\ ll.kind = "pfc_sent"
       /\ CheckCommon(ll)
       /\ Has(ll, "queue_occupancy_bytes")
       /\ Has(ll, "xoff_threshold_bytes")
       /\ Has(ll, "xon_threshold_bytes")
       /\ Has(ll, "buffer_capacity_bytes")
       /\ xon <= xoff
       /\ cap = 0 \/ occ <= cap
       /\ ~pk0.present
       /\ IF ll.pause_quanta = 0 THEN
            /\ wasPaused
            /\ occ <= xon
          ELSE
            IF wasPaused THEN
              occ > xon
            ELSE
              occ >= xoff
       /\ l' = l + 1
       /\ paused' =
            IF ll.pause_quanta = 0 THEN
              [paused EXCEPT ![<<ll.sender_id, ll.priority>>] = FALSE]
            ELSE
              [paused EXCEPT ![<<ll.sender_id, ll.priority>>] = TRUE]
       /\ pending' =
            [pending EXCEPT ![pkey] =
              [pk0 EXCEPT
                !.present = TRUE,
                !.sent_time_ns = ll.time_ns,
                !.sent_event_id = ll.event_id,
                !.sender_id = ll.sender_id,
                !.receiver_id = ll.receiver_id,
                !.class_enable = ll.class_enable,
                !.pause_quanta = ll.pause_quanta]]

PfcRecv ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         pkey == <<ll.pfc_frame_id, ll.priority>>
         pk0 == pending[pkey]
     IN
       /\ ll.kind = "pfc_recv"
       /\ CheckCommon(ll)
       /\ pk0.present
       /\ KeyLt(<<pk0.sent_time_ns, pk0.sent_event_id>>, <<ll.time_ns, ll.event_id>>)
       /\ ll.sender_id = pk0.sender_id
       /\ ll.receiver_id = pk0.receiver_id
       /\ ll.class_enable = pk0.class_enable
       /\ ll.pause_quanta = pk0.pause_quanta
       /\ l' = l + 1
       /\ paused' = paused
       /\ pending' = [pending EXCEPT ![pkey] = [pk0 EXCEPT !.present = FALSE]]

Next == PfcSent \/ PfcRecv

TraceSpec == Init /\ [][Next]_Vars

=============================================================================

