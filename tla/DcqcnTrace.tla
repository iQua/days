------------------------------ MODULE DcqcnTrace ------------------------------
\* Trace validation baseline for Days.
\*
\* This module is intended to mirror the checks in `lean/LeanGuard/DcqcnEventLog.lean`:
\* - canonical total order by (time_ns, event_id)
\* - CNP packet field constraints
\* - sink CNP interval gating + pending pairing
\* - source parameter constancy + time monotonicity
\* - replay-like alpha/rate updates (in the integer domain) with snapshot equality
\*
\* Trace loading (TLC): this module expects a companion module named `TraceData` that
\* defines `Trace` as a sequence of records (one per CSV row). The intended workflow is:
\*
\* - export `dcqcn_events.csv` in canonical order (sort by `(time_ns,event_id)`)
\* - generate `TraceData.tla` next to this module
\* - run TLC on this module.
\*
\* IMPORTANT: `TraceData.tla` is rescaled to fit TLC's 32-bit integers:
\* - `*_ns` fields are stored in microseconds
\* - `*_bps` fields are stored in units of 10 Mbps
\* - `*_ppb` fields are stored in permille (PPB == 1000)

EXTENDS Naturals, Integers, Sequences, TLC, FiniteSets, TraceData

PPB == 1000
PPB2 == PPB * PPB

LenTrace == Len(Trace)

Key(rec) == <<rec.time_ns, rec.event_id>>
KeyLt(k1, k2) ==
  (k1[1] < k2[1]) \/ (k1[1] = k2[1] /\ k1[2] < k2[2])

TraceOk ==
  IF LenTrace <= 1 THEN
    TRUE
  ELSE
    \A i \in 1..(LenTrace - 1) : KeyLt(Key(Trace[i]), Key(Trace[i + 1]))

EndpointIds == { Trace[i].endpoint_id : i \in 1..LenTrace }

CnpIndices == { i \in 1..LenTrace : Trace[i].kind \in {"cnp_sent", "cnp_recv"} }

PendingKeys ==
  { <<Trace[i].flow_id, Trace[i].pkt_id>> : i \in CnpIndices }

Has(ll, field) == field \in DOMAIN ll

RoundDiv(n, d) == (n + d \div 2) \div d

ClampMin(x, lo) == IF x < lo THEN lo ELSE x
ClampMax(x, hi) == IF x < hi THEN x ELSE hi

AbsDiff(a, b) == IF a >= b THEN a - b ELSE b - a
ApproxEq(a, b, eps) == AbsDiff(a, b) <= eps

MatchOptNat(ll, field, hasVal, val) ==
  IF hasVal THEN
    /\ Has(ll, field)
    /\ ll[field] = val
  ELSE
    ~Has(ll, field)

OkSrcParams(p) ==
  /\ p.g_ppb <= PPB
  /\ p.mi_ppb <= PPB
  /\ p.min_rate_bps <= p.init_rate_bps
  /\ p.init_rate_bps <= p.max_rate_bps

DefaultSrc ==
  [ present |-> FALSE,
    flow_id |-> 0,
    cnp_interval_ns |-> 0,
    g_ppb |-> 0,
    mi_ppb |-> 0,
    init_rate_bps |-> 0,
    min_rate_bps |-> 0,
    max_rate_bps |-> 0,
    ai_rate_bps |-> 0,
    hai_rate_bps |-> 0,
    alpha_ppb |-> 0,
    rate_bps |-> 0,
    cnp_seen |-> FALSE,
    has_last_cnp |-> FALSE,
    last_cnp_ns |-> 0,
    has_last_time |-> FALSE,
    last_time_ns |-> 0 ]

DefaultSink ==
  [ present |-> FALSE,
    flow_id |-> 0,
    cnp_interval_ns |-> 0,
    cnp_priority |-> 0,
    has_last_cnp |-> FALSE,
    last_cnp_ns |-> 0,
    has_last_time |-> FALSE,
    last_time_ns |-> 0 ]

DefaultPending ==
  [ present |-> FALSE,
    sent_time_ns |-> 0,
    sent_event_id |-> 0 ]

VARIABLES l, src, sink, pending

Vars == <<l, src, sink, pending>>

Init ==
  /\ l = 1
  /\ TraceOk
  /\ src = [e \in EndpointIds |-> DefaultSrc]
  /\ sink = [e \in EndpointIds |-> DefaultSink]
  /\ pending = [k \in PendingKeys |-> DefaultPending]

InitSrcFrom(ll) ==
  [ present |-> TRUE,
    flow_id |-> ll.flow_id,
    cnp_interval_ns |-> ll.cnp_interval_ns,
    g_ppb |-> ll.g_ppb,
    mi_ppb |-> ll.mi_ppb,
    init_rate_bps |-> ll.init_rate_bps,
    min_rate_bps |-> ll.min_rate_bps,
    max_rate_bps |-> ll.max_rate_bps,
    ai_rate_bps |-> ll.ai_rate_bps,
    hai_rate_bps |-> ll.hai_rate_bps,
    alpha_ppb |-> 0,
    rate_bps |-> ll.init_rate_bps,
    cnp_seen |-> FALSE,
    has_last_cnp |-> FALSE,
    last_cnp_ns |-> 0,
    has_last_time |-> FALSE,
    last_time_ns |-> 0 ]

InitSinkFrom(ll) ==
  [ present |-> TRUE,
    flow_id |-> ll.flow_id,
    cnp_interval_ns |-> ll.cnp_interval_ns,
    cnp_priority |-> ll.cnp_priority,
    has_last_cnp |-> FALSE,
    last_cnp_ns |-> 0,
    has_last_time |-> FALSE,
    last_time_ns |-> 0 ]

CnpPacketOk(ll) ==
  /\ Has(ll, "pkt_id")
  /\ Has(ll, "pkt_flow_id")
  /\ ll.pkt_flow_id = ll.flow_id
  /\ Has(ll, "cnp_size_b") /\ ll.cnp_size_b = 64
  /\ Has(ll, "cnp_ecn") /\ ll.cnp_ecn = "NotEct"
  /\ Has(ll, "cnp_cwr") /\ ll.cnp_cwr = FALSE
  /\ Has(ll, "cnp_last_packet") /\ ll.cnp_last_packet = FALSE

CnpSent ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         eid == ll.endpoint_id
         fid == ll.flow_id
         pktId == ll.pkt_id
         pkey == <<fid, pktId>>
         sk0 == sink[eid]
         sk1 == IF sk0.present THEN sk0 ELSE InitSinkFrom(ll)
         pk0 == pending[pkey]
     IN
       /\ ll.kind = "cnp_sent"
       /\ CnpPacketOk(ll)
       /\ Has(ll, "trigger_ecn") /\ ll.trigger_ecn = "Ce"
       /\ Has(ll, "cnp_priority")
       /\ Has(ll, "cnp_interval_ns")
       /\ Has(ll, "last_cnp_ns") /\ ll.last_cnp_ns = ll.time_ns
       /\ IF sk0.present THEN
            /\ sk0.flow_id = fid
            /\ sk0.cnp_interval_ns = ll.cnp_interval_ns
            /\ sk0.cnp_priority = ll.cnp_priority
          ELSE TRUE
       /\ ~sk1.has_last_time \/ sk1.last_time_ns <= ll.time_ns
       /\ ~sk1.has_last_cnp \/ sk1.last_cnp_ns + sk1.cnp_interval_ns <= ll.time_ns
       /\ ~pk0.present
       /\ l' = l + 1
       /\ src' = src
       /\ sink' =
            [sink EXCEPT ![eid] =
              [sk1 EXCEPT
                !.present = TRUE,
                !.has_last_time = TRUE,
                !.last_time_ns = ll.time_ns,
                !.has_last_cnp = TRUE,
                !.last_cnp_ns = ll.time_ns]]
       /\ pending' =
            [pending EXCEPT ![pkey] =
              [pk0 EXCEPT
                !.present = TRUE,
                !.sent_time_ns = ll.time_ns,
                !.sent_event_id = ll.event_id]]

CnpRecv ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         eid == ll.endpoint_id
         fid == ll.flow_id
         pktId == ll.pkt_id
         pkey == <<fid, pktId>>
         pk0 == pending[pkey]
         sentKey == <<pk0.sent_time_ns, pk0.sent_event_id>>
         recvKey == Key(ll)
         sPre0 == src[eid]
         s0 == IF sPre0.present THEN sPre0 ELSE InitSrcFrom(ll)
         applied == ~s0.has_last_cnp \/ (s0.last_cnp_ns + s0.cnp_interval_ns <= ll.time_ns)
         alpha1 ==
           IF applied THEN
             RoundDiv((PPB - s0.g_ppb) * s0.alpha_ppb + s0.g_ppb * PPB, PPB)
           ELSE
             s0.alpha_ppb
         decNumer == PPB2 - (s0.mi_ppb * alpha1)
         rateDecr ==
           IF applied THEN
             RoundDiv(s0.rate_bps * decNumer, PPB2)
           ELSE
             s0.rate_bps
         rate1 == ClampMin(rateDecr, s0.min_rate_bps)
         seen1 == IF applied THEN TRUE ELSE s0.cnp_seen
         hasLastCnp1 == IF applied THEN TRUE ELSE s0.has_last_cnp
         lastCnp1 == IF applied THEN ll.time_ns ELSE s0.last_cnp_ns
         s1 ==
           [s0 EXCEPT
             !.present = TRUE,
             !.alpha_ppb = alpha1,
             !.rate_bps = rate1,
             !.cnp_seen = seen1,
             !.has_last_cnp = hasLastCnp1,
             !.last_cnp_ns = lastCnp1,
             !.has_last_time = TRUE,
             !.last_time_ns = ll.time_ns]
     IN
       /\ ll.kind = "cnp_recv"
       /\ CnpPacketOk(ll)
       /\ pk0.present
       /\ KeyLt(sentKey, recvKey)
       /\ IF sPre0.present THEN
            /\ sPre0.flow_id = ll.flow_id
            /\ sPre0.cnp_interval_ns = ll.cnp_interval_ns
            /\ sPre0.g_ppb = ll.g_ppb
            /\ sPre0.mi_ppb = ll.mi_ppb
            /\ sPre0.init_rate_bps = ll.init_rate_bps
            /\ sPre0.min_rate_bps = ll.min_rate_bps
            /\ sPre0.max_rate_bps = ll.max_rate_bps
            /\ sPre0.ai_rate_bps = ll.ai_rate_bps
            /\ sPre0.hai_rate_bps = ll.hai_rate_bps
          ELSE TRUE
       /\ OkSrcParams(ll)
       /\ ~s0.has_last_time \/ s0.last_time_ns <= ll.time_ns
       /\ Has(ll, "alpha_ppb") /\ ApproxEq(ll.alpha_ppb, alpha1, 1)
       /\ Has(ll, "rate_bps") /\ ApproxEq(ll.rate_bps, rate1, 1)
       /\ Has(ll, "cnp_seen") /\ ll.cnp_seen = seen1
       /\ MatchOptNat(ll, "last_cnp_ns", hasLastCnp1, lastCnp1)
       /\ l' = l + 1
       /\ sink' = sink
       /\ src' = [src EXCEPT ![eid] = s1]
       /\ pending' = [pending EXCEPT ![pkey] = [pk0 EXCEPT !.present = FALSE]]

TimerTick ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         eid == ll.endpoint_id
         sPre0 == src[eid]
         s0 == IF sPre0.present THEN sPre0 ELSE InitSrcFrom(ll)
         decNumer == (PPB - s0.g_ppb) * s0.alpha_ppb
         alphaDecPpb == RoundDiv(decNumer, PPB)
         alphaDecIsSmall == (decNumer * 10) < PPB2
         inc == IF alphaDecIsSmall THEN s0.hai_rate_bps ELSE s0.ai_rate_bps
         rateInc == ClampMax(s0.rate_bps + inc, s0.max_rate_bps)
         alpha1 == IF s0.cnp_seen THEN s0.alpha_ppb ELSE alphaDecPpb
         rate1 == IF s0.cnp_seen THEN s0.rate_bps ELSE rateInc
         s1 ==
           [s0 EXCEPT
             !.present = TRUE,
             !.alpha_ppb = alpha1,
             !.rate_bps = rate1,
             !.cnp_seen = FALSE,
             !.has_last_time = TRUE,
             !.last_time_ns = ll.time_ns]
     IN
       /\ ll.kind = "timer_tick"
       /\ IF sPre0.present THEN
            /\ sPre0.flow_id = ll.flow_id
            /\ sPre0.cnp_interval_ns = ll.cnp_interval_ns
            /\ sPre0.g_ppb = ll.g_ppb
            /\ sPre0.mi_ppb = ll.mi_ppb
            /\ sPre0.init_rate_bps = ll.init_rate_bps
            /\ sPre0.min_rate_bps = ll.min_rate_bps
            /\ sPre0.max_rate_bps = ll.max_rate_bps
            /\ sPre0.ai_rate_bps = ll.ai_rate_bps
            /\ sPre0.hai_rate_bps = ll.hai_rate_bps
          ELSE TRUE
       /\ OkSrcParams(ll)
       /\ ~s0.has_last_time \/ s0.last_time_ns <= ll.time_ns
       /\ Has(ll, "alpha_ppb") /\ ApproxEq(ll.alpha_ppb, alpha1, 1)
       /\ Has(ll, "rate_bps") /\ ApproxEq(ll.rate_bps, rate1, 1)
       /\ Has(ll, "cnp_seen") /\ ll.cnp_seen = FALSE
       /\ MatchOptNat(ll, "last_cnp_ns", s0.has_last_cnp, s0.last_cnp_ns)
       /\ l' = l + 1
       /\ sink' = sink
       /\ pending' = pending
       /\ src' = [src EXCEPT ![eid] = s1]

Next == CnpSent \/ CnpRecv \/ TimerTick

TraceSpec == Init /\ [][Next]_Vars

===============================================================================
