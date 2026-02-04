------------------------------ MODULE CubicTrace ------------------------------
\* Trace validation baseline for Days TCP CUBIC traces.
\*
\* Intended to mirror:
\* - `lean/LeanGuard/Cubic/Semantics.lean`
\* - `lean/LeanGuard/CubicEventLog.lean`
\* - the implementation in `src/flows/cubic.rs` and CUBIC event logging in `src/flows/tcp_source.rs`.
\*
\* This module expects a companion module named `TraceData` defining `Trace`.
\*
\* IMPORTANT: `TraceData` is rescaled to fit TLC's 32-bit integers:
\* - `*_ns` fields are stored in microseconds
\* - `*_ppb` fields are stored in permille (PPB == 1000)
\*
\* NOTE: This baseline implements:
\* - slow-start ACK growth,
\* - congestion-avoidance ACK growth via a fixed-point approximation of RFC 8312's
\*   CUBIC window-growth function (scaled time + scaled segments),
\* - and the byte-level congestion/timeout reductions.
\*
\* Due to TraceData rescaling and the fixed-point approximation, this spec uses a
\* small tolerance when comparing the computed `cwnd_bytes` against the logged value.

EXTENDS Naturals, Integers, Sequences, TLC, FiniteSets, TraceData

PPB == 1000
TIME_SCALE == 100
\* Segment fixed-point scale: 1 segment == SEG_SCALE units.
SEG_SCALE == 1000
\* Allowed absolute error (bytes) in cwnd snapshot equality.
CWND_BYTES_TOL == 64

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

OkParams(ll) ==
  /\ ll.mss_bytes > 0
  /\ ll.beta_ppb > 0 /\ ll.beta_ppb < PPB
  /\ ll.c_ppb > 0
  /\ ll.init_cwnd_bytes > 0
  /\ ll.init_ssthresh_bytes > 0

AbsDiff(a, b) == IF a >= b THEN a - b ELSE b - a

ApproxEq(a, b, eps) == AbsDiff(a, b) <= eps

DefaultFlow ==
  [ present |-> FALSE,
    flow_id |-> 0,
    mss_bytes |-> 0,
    beta_ppb |-> 0,
    c_ppb |-> 0,
    tcp_friendly |-> FALSE,
    fast_convergence |-> FALSE,
    init_cwnd_bytes |-> 0,
    init_ssthresh_bytes |-> 0,
    cwnd_bytes |-> 0,
    ssthresh_bytes |-> 0,
    w_max_bytes |-> 0,
    w_last_max_bytes |-> 0,
    epoch_has |-> FALSE,
    epoch_start_ns |-> 0,
    srtt_scaled |-> 0,
    k_zero |-> TRUE,
    last_time_has |-> FALSE,
    last_time_ns |-> 0 ]

EndpointIds == { Trace[i].endpoint_id : i \in 1..LenTrace }

RoundDiv(n, d) == (n + (d \div 2)) \div d

Max2(a, b) == IF a >= b THEN a ELSE b

MulDiv(n, mul, div) ==
  LET q == n \div div
      r == n - (q * div)
  IN q * mul + (r * mul) \div div

ScaledTime(us) == RoundDiv(us * TIME_SCALE, 1000000)

BytesToMseg(bytes, mssBytes) ==
  LET q == bytes \div mssBytes
      r == bytes - (q * mssBytes)
  IN (q * SEG_SCALE) + (r * SEG_SCALE) \div mssBytes

MsegToBytes(mseg, mssBytes) ==
  LET q == mseg \div SEG_SCALE
      r == mseg - (q * SEG_SCALE)
  IN (q * mssBytes) + (r * mssBytes) \div SEG_SCALE

Cube(n) == n * n * n

CubicTermMseg(cPpb, tScaled) ==
  \* term_mseg = floor(c * t^3 * SEG_SCALE) where:
  \* - c = cPpb / PPB
  \* - t = tScaled / TIME_SCALE seconds
  \* With SEG_SCALE == PPB, this simplifies to:
  \*   floor(cPpb * tScaled^3 / TIME_SCALE^3)
  LET t3 == Cube(tScaled)
      denom == TIME_SCALE * TIME_SCALE * TIME_SCALE
      q == t3 \div denom
      r == t3 - (q * denom)
  IN (cPpb * q) + (cPpb * r) \div denom

UpdateSrttScaled(prevSrtt, rttUs) ==
  LET rttScaled == ScaledTime(rttUs)
  IN IF prevSrtt = 0 THEN
       rttScaled
     ELSE
       \* Standard TCP smoothing: (7/8)*srtt + (1/8)*rtt.
       (7 * prevSrtt + rttScaled + 4) \div 8

ReducedBytes(cwndBytes, betaPpb, mssBytes) ==
  LET reduced == MulDiv(cwndBytes, betaPpb, PPB)
  IN IF reduced < mssBytes THEN mssBytes ELSE reduced

SsthreshBytes(reducedBytes, mssBytes) ==
  LET twoMss == 2 * mssBytes
  IN IF reducedBytes < twoMss THEN twoMss ELSE reducedBytes

LastTimeOk(st, t) == ~st.last_time_has \/ st.last_time_ns <= t

UpdateLastTimeState(st, t) ==
  [st EXCEPT !.last_time_has = TRUE, !.last_time_ns = t]

EnsureParamsOk(st, ll) ==
  IF ~st.present THEN
    TRUE
  ELSE
    /\ st.flow_id = ll.flow_id
    /\ st.mss_bytes = ll.mss_bytes
    /\ st.beta_ppb = ll.beta_ppb
    /\ st.c_ppb = ll.c_ppb
    /\ st.tcp_friendly = ll.tcp_friendly
    /\ st.fast_convergence = ll.fast_convergence
    /\ st.init_cwnd_bytes = ll.init_cwnd_bytes
    /\ st.init_ssthresh_bytes = ll.init_ssthresh_bytes

EnsureParamsState(st, ll) ==
  IF ~st.present THEN
    [ st EXCEPT
      !.present = TRUE,
      !.flow_id = ll.flow_id,
      !.mss_bytes = ll.mss_bytes,
      !.beta_ppb = ll.beta_ppb,
      !.c_ppb = ll.c_ppb,
      !.tcp_friendly = ll.tcp_friendly,
      !.fast_convergence = ll.fast_convergence,
      !.init_cwnd_bytes = ll.init_cwnd_bytes,
      !.init_ssthresh_bytes = ll.init_ssthresh_bytes,
      !.cwnd_bytes = ll.init_cwnd_bytes,
      !.ssthresh_bytes = ll.init_ssthresh_bytes,
      !.w_max_bytes = 0,
      !.w_last_max_bytes = 0,
      !.epoch_has = FALSE,
      !.epoch_start_ns = 0,
      !.srtt_scaled = 0,
      !.k_zero = TRUE ]
  ELSE
    st

VARIABLES l, flow

Vars == <<l, flow>>

Init ==
  /\ l = 1
  /\ TraceOk
  /\ flow = [eid \in EndpointIds |-> DefaultFlow]

Ack ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         eid == ll.endpoint_id
         st0 == flow[eid]
         stT == UpdateLastTimeState(st0, ll.time_ns)
         st1 == EnsureParamsState(stT, ll)
         acked == ll.acked_segs
         rttUs == ll.rtt_ns
         srtt1 == UpdateSrttScaled(st1.srtt_scaled, rttUs)
         ssPre == st1.cwnd_bytes < st1.ssthresh_bytes
         cwndSsNext == st1.cwnd_bytes + (acked * st1.mss_bytes)
         enterCa == ssPre /\ cwndSsNext >= st1.ssthresh_bytes
         epochHasSs == IF enterCa THEN TRUE ELSE st1.epoch_has
         epochStartSs == IF enterCa THEN ll.time_ns ELSE st1.epoch_start_ns
         wMaxSs == IF enterCa /\ st1.w_max_bytes = 0 THEN cwndSsNext ELSE st1.w_max_bytes
         kZeroSs == IF enterCa /\ st1.w_max_bytes = 0 THEN TRUE ELSE st1.k_zero
         \* Congestion-avoidance update (fixed-point, approximate).
         epochHasCa == TRUE
         epochStartCa == IF st1.epoch_has THEN st1.epoch_start_ns ELSE ll.time_ns
         wMaxCa == IF st1.w_max_bytes = 0 THEN st1.cwnd_bytes ELSE st1.w_max_bytes
         kZeroCa == IF st1.w_max_bytes = 0 THEN TRUE ELSE st1.k_zero
         tScaled == ScaledTime(ll.time_ns - epochStartCa)
         wMaxMseg == BytesToMseg(wMaxCa, st1.mss_bytes)
         cwndMseg == BytesToMseg(st1.cwnd_bytes, st1.mss_bytes)
         wCubicTMseg == wMaxMseg + CubicTermMseg(st1.c_ppb, tScaled)
         wMaxBetaMseg == MulDiv(wMaxMseg, st1.beta_ppb, PPB)
         aNum == 3 * (PPB - st1.beta_ppb)
         aDen == (PPB + st1.beta_ppb) * Max2(srtt1, 1)
         bNum == aNum * tScaled
         bQ == bNum \div aDen
         bR == bNum - (bQ * aDen)
         secondMseg == (bQ * SEG_SCALE) + (bR * SEG_SCALE) \div aDen
         wEstMseg == wMaxBetaMseg + secondMseg
         caTargetScaled == tScaled + srtt1
         wTargetMseg == wMaxMseg + CubicTermMseg(st1.c_ppb, caTargetScaled)
         denomMseg == Max2(cwndMseg, SEG_SCALE)
         diffMseg == wTargetMseg - cwndMseg
         deltaMseg == (diffMseg * SEG_SCALE) \div denomMseg
         cwndCaNextMseg ==
           IF st1.tcp_friendly /\ wCubicTMseg < wEstMseg THEN wEstMseg ELSE cwndMseg + deltaMseg
         cwndCaNextBytes == MsegToBytes(cwndCaNextMseg, st1.mss_bytes)
         st2 ==
           IF ssPre THEN
             [st1 EXCEPT
               !.cwnd_bytes = cwndSsNext,
               !.epoch_has = epochHasSs,
               !.epoch_start_ns = epochStartSs,
               !.w_max_bytes = wMaxSs,
               !.srtt_scaled = srtt1,
               !.k_zero = kZeroSs]
           ELSE
             [st1 EXCEPT
               !.cwnd_bytes = cwndCaNextBytes,
               !.epoch_has = epochHasCa,
               !.epoch_start_ns = epochStartCa,
               !.w_max_bytes = wMaxCa,
               !.srtt_scaled = srtt1,
               !.k_zero = kZeroCa]
     IN
       /\ ll.kind = "ack"
       /\ OkParams(ll)
       /\ LastTimeOk(st0, ll.time_ns)
       /\ EnsureParamsOk(stT, ll)
        /\ Has(ll, "acked_segs") /\ acked > 0
        /\ Has(ll, "rtt_ns") /\ rttUs > 0
        /\ (~st1.epoch_has \/ st1.epoch_start_ns <= ll.time_ns)
       /\ ApproxEq(ll.cwnd_bytes, st2.cwnd_bytes, CWND_BYTES_TOL)
       /\ ll.ssthresh_bytes = st2.ssthresh_bytes
       /\ ll.w_max_bytes = st2.w_max_bytes
       /\ ll.w_last_max_bytes = st2.w_last_max_bytes
       /\ (IF st2.epoch_has THEN Has(ll, "epoch_start_ns") /\ ll.epoch_start_ns = st2.epoch_start_ns
          ELSE ~Has(ll, "epoch_start_ns"))
       /\ l' = l + 1
       /\ flow' = [flow EXCEPT ![eid] = st2]

Congestion ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         eid == ll.endpoint_id
         st0 == flow[eid]
         stT == UpdateLastTimeState(st0, ll.time_ns)
         st1 == EnsureParamsState(stT, ll)
         cwnd0 == st1.cwnd_bytes
         reduced == ReducedBytes(cwnd0, st1.beta_ppb, st1.mss_bytes)
         ssthresh1 == SsthreshBytes(reduced, st1.mss_bytes)
         wMaxCur == cwnd0
         fast == st1.fast_convergence
         wLast0 == st1.w_last_max_bytes
         fcCond == fast /\ wLast0 > 0 /\ wMaxCur < wLast0
         wLast1 == wMaxCur
         wMax1 ==
           IF fcCond THEN
             MulDiv(wMaxCur, (PPB + st1.beta_ppb), 2 * PPB)
           ELSE
             wMaxCur
         st2 ==
           [st1 EXCEPT
             !.cwnd_bytes = reduced,
             !.ssthresh_bytes = ssthresh1,
             !.w_max_bytes = wMax1,
             !.w_last_max_bytes = wLast1,
             !.epoch_has = TRUE,
             !.epoch_start_ns = ll.time_ns,
             !.k_zero = FALSE]
     IN
       /\ ll.kind = "congestion"
       /\ OkParams(ll)
       /\ LastTimeOk(st0, ll.time_ns)
       /\ EnsureParamsOk(stT, ll)
        /\ ~Has(ll, "acked_segs")
        /\ ~Has(ll, "rtt_ns")
       /\ ll.cwnd_bytes = st2.cwnd_bytes
       /\ ll.ssthresh_bytes = st2.ssthresh_bytes
       /\ ll.w_max_bytes = st2.w_max_bytes
       /\ ll.w_last_max_bytes = st2.w_last_max_bytes
       /\ Has(ll, "epoch_start_ns") /\ ll.epoch_start_ns = st2.epoch_start_ns
       /\ l' = l + 1
       /\ flow' = [flow EXCEPT ![eid] = st2]

Timeout ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l]
         eid == ll.endpoint_id
         st0 == flow[eid]
         stT == UpdateLastTimeState(st0, ll.time_ns)
         st1 == EnsureParamsState(stT, ll)
         cwnd0 == st1.cwnd_bytes
         reduced == ReducedBytes(cwnd0, st1.beta_ppb, st1.mss_bytes)
         ssthresh1 == SsthreshBytes(reduced, st1.mss_bytes)
         st2 ==
           [st1 EXCEPT
             !.cwnd_bytes = st1.mss_bytes,
             !.ssthresh_bytes = ssthresh1,
             !.w_max_bytes = 0,
             !.w_last_max_bytes = 0,
             !.epoch_has = FALSE,
             !.epoch_start_ns = 0,
             !.k_zero = TRUE]
     IN
       /\ ll.kind = "timeout"
       /\ OkParams(ll)
       /\ LastTimeOk(st0, ll.time_ns)
       /\ EnsureParamsOk(stT, ll)
        /\ ~Has(ll, "acked_segs")
        /\ ~Has(ll, "rtt_ns")
       /\ ll.cwnd_bytes = st2.cwnd_bytes
       /\ ll.ssthresh_bytes = st2.ssthresh_bytes
       /\ ll.w_max_bytes = st2.w_max_bytes
       /\ ll.w_last_max_bytes = st2.w_last_max_bytes
       /\ ~Has(ll, "epoch_start_ns")
       /\ l' = l + 1
       /\ flow' = [flow EXCEPT ![eid] = st2]

Next == Ack \/ Congestion \/ Timeout

TraceSpec == Init /\ [][Next]_Vars

ProgressOk == IF l <= LenTrace THEN ENABLED Next ELSE TRUE

=============================================================================
