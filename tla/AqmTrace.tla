------------------------------ MODULE AqmTrace ------------------------------
\* Trace validation baseline for Days AQM decisions.
\*
\* Intended to mirror the checks in:
\* - `lean/LeanGuard/Aqm/Semantics.lean`
\* - `lean/LeanGuard/AqmEventLog.lean`
\*
\* This module expects a companion module named `TraceData` that defines:
\*   Trace == << [ time_ns |-> ..., event_id |-> ..., ... ], ... >>
\*
\* IMPORTANT: `TraceData` is rescaled to fit TLC's 32-bit integers:
\* - `*_ns`  fields are stored in microseconds
\* - `*_ppb` fields are stored in permille (PPB == 1000)
\* - `*_bps` fields are stored in units of 10 Mbps (unused here)

EXTENDS Naturals, Integers, Sequences, TLC, TraceData

PPB == 1000

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

ApproxLeq(a, b, eps) == a <= b + eps

EcnMarkAllowed(before, after) ==
  ~((before = "not_ect") /\ (after = "ce"))

ThresholdCap(ppb, cap) == (ppb * cap) \div PPB

QueueOverflow(ll) ==
  IF ll.capacity = 0 THEN
    FALSE
  ELSE
    IF ll.capacity_unit = "bytes" THEN
      ll.byte_length + ll.size_bytes > ll.capacity
    ELSE
      ll.queue_length + 1 > ll.capacity

ExceedsThreshold(ll, ppb) ==
  IF ll.capacity = 0 THEN
    FALSE
  ELSE
    IF ll.capacity_unit = "bytes" THEN
      ll.byte_length + ll.size_bytes > ThresholdCap(ppb, ll.capacity)
    ELSE
      ll.queue_length + 1 > ThresholdCap(ppb, ll.capacity)

RedProbPpb(ll, minPpb, maxPpb, maxProbPpb, avg) ==
  IF maxPpb <= minPpb THEN
    0
  ELSE
    LET minCap == ThresholdCap(minPpb, ll.capacity)
        diff == IF avg > minCap THEN avg - minCap ELSE 0
    IN (diff * ll.capacity * maxProbPpb) \div (maxPpb - minPpb)

RedDecisionOk(ll) ==
  /\ Has(ll, "red_min_threshold_ppb")
  /\ Has(ll, "red_max_threshold_ppb")
  /\ Has(ll, "red_max_probability_ppb")
  /\ Has(ll, "red_avg_queue_length")
  /\ LET minPpb == ll.red_min_threshold_ppb
         maxPpb == ll.red_max_threshold_ppb
         maxProbPpb == ll.red_max_probability_ppb
         avg == ll.red_avg_queue_length
         overMax == ExceedsThreshold(ll, maxPpb)
         overMin == ExceedsThreshold(ll, minPpb)
         overflow == QueueOverflow(ll)
         maxRandOk == IF overMax THEN Has(ll, "red_rand_max_ppb") ELSE TRUE
         minRandOk == IF overMin THEN Has(ll, "red_rand_min_ppb") ELSE TRUE
         maxHit ==
           IF Has(ll, "red_rand_max_ppb") THEN
             ApproxLeq(ll.red_rand_max_ppb, maxProbPpb, 1)
           ELSE
             FALSE
         minProbPpb == RedProbPpb(ll, minPpb, maxPpb, maxProbPpb, avg)
         minHit ==
           IF Has(ll, "red_rand_min_ppb") THEN
             ApproxLeq(ll.red_rand_min_ppb, minProbPpb, 1)
           ELSE
             FALSE
         shouldMark == (overMax /\ maxHit) \/ (overMin /\ minHit)
     IN
       /\ maxRandOk
       /\ minRandOk
       /\ IF overflow THEN
            ll.action = "drop"
          ELSE
            CASE ll.drop_strategy = "red" ->
                IF shouldMark THEN ll.action = "drop" ELSE ll.action = "enqueue"
              [] ll.drop_strategy = "red_ecn" ->
                IF shouldMark THEN
                  (ll.action = "mark_ecn" \/ (ll.action = "drop" /\ ll.ecn_before = "not_ect"))
                ELSE
                  ll.action = "enqueue"
              [] OTHER -> FALSE

TailDropOk(ll) ==
  LET overflow == QueueOverflow(ll)
  IN IF overflow THEN ll.action = "drop" ELSE ll.action = "enqueue"

EcnThresholdOk(ll) ==
  /\ Has(ll, "ecn_threshold_ppb")
  /\ LET overflow == QueueOverflow(ll)
         thresh == ExceedsThreshold(ll, ll.ecn_threshold_ppb)
     IN
       IF overflow THEN
         ll.action = "drop"
       ELSE IF thresh THEN
         (ll.action = "mark_ecn" \/ (ll.action = "drop" /\ ll.ecn_before = "not_ect"))
       ELSE
         ll.action = "enqueue"

DecisionOk(ll) ==
  /\ ll.kind = "decision"
  /\ EcnMarkAllowed(ll.ecn_before, ll.ecn_after)
  /\ CASE ll.drop_strategy = "tail_drop"      -> TailDropOk(ll)
     [] ll.drop_strategy = "ecn_threshold"   -> EcnThresholdOk(ll)
     [] ll.drop_strategy = "red"             -> RedDecisionOk(ll)
     [] ll.drop_strategy = "red_ecn"         -> RedDecisionOk(ll)
     [] OTHER                                -> FALSE

VARIABLES l

Vars == <<l>>

Init ==
  /\ l = 1
  /\ TraceOk

Next ==
  /\ l \in 1..LenTrace
  /\ LET ll == Trace[l] IN
       /\ DecisionOk(ll)
  /\ l' = l + 1

TraceSpec == Init /\ [][Next]_Vars

=============================================================================

