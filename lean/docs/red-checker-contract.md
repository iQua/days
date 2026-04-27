# RED checker contract

This document records the RED behavior that `aqm_check` is intended to
validate.

## Sources

- Floyd and Jacobson, "Random Early Detection gateways for Congestion
  Avoidance", IEEE/ACM Transactions on Networking, 1993.
- RFC 2309, "Recommendations on Queue Management and Congestion Avoidance in
  the Internet".
- RFC 3168, "The Addition of Explicit Congestion Notification (ECN) to IP".

## RED decision model

The RED decision is made on each packet arrival. RED first computes an
exponentially weighted moving average queue length, `avg`, then compares `avg`
against `minth` and `maxth`.

- If `avg < minth`, RED does not signal congestion.
- If `minth <= avg < maxth`, RED signals congestion probabilistically.
- If `maxth <= avg`, RED signals congestion for every arriving packet.

For a non-ECN RED queue, signaling congestion means dropping the packet. For a
RED_ECN queue, signaling congestion means marking an ECN-capable packet with
CE; a Not-ECT packet is dropped instead.

Physical queue capacity is a separate hard invariant. If the arriving packet
does not fit in the configured queue capacity, the only valid action is `drop`,
even when the RED average would otherwise permit enqueue or ECN marking.

## CSV witness contract

The checker consumes one `decision` row per packet-arrival decision.

- `queue_length` and `byte_length` describe instantaneous occupancy before the
  arriving packet is admitted.
- `capacity` and `capacity_unit` describe physical queue capacity. A capacity
  of zero means unlimited capacity.
- `red_avg_queue_length` is the RED EWMA `avg` in the same queue units used for
  the configured capacity.
- `red_min_threshold_ppb` and `red_max_threshold_ppb` encode `minth` and
  `maxth` as fractions of `capacity`, in parts per billion.
- `red_max_probability_ppb` encodes `maxp`, in parts per billion.
- `red_rand_min_ppb` is the random witness for the between-threshold RED
  probability decision. It is not used below `minth`, above `maxth`, or for
  physical overflow.
- `red_rand_max_ppb` is not required by the original RED algorithm above
  `maxth`; above `maxth`, signaling is deterministic.

The checker derives RED's `count` state from the accepted trace order. The CSV
must not be trusted to provide `count`.

## Intentional scope

The checker validates that logged RED decisions are consistent with the logged
RED witnesses. It does not currently recompute the EWMA `avg` from queue
history, because that would require additional implementation-specific trace
fields such as the RED queue weight and idle-time decay inputs.
