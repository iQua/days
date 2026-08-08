#!/usr/bin/env python3
"""Generate the P12 E3 legacy-comparability fixture pair.

E3 is the fixture the four in-house arms share: legacy Days ST and MT (Nexosim) against Days AGO
scalar / CPU / Metal / CUDA. It is re-frozen here, in the repository, from the generator retained
beside the P11 slide preview (`days-gpu/evidence/P11/slide-preview/gen_slide.py`, fixture measured
at `60d13ad`). The slide preview's own header scopes its NUMBERS out of the paper; what it hands
forward is METHOD, and this file is that method under version control.

Three constraints from the slide preview decide every parameter, and all three are recorded there
as structural findings:

1. BYTE-TERMINATED FLOWS ARE MANDATORY. Legacy emits one extra packet per duration-terminated flow,
   so a duration-terminated fixture cannot be compared packet-for-packet across the two engines.
   Every flow here terminates on an exact byte count.

2. THE TRAFFIC MATRIX MUST BE EXPLICIT AND RACK-LOCAL-DOMINANT. The executor rejects non-constant
   arrival distributions, so de-synchronisation has to come from the matrix rather than from an
   RNG; and the shared canonical fat-tree route table funnels all inter-pod traffic through one
   aggregation switch per pod and a single core switch, capping active width at roughly hops x k
   (~192) whatever the rates are. A rack-local-dominant matrix is what produces width on this
   topology family.

3. E3 KEEPS THE DEFAULT SHORTEST-PATH ROUTING. T21 added `[routing] policy = "FatTreeEcmp"`, which
   removes exactly the funnel that finding 2 describes, and E1/E2 use it. E3 must NOT: legacy Days
   has no equal-cost multipath and is frozen against new features (PLAN:605), so an ECMP Days AGO
   row would no longer be running legacy's fabric. The funnel is why the matrix is rack-local, and
   both halves of that pairing have to stay.

The matrix has three permutation components on the k = 32, 16-hosts-per-edge fat tree
(host(o, s) = o * 512 + s, the ordinal-major numbering the builder produces):

  A  intra-rack        host(o, s)     -> host((o+1) mod 16, s)     8,192 flows, 320 pkt/s
  B  intra-pod, cross-rack
                       host(o, s)     -> host(o, s2), s2 the next
                       edge switch in the same pod                 8,192 flows, 20 pkt/s
  C  cross-pod         host(0, s)     -> host(0, (s+16) mod 512)     512 flows, 20 pkt/s

16,896 flows, 1,000-byte packets, ~93.8% of packets rack-local, 5.9% cross-rack intra-pod, 0.4%
cross-pod. Two arms are written: `st` (legacy single-threaded) and `mt` (legacy multi-threaded,
accelerated, hot_workers = 2). Days AGO ignores the threading keys and lowers both to the same
image, which is itself part of the comparison.

Usage (from the repository root, output is byte-identical for identical arguments):

    python3 configs/benchmarks/p12/gen_e3_rack_local.py
"""

import argparse
import os

K = 32
SWITCHES_PER_POD = K // 2  # 16
EDGE_SWITCHES = K * K // 2  # 512
HOSTS_PER_EDGE = K // 2  # 16
PACKET_BYTES = 1000

HEADER = '''# P12 E3 legacy-comparability fixture, %(arm)s arm. GENERATED -- do not hand-edit.
#
# Regenerate with: python3 configs/benchmarks/p12/gen_e3_rack_local.py
# The generator carries the full rationale; the three load-bearing facts are:
#   * every flow is BYTE-terminated (legacy emits one extra packet per duration-terminated flow);
#   * the traffic matrix is explicit and rack-local-dominant (the executor rejects non-constant
#     arrival distributions, and the shared canonical fat-tree route table funnels all inter-pod
#     traffic through one core switch);
#   * routing is left at the DEFAULT shortest path. E1 and E2 use `FatTreeEcmp`; E3 must not,
#     because legacy Days has no equal-cost multipath and is frozen against new features, so an
#     ECMP row here would not be running legacy's fabric.
#
# %(flows)d flows in three permutation components on a k = 32, 16-hosts-per-edge fat tree:
#   A intra-rack             %(count_a)6d flows, %(rate_a)d pkt/s, %(size_a)d B
#   B intra-pod cross-rack   %(count_b)6d flows, %(rate_b)d pkt/s, %(size_b)d B
#   C cross-pod              %(count_c)6d flows, %(rate_c)d pkt/s, %(size_c)d B
seed = 1000
duration = %(stop)g
threading = "%(threading)s"
%(extra)slog_path = "logs/p12/e3_legacy_rack_local_%(arm)s"

[topology]
    category = "FatTree"
[topology.fat_tree]
    k = 32
    hosts_per_edge = 16

[switch]
    port_rate = 3200000
    capacity = 100
    weights = [1]
    discipline = "FIFO"
    drop = "TailDrop"

'''


def interval(rate):
    """Exact decimal seconds per packet. Rates are chosen so the reciprocal terminates."""
    value = 1.0 / rate
    return f"{value:.12f}".rstrip("0")


def host(ordinal, switch):
    return ordinal * EDGE_SWITCHES + switch


def build_flows(send_seconds, rate_a, rate_b, rate_c):
    interval_a, interval_b, interval_c = interval(rate_a), interval(rate_b), interval(rate_c)
    size_a = int(rate_a * send_seconds) * PACKET_BYTES
    size_b = int(rate_b * send_seconds) * PACKET_BYTES
    size_c = int(rate_c * send_seconds) * PACKET_BYTES

    body = []
    counts = {"A": 0, "B": 0, "C": 0}

    def flow(component, source, target, size, step):
        counts[component] += 1
        body.append(
            '[[flow]]\nflow_type = "PacketDistribution"\ngraph = [[%d, %d]]\n'
            'traffic = {initial_delay = 1.0, size = %d, '
            'arr_dist = {type = "Uniform", low = %s, high = %s}, '
            'pkt_size_dist = {type = "Uniform", low = 1000, high = 1000}}\n'
            % (source, target, size, step, step)
        )

    # A: intra-rack permutation, both endpoints on the same edge switch.
    if size_a > 0:
        for switch in range(EDGE_SWITCHES):
            for ordinal in range(HOSTS_PER_EDGE):
                flow(
                    "A",
                    host(ordinal, switch),
                    host((ordinal + 1) % HOSTS_PER_EDGE, switch),
                    size_a,
                    interval_a,
                )
    # B: intra-pod, cross-rack permutation.
    if size_b > 0:
        for switch in range(EDGE_SWITCHES):
            pod, index = switch // SWITCHES_PER_POD, switch % SWITCHES_PER_POD
            partner = pod * SWITCHES_PER_POD + (index + 1) % SWITCHES_PER_POD
            for ordinal in range(HOSTS_PER_EDGE):
                flow("B", host(ordinal, switch), host(ordinal, partner), size_b, interval_b)
    # C: cross-pod, the component that reaches aggregation and core switches.
    if size_c > 0:
        for switch in range(EDGE_SWITCHES):
            partner = (switch + SWITCHES_PER_POD) % EDGE_SWITCHES
            flow("C", host(0, switch), host(0, partner), size_c, interval_c)

    return "".join(body), counts, (size_a, size_b, size_c)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--send-seconds", type=float, default=15.0)
    parser.add_argument("--rate-a", type=int, default=320, help="intra-rack packets per second")
    parser.add_argument("--rate-b", type=int, default=20, help="intra-pod cross-rack packets/s")
    parser.add_argument("--rate-c", type=int, default=20, help="cross-pod packets per second")
    parser.add_argument("--out-dir", default=os.path.dirname(os.path.abspath(__file__)))
    arguments = parser.parse_args()

    flows, counts, sizes = build_flows(
        arguments.send_seconds, arguments.rate_a, arguments.rate_b, arguments.rate_c
    )
    total = counts["A"] + counts["B"] + counts["C"]
    # One second of quiet before the first emission and two after the last, as the slide-preview
    # generator did, so start-up and drain are inside the measured window rather than clipped.
    stop = 1.0 + arguments.send_seconds + 2.0

    for arm, threading, extra in (
        ("st", "single", ""),
        ("mt", "multiple", 'concurrency_level = "accelerated"\nhot_workers = 2\n'),
    ):
        path = os.path.join(arguments.out_dir, "e3_legacy_rack_local_%s.toml" % arm)
        with open(path, "w") as handle:
            handle.write(
                HEADER
                % {
                    "arm": arm,
                    "flows": total,
                    "count_a": counts["A"],
                    "count_b": counts["B"],
                    "count_c": counts["C"],
                    "rate_a": arguments.rate_a,
                    "rate_b": arguments.rate_b,
                    "rate_c": arguments.rate_c,
                    "size_a": sizes[0],
                    "size_b": sizes[1],
                    "size_c": sizes[2],
                    "stop": stop,
                    "threading": threading,
                    "extra": extra,
                }
            )
            handle.write(flows)
        print(path, total, "flows")


if __name__ == "__main__":
    main()
