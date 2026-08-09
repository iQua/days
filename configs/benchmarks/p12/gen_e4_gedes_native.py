#!/usr/bin/env python3
"""Generate the P12 E4 fixture: GeDES's OWN native k=32 workload, expressed for Days.

    python3 configs/benchmarks/p12/gen_e4_gedes_native.py

Writes `configs/benchmarks/p12/e4_gedes_native_k32.toml`. A test
(`tests/t21_p12_e4.rs::the_generator_reproduces_the_committed_fixture`) regenerates and compares
byte-for-byte, so this script and the fixture cannot drift apart.

===============================================================================================
WHY THIS EXISTS -- E4 IS THE FAIR ROW
===============================================================================================

E1, E1-LONG and E2 all put a workload WE authored onto a fabric sized like GeDES's. E4 inverts the
burden: it runs GeDES's OWN shipped workload -- the flow-size distribution, the arrival process and
the pairing that `script.py`'s k=32 sweep row produces -- and makes Days express it. Every arm in
the P12 roster then runs the same 8,192 flows with the same per-flow byte counts and the same
per-flow start instants, to completion, and the metric is time-to-completion.

===============================================================================================
THE SOURCE OF TRUTH
===============================================================================================

GeDES's `script.py:99-105` canonical sweep, k=32 row:

    params = [
        ...
        (32, 10000000, "data/flow_statics_32.csv", 10000, 10000000),
        ...
    ]   # (ft_k, packet_pool_size, output, average_flow_size, flow_time_range)

i.e. `--ft_k=32 --packet_pool_size=10000000 --average_flow_size=10000 --flow_time_range=10000000`.
That row is what GeDES publishes as its k=32 point, and it is the workload E4 expresses.

`--average_flow_size` and `--flow_time_range` are NOT read directly here. They are read through
GeDES's own emitted flow table, because reproducing the draw offline would mean reproducing
`std::mt19937` plus libstdc++'s `std::uniform_real_distribution` bit-for-bit -- a portability
liability with no upside when GeDES already writes the answer to a CSV:

    ~/gedes-bench/runs/k32pub_seed41_a.csv   (boston, GeDES tree at 1376c638a0ae661164...)
    md5 f9eec61c11a29a998d2c849f120eb396

That md5 is the SAME one P09 sec 7.3 recorded on madrid -- GB10 / sm_121 / CUDA 13.0 / aarch64 --
for the same seeded configuration, so the table is architecture-invariant, not a boston artifact.
It is committed beside this script as `gedes_k32_seed41_flows.csv`, verbatim and unmodified, and a
test asserts that md5.

How GeDES produced it (`GeDES_code/code/c++/src/components/topology.cc:1504-1690`, read at the same
pin):

    auto poison_inter = [](int64_t avg) {                      // inverse-CDF exponential
        static std::mt19937 gen(flow_rng_seed >= 0 ? flow_rng_seed : std::random_device{}());
        static std::uniform_real_distribution<> dis(0.0, 1.0);
        return -std::log(1.0 - dis(gen)) * avg;
    };
    for (i in 0..node_num)                                     // per-node byte demand, in packets
        traffic[i] = min(poison_inter(average_flow_size) + 1, 2 * average_flow_size);
    for (i in 0..node_num) if (node i unpaired) {
        int j = i + node_num / 2;                              // the random pick is OVERRIDDEN
        flow1.flow_size = 1460 * traffic[i];  flow1.timestamp = poison_inter(flow_time_range);
        flow2.flow_size = 1460 * traffic[j];  flow2.timestamp = poison_inter(flow_time_range);
    }

So: exponential flow sizes truncated at twice the mean, in 1,460 B units; exponential start times
with a 10 ms mean; one flow per host in each direction of a fixed i <-> i + N/2 pairing. `--rng_seed`
is PATCH-07 from the P10b bring-up (the artifact as shipped seeds from `std::random_device` and is
irreproducible run to run -- P09 sec 7.4); seed 41 is the campaign's standing seed and is also the
literal `seed = 41U` `main.cc` hardcodes elsewhere.

Measured over the committed CSV, and asserted by the tests:

    flows                       8,192
    sum of flow_size      104,063,174,620 B          <- E4's byte gate
    smallest flow               1,460 B  (1 segment)
    largest flow           29,200,000 B  (20,000 segments = the 2 x average_flow_size cap)
    latest start           95,787,147 ns             <- 95.787147 ms
    mean offered load       ~1.06% of 100 Gbit/s per host, averaged over the arrival window

===============================================================================================
THE MAPPING, PARAMETER BY PARAMETER
===============================================================================================

HOST IDENTITY. GeDES assigns host `n` the address `0xC0A80000 + (n / 16) * 48 + (n % 16)`
(`topology.cc:169`, `ip_group_size = 48`, 16 hosts per edge switch), i.e. it numbers hosts
SWITCH-MAJOR: `n = switch * 16 + ordinal`. Days numbers hosts ORDINAL-MAJOR:
`host = ordinal * switch_count + switch` (`src/topos/build.rs`, `create_uniform_host_list`). The
map is therefore

    days_host(n) = (n % 16) * 512 + (n / 16)

and it is a bijection on 0..8192. Copying GeDES's index arithmetic instead of restating it on the
(switch, ordinal) grid would turn its cross-pod matrix into a rack-local one -- the same trap
T21 sec 2.3 recorded for E1.

PAIRING. GeDES pairs `n <-> n + N/2`; verified to hold for all 8,192 rows of the committed CSV
(0 mismatches). On the grid that is `switch s -> switch (s + 256) mod 512` at the same rack
ordinal, i.e. pod `p -> p + 16 mod 32`: EXACTLY Days' `SwitchOffsetHalf` policy. E4 cannot use
`[[flow_set]]` with `pairing = "SwitchOffsetHalf"` because every flow needs its own byte size and
its own start instant, so it emits 8,192 explicit `[[flow]]` blocks -- and a test asserts the
resulting pair list equals `structural_flow_pairs(SwitchOffsetHalf, 8192)` on the same topology.

FLOW SIZE. GeDES's `flow_size` is the payload byte count a sender must deliver; Days' TCP
`size` is byte-termination in payload bytes. Copied exactly, per flow, with no rounding.

ARRIVAL. GeDES's `start_timestamp` is in nanoseconds; Days' `initial_delay` is exact decimal
seconds scaled by 1e9 in lowering, so `t` ns is written `t/1e9` with nine fraction digits and
lowers to `t` ns with no floating point anywhere on the path.

SEGMENT. 1,460 B, GeDES's `mss` (`component.cc:173-227`) and the payload E1/E2 already match.

FABRIC. k = 32 with 16 hosts per edge switch (8,192 hosts, 1,280 switches); 100 Gbit/s ports
(`tx_rate = 100` bits/ns, `main.cc:55-71`); 1,000 ns propagation per link
(`popogation_delay = 1000`).

QUEUES. `capacity = 1024`, FIFO tail-drop -- E1 and E2's depth, NOT GeDES's
`Switch_DEFAULT_EGRESS_QUEUE_SIZE = 200` (`include/conf.h`, `switch.cu:216-223`). E4's subject is
GeDES's WORKLOAD -- flow sizes, pairings, arrivals, on a k=32 fat tree -- and the queue depth is a
fabric parameter E1/E2 already fix and disclose, so E4 keeps the family's value and stays
differenceable against them.

  THE 200-PACKET VARIANT WAS AUTHORED, RUN AND REJECTED, AND THAT IS A RESULT. At `capacity = 200`
  this exact workload does NOT complete on Days. Measured (full scalar runs at 150 ms and at 4 s):
  776 tail drops out of 142,738,118 sourced packets -- 0.00054 %, concentrated in 7 flows -- and
  those flows then make ~39 kB of progress per RETRANSMISSION TIMEOUT, so 8,185/8,192 complete by
  150 ms, 8,189/8,192 by 4 s, and the remaining flows owe ~400 more seconds of simulated tail. Two
  mechanisms compose: Days' RTO floor is 1 s (`executor/src/tcp.rs` MIN_RTO_NS, RFC 6298) against a
  MEASURED 25-36 us smoothed RTT on this fabric, and after a timeout `cwnd` collapses to one MSS
  while `bytes_in_flight` is left standing (`scalar.rs:4338`: `allowance = cwnd - bytes_in_flight`),
  so a flow moves exactly one segment per second until the receiver's cumulative ACK catches up.
  Keeping 200 would therefore have bought GeDES-fidelity on one fabric constant at the price of a
  fixture that does not terminate -- and of a second, uncontrolled difference from E1/E2.
  The variant is recorded in `evidence/P12/e4-authoring.md` and is owed as a separate capability
  row, not smuggled into the completion-time row.

ORDERING. Flows are emitted in ascending Days source host id, not in GeDES's CSV order. The two
are related by the switch-major/ordinal-major permutation above; the SET of flows, every byte
count and every start instant are unchanged. Ascending source host id is the order Days' own
structural flow sets enumerate in, so E4's flow ids line up with `SwitchOffsetHalf`'s.

===============================================================================================
WHAT DOES NOT MAP -- STATED, NOT WORKED AROUND
===============================================================================================

* TRANSPORT. GeDES is DCTCP (`ENABLE_DCTCP = 1`; the Reno branch is compiled out), ACK-clocked,
  Go-Back-N, null payloads, `packets_num_per_ack = 5`, `rto = 1 ms`. Days runs TCP Reno with a
  per-flow segment ledger and real byte accounting. E4 does not pretend these produce the same
  packet trajectory; it pretends only that they are asked to deliver the same bytes between the
  same endpoints starting at the same instants. Retransmission counts are published per arm so a
  reader can see how much extra work each transport did.
* ECMP. GeDES sprays PER PACKET with a deterministic round-robin counter (`switch.cu:84,111`).
  Days' `FatTreeEcmp` selects ONE path per flow from the flow's own semantic hash -- the
  conventional 5-tuple model. Both are equal-cost multipath over the same fat tree; neither is the
  other. Days' single-path table is not an option here: under an offset permutation it collapses
  every cross-pod route in a pod onto one aggregation switch and one core (see
  `src/topos/route.rs`), which is not the fabric any other arm runs.
* EMISSION CAP. GeDES admits at most 38 TCP packets per node per timeslot
  (`MAX_TRANSMITTED_PACKET_NUM 18` + `MAX_GENERATED_PACKET_NUM 20`, `tcp_controller.cu:639`) --
  a GPU-batching artifact with no counterpart in an event-ordered engine. Not expressed.
* QUEUE STRUCTURE. GeDES holds separate ingress (100) and egress (200) queues per port; Days has
  one queue per egress port. Only the egress depth is matched.
"""

import argparse
import hashlib
import pathlib
import sys

HERE = pathlib.Path(__file__).resolve().parent
FLOW_TABLE = HERE / "gedes_k32_seed41_flows.csv"
OUTPUT = HERE / "e4_gedes_native_k32.toml"

# The committed GeDES flow table, exactly as GeDES wrote it.
FLOW_TABLE_MD5 = "f9eec61c11a29a998d2c849f120eb396"

# GeDES fabric constants (main.cc:55-71, include/conf.h, topology.cc).
GEDES_IP_BASE = 0xC0A80000
GEDES_IP_GROUP_SIZE = 48
FT_K = 32
HOSTS_PER_EDGE = 16
EDGE_SWITCHES = FT_K * FT_K // 2  # 512
HOSTS = EDGE_SWITCHES * HOSTS_PER_EDGE  # 8192
SEGMENT_BYTES = 1460
PORT_RATE_BPS = 100_000_000_000
PROPAGATION_NS = 1000
SWITCH_CAPACITY_PACKETS = 1024

# Horizon. NOT a measurement window: E4 is run-to-completion and the horizon is a non-binding
# upper bound whose non-bindingness is a gate (every flow completed, fabric drained, strictly
# inside it).
#
# WHAT THE HORIZON IS SIZED AGAINST, and it is not the workload. The workload's own arithmetic is
# small: the last flow ARRIVES at 95.787147 ms and the largest flow (29,200,000 B) needs 2.336 ms
# of serialisation at 100 Gbit/s, so 98.123147 ms bounds an uncongested drain -- and the committed
# fixture MEASURES a drain at 96.054393 ms, drop-free, 8,192/8,192 complete, 78,999 rounds.
#
# The horizon is nonetheless four seconds, because the cost of ONE lost segment is a full second.
# Days' RTO floor is 1 s (`executor/src/tcp.rs` MIN_RTO_NS, RFC 6298) against a MEASURED smoothed
# RTT of 25-36 us on this fabric -- a floor roughly 30,000x the actual round trip -- and after a
# timeout `cwnd` collapses to one MSS while `bytes_in_flight` stands (`scalar.rs:4338`:
# `allowance = cwnd - bytes_in_flight`), so a flow that loses a burst moves about one segment per
# second. A horizon sized to the drain would therefore turn a single drop into a SILENT truncation
# instead of a loud one. Four seconds admits a first timeout plus one x2 backoff and still refuses
# a third, so a pathology is reported rather than absorbed. The slack is measured to be free: the
# 4 s horizon costs 78,999 rounds for a run that drains at 96 ms, because an empty fabric costs
# zero executor rounds.
#
# THIS IS ALSO A CROSS-ARM DISCLOSURE. GeDES's own `rto = 1000000` ns is ONE MILLISECOND
# (`component.cc:173-227`), three orders of magnitude below the floor Days and ns-3 both use. Any
# E4 time-to-completion compared across arms is RTO-floor-dominated the moment a single segment is
# lost, so the completion instant must always be published beside the drop count. On the committed
# fabric Days drops nothing, which is what makes its 96.054393 ms drain a workload result rather
# than a timer result.
DURATION_S = "4.000000000"

# Family seed. E4 draws no random endpoints (every flow is explicit), so this only keeps the file
# in the shape of its E1/E2 siblings.
SEED = 21032


def gedes_host_index(ip: int) -> int:
    """GeDES address -> GeDES host index, inverting topology.cc:169's assignment."""
    offset = ip - GEDES_IP_BASE
    group, ordinal = divmod(offset, GEDES_IP_GROUP_SIZE)
    if not 0 <= ordinal < HOSTS_PER_EDGE:
        raise ValueError(f"address {ip} has rack ordinal {ordinal}, outside 0..{HOSTS_PER_EDGE}")
    if not 0 <= group < EDGE_SWITCHES:
        raise ValueError(f"address {ip} has edge switch {group}, outside 0..{EDGE_SWITCHES}")
    return group * HOSTS_PER_EDGE + ordinal


def days_host_id(gedes_index: int) -> int:
    """GeDES switch-major host index -> Days ordinal-major host id."""
    switch, ordinal = divmod(gedes_index, HOSTS_PER_EDGE)
    return ordinal * EDGE_SWITCHES + switch


def exact_seconds(nanoseconds: int) -> str:
    """Nanoseconds -> the exact decimal-seconds literal Days' lowering scales by 1e9."""
    whole, fraction = divmod(nanoseconds, 1_000_000_000)
    return f"{whole}.{fraction:09d}"


def read_flow_table():
    digest = hashlib.md5(FLOW_TABLE.read_bytes()).hexdigest()
    if digest != FLOW_TABLE_MD5:
        raise SystemExit(
            f"{FLOW_TABLE.name} md5 {digest} != the GeDES-emitted {FLOW_TABLE_MD5}; refusing to "
            "generate E4 from a table that is not GeDES's own output"
        )

    lines = FLOW_TABLE.read_text().splitlines()
    header = lines[0].split(",")
    if header[:4] != ["source_ip", "dst_ip", "flow_size", "start_timestamp"]:
        raise SystemExit(f"unexpected GeDES CSV header: {lines[0]}")

    flows = []
    for row, line in enumerate(lines[1:]):
        fields = line.split(",")
        source_ip, dst_ip, flow_size, start_ns = (int(field) for field in fields[:4])
        source = gedes_host_index(source_ip)
        target = gedes_host_index(dst_ip)
        # GeDES emits one row per source host, in host-index order; anything else means the
        # table is not the one this mapping was verified against.
        if source != row:
            raise SystemExit(f"row {row}: source host {source} breaks GeDES's emission order")
        if target != (source + HOSTS // 2) % HOSTS:
            raise SystemExit(
                f"row {row}: {source} -> {target} is not GeDES's i <-> i + N/2 pairing"
            )
        if flow_size <= 0 or flow_size % SEGMENT_BYTES != 0:
            raise SystemExit(f"row {row}: flow_size {flow_size} is not a positive segment multiple")
        if start_ns < 0:
            raise SystemExit(f"row {row}: negative start {start_ns}")
        flows.append((days_host_id(source), days_host_id(target), flow_size, start_ns))

    if len(flows) != HOSTS:
        raise SystemExit(f"expected {HOSTS} flows, got {len(flows)}")
    flows.sort()
    if [flow[0] for flow in flows] != list(range(HOSTS)):
        raise SystemExit("sources are not a permutation of the host set")
    return flows


def render(flows) -> str:
    total_bytes = sum(flow[2] for flow in flows)
    latest_start = max(flow[3] for flow in flows)
    smallest = min(flow[2] for flow in flows)
    largest = max(flow[2] for flow in flows)

    out = []
    w = out.append
    w("# P12 E4: GeDES's OWN native k=32 workload, run to completion. GENERATED -- do not hand-edit.")
    w("#")
    w("# Regenerate with: python3 configs/benchmarks/p12/gen_e4_gedes_native.py")
    w("# The generator carries the full parameter-by-parameter mapping and the list of GeDES")
    w("# behaviours that deliberately do NOT map. The load-bearing facts are:")
    w("#")
    w("#   * THE WORKLOAD IS GeDES'S, NOT OURS. Every flow's byte count and start instant is read")
    w("#     verbatim from the flow table GeDES emitted for its own published k=32 sweep row")
    w("#     (`script.py:102`: --ft_k=32 --packet_pool_size=10000000 --average_flow_size=10000")
    w("#     --flow_time_range=10000000, --rng_seed=41), committed beside this file as")
    w("#     `gedes_k32_seed41_flows.csv`, md5 f9eec61c11a29a998d2c849f120eb396 -- the same md5")
    w("#     P09 sec 7.3 recorded on madrid (GB10/sm_121/aarch64), so the table is")
    w("#     architecture-invariant. The adaptation burden is on Days: this is THE FAIR ROW.")
    w("#")
    w("#   * RUN TO COMPLETION, NOT TO A HORIZON. Every flow is byte-terminated at its own")
    w("#     GeDES-defined size and the metric is time-to-completion. Days has no stop-when-idle")
    w("#     control, so `duration` is a NON-BINDING upper bound: the gates assert that all 8,192")
    w("#     flows complete and the fabric drains strictly inside it. An empty fabric costs zero")
    w("#     executor rounds (the F-BURST tail measured exactly that), so the slack is free.")
    w("#")
    w("#   * THE HORIZON IS 40x THE DRAIN, ON PURPOSE. One lost segment costs a full second here:")
    w("#     Days' RTO floor is 1 s (`executor/src/tcp.rs` MIN_RTO_NS, RFC 6298) against a MEASURED")
    w("#     25-36 us RTT, and after a timeout `cwnd` collapses to one MSS without releasing the")
    w("#     in-flight bytes, so a flow that loses a burst advances ~one segment per second. A")
    w("#     horizon sized to the 96 ms drain would turn a single drop into a SILENT truncation.")
    w("#     4 s admits one timeout plus one x2 backoff and refuses a third. It is free: 78,999")
    w("#     rounds for a 4 s horizon that drains at 96 ms, because an empty fabric costs 0 rounds.")
    w("#     CROSS-ARM: GeDES's own rto is 1 ms, three orders of magnitude below Days' and ns-3's")
    w("#     floor, so any E4 completion time is RTO-floor-dominated the moment a segment is lost")
    w("#     and must be published beside the drop count. Days drops NOTHING here, which is what")
    w("#     makes its 96.054393 ms drain a workload result and not a timer result.")
    w("#")
    w("#   * COUNT GATES, ALL MEASURED GREEN on scalar. flows 8192/8192 completed; cumulative ACK")
    w(f"#     exactly {total_bytes:,} B = the sum of GeDES's own flow_size column;")
    w("#     0 packets resident, 0 events pending, 0 retransmission timers armed at the end;")
    w("#     0 dropped packets and 0 retransmitted bytes; DRAIN at 96,054,393 ns over 78,999")
    w("#     rounds, against the 4,000,000,000 ns horizon -- a margin of 3,903,945,607 ns.")
    w("#")
    w("# WORKLOAD SHAPE (measured over the committed table, asserted in tests/t21_p12_e4.rs):")
    w(f"#   flows                 {len(flows):>18,}")
    w(f"#   total payload         {total_bytes:>18,} B   ({total_bytes / SEGMENT_BYTES:,.0f} segments)")
    w(f"#   smallest / largest    {smallest:>18,} B / {largest:,} B  (1 and 20,000 segments)")
    w(f"#   latest arrival        {latest_start:>18,} ns  ({latest_start / 1e6:.6f} ms)")
    w("#   mean offered load     ~1.06% of each host's 100 Gbit/s over the arrival window --")
    w("#                         this is a LIGHTLY loaded fabric with a heavy tail, which is what")
    w("#                         GeDES's own configuration produces. It is not E1's or E2's")
    w("#                         saturated regime and must not be described as one.")
    w("#")
    w("# FABRIC. Taken from GeDES where GeDES fixes it and Days can express it exactly: k = 32 with")
    w("# 16 hosts per edge switch (8,192 hosts), 100 Gbit/s ports, 1,000 ns propagation, 1,460 B")
    w("# segments, FIFO tail-drop. The one fabric constant NOT taken from GeDES is the queue depth:")
    w("# E4 uses E1/E2's 1,024 packets, not GeDES's Switch_DEFAULT_EGRESS_QUEUE_SIZE = 200, so that")
    w("# E4 stays differenceable against its own family. The 200-packet variant was authored, run")
    w("# and REJECTED, and that rejection is a measured result: at 200 this workload does not")
    w("# complete on Days (776 tail drops in 142,738,118 packets, concentrated in 7 flows, each")
    w("# then advancing ~39 kB per 1-second retransmission timeout -- 8,185/8,192 done at 150 ms,")
    w("# 8,189/8,192 at 4 s, ~400 s of tail owed). See the generator docstring and")
    w("# evidence/P12/e4-authoring.md for the two mechanisms that compose to produce it.")
    w("#")
    w("# TRAJECTORY NON-IDENTITY, DISCLOSED. GeDES runs DCTCP (ENABLE_DCTCP = 1; its Reno branch is")
    w("# compiled out), Go-Back-N, null payloads, ACK-per-5-packets, and per-packet round-robin")
    w("# ECMP. Days runs TCP Reno with a real byte ledger and per-flow hashed ECMP. The two engines")
    w("# are asked to deliver the SAME bytes between the SAME endpoints from the SAME instants;")
    w("# they are not asked to produce the same packet trajectory, and they will not. Every E4 arm")
    w("# publishes its retransmission count so the extra work each transport did is visible.")
    w("#")
    w("# ROUTING. `FatTreeEcmp`, stated: one path per flow chosen from the flow's own semantic hash")
    w("# (aggregation group from the low half, core offset from the high half). Days' default")
    w("# single-path table is not usable on an offset permutation -- it collapses every cross-pod")
    w("# route out of a pod onto one aggregation switch and one core.")
    w(f"seed = {SEED}")
    w(f"duration = {DURATION_S}")
    w('threading = "single"')
    w('log_path = "logs/p12/e4_gedes_native_k32"')
    w("")
    w("[topology]")
    w('category = "FatTree"')
    w("")
    w("[topology.fat_tree]")
    w(f"k = {FT_K}")
    w(f"hosts_per_edge = {HOSTS_PER_EDGE}")
    w("")
    w("[switch]")
    w(f"port_rate = {PORT_RATE_BPS}")
    w(f"capacity = {SWITCH_CAPACITY_PACKETS}")
    w("weights = [1]")
    w('discipline = "FIFO"')
    w('drop = "TailDrop"')
    w("")
    w("[link]")
    w(f"propagation_ns = {PROPAGATION_NS}")
    w("")
    w("[routing]")
    w('policy = "FatTreeEcmp"')
    w("")
    for source, target, size, start_ns in flows:
        w("[[flow]]")
        w('flow_type = "TCP"')
        w(f"graph = [[{source}, {target}]]")
        w(
            "traffic = {"
            f"initial_delay = {exact_seconds(start_ns)}, size = {size}, "
            'arr_dist = {type = "Uniform", low = 1.0, high = 1.0}, '
            'pkt_size_dist = {type = "DiscreteUniform", low = 1460, high = 1460}, '
            'tcp = {cc_algorithm = "TCPReno"}'
            "}"
        )
    return "\n".join(out) + "\n"


def main(argv) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--out-dir",
        type=pathlib.Path,
        default=HERE,
        help="directory to write e4_gedes_native_k32.toml into (default: beside this script)",
    )
    arguments = parser.parse_args(argv)
    flows = read_flow_table()
    output = arguments.out_dir / OUTPUT.name
    output.write_text(render(flows))
    print(f"wrote {output} ({len(flows)} flows, {sum(f[2] for f in flows):,} B)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
