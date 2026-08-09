#!/usr/bin/env python3
"""Generate the frozen P12 E5 wide-TCP primary and backup fixtures.

Run from any directory:

    python3 configs/benchmarks/p12/gen_e5_wide_tcp.py

The authored fixtures preserve the design probe's single `[[flow_set]]`. This is intentional:
`FatTreeEcmp` hashes semantic flow identity, and round 1 proved that expanding the same endpoints
into explicit `[[flow]]` blocks reroutes 8,158 of 8,192 flows and changes the trajectory.

The protocol endpoint/work target is committed beside the fixtures as
`e5_wide_k32_q200_protocol_golden.csv`. Its stable CSV schema is generated and byte-checked by the
ignored Full-observation test `e5_primary_protocol_golden_vectors_match`; set
`E5_UPDATE_GOLDEN=1` only when intentionally re-authoring it. Every `flow` row contains FlowId,
endpoints, demand/ACKed bytes, start/first-send/completion instants, original/retransmitted data and
ACK packet/byte counts, data/ACK drop packet/byte counts, fast-retransmit/RTO counters, and final
cwnd/ssthresh/RTO. The final `aggregate` row sums meaningful work counters and records the earliest
start/first send and last completion; aggregate final-state fields stay blank.
"""

import argparse
import pathlib
import sys


HERE = pathlib.Path(__file__).resolve().parent

FT_K = 32
HOSTS_PER_EDGE = 16
EDGE_SWITCHES = FT_K * FT_K // 2
HOSTS = EDGE_SWITCHES * HOSTS_PER_EDGE
PORT_RATE_BPS = 100_000_000_000
PROPAGATION_NS = 1_000
FLOW_BYTES = 1_048_576
MSS_BYTES = 1_460
SEED = 51_001
DURATION_S = "3.000000000"

VARIANTS = (
    ("e5_wide_k32_q200.toml", 200, "PRIMARY", 549, 9_551),
    ("e5_wide_k32_q256.toml", 256, "BACKUP", 157, 8_508),
)


def render(name: str, queue_packets: int, role: str, drops: int, data_attempt_excess: int) -> str:
    total_bytes = HOSTS * FLOW_BYTES
    out: list[str] = []
    write = out.append
    write(f"# P12 E5 {role}: frozen wide TCP, k=32, queue {queue_packets}. GENERATED -- do not hand-edit.")
    write("#")
    write("# Regenerate with: python3 configs/benchmarks/p12/gen_e5_wide_tcp.py")
    write("# The generator preserves the design probe's single `[[flow_set]]`; the test suite")
    write("# compares regenerated files byte-for-byte and pins the ruled semantic form.")
    write("#")
    write("# FROZEN CONTRACT (evidence/P12/e5-design.md):")
    write("#   topology              FatTree k=32, hosts_per_edge=16 (8,192 hosts)")
    write("#   fabric                100 Gbit/s, 1,000 ns propagation, FatTreeEcmp")
    write(f"#   queue                 FIFO TailDrop, {queue_packets} packets ({role})")
    write("#   transport             Days AGO Reno, MSS 1,460 B, immediate 40 B ACK")
    write("#   traffic               one fixed 1 MiB flow per host, SwitchOffsetHalf")
    write("#   starts / seed         all 0 ns / 51001")
    write("#   horizon               3 s; acceptance requires natural drain before it")
    write(f"#   total demand          {total_bytes:,} B")
    write(
        f"#   frozen scalar result  {drops:,} drops / "
        f"{data_attempt_excess:,} inferred data-attempt excess"
    )
    write("#")
    write("# FIXTURE-FORM RULING. E5 retains the probe's `[[flow_set]]` semantic identity. Round 1's")
    write("# explicit rewrite rerouted 8,158/8,192 flows and did not complete; E4 remains explicit")
    write("# because its flows have distinct byte sizes and starts. See evidence/P12/e5-authoring.md.")
    write(f"seed = {SEED}")
    write(f"duration = {DURATION_S}")
    write('threading = "single"')
    write(f'log_path = "logs/p12/{pathlib.Path(name).stem}"')
    write("")
    write("[topology]")
    write('category = "FatTree"')
    write("")
    write("[topology.fat_tree]")
    write(f"k = {FT_K}")
    write(f"hosts_per_edge = {HOSTS_PER_EDGE}")
    write("")
    write("[switch]")
    write(f"port_rate = {PORT_RATE_BPS}")
    write(f"capacity = {queue_packets}")
    write("weights = [1]")
    write('discipline = "FIFO"')
    write('drop = "TailDrop"')
    write("")
    write("[link]")
    write(f"propagation_ns = {PROPAGATION_NS}")
    write("")
    write("[routing]")
    write('policy = "FatTreeEcmp"')
    write("")

    write("[[flow_set]]")
    write('flow_type = "TCP"')
    write(f"flow_count = {HOSTS}")
    write('pairing = "SwitchOffsetHalf"')
    write("")
    write("[flow_set.traffic]")
    write("initial_delay = 0.000000000")
    write(f"size = {FLOW_BYTES}")
    write('arr_dist = {type = "Uniform", low = 1.0, high = 1.0}')
    write(
        f'pkt_size_dist = {{type = "DiscreteUniform", low = {MSS_BYTES}, high = {MSS_BYTES}}}'
    )
    write("")
    write("[flow_set.traffic.tcp]")
    write('cc_algorithm = "TCPReno"')
    return "\n".join(out) + "\n"


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--out-dir",
        type=pathlib.Path,
        default=HERE,
        help="directory for both generated TOML files (default: beside this script)",
    )
    arguments = parser.parse_args(argv)
    arguments.out_dir.mkdir(parents=True, exist_ok=True)

    for name, queue_packets, role, drops, data_attempt_excess in VARIANTS:
        output = arguments.out_dir / name
        output.write_text(render(name, queue_packets, role, drops, data_attempt_excess))
        print(f"wrote {output} (one flow set, {HOSTS:,} flows, {HOSTS * FLOW_BYTES:,} B)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
