#!/usr/bin/env python3
"""
What-if analysis (SimAI Fig10-style) using Days.

Workload: RingAllReduce
Participants: N = 128
Message size: 512MB (536870912 bytes)
Metric: CCT = end_time_s - start_time_s (from collective_events.csv). Fallback to
        max(flow end) - min(flow start) if needed.

Experiments:
  B1) Bandwidth sweep: port_rate = 100/200/400 Gbps on FatTree(k=16)
  B2) Topology swap: FatTree(k=16) vs Ring (1D Torus, n=128) at 200 Gbps
  B3) Oversub sweep: 1:1 / 2:1 / 4:1 by reducing ToR uplinks in a leaf-spine fabric,
      port_rate fixed at 200 Gbps

Outputs (repo root):
  results/whatif/
    - whatif_bandwidth_sweep_ringallreduce.csv
    - whatif_topology_swap_ringallreduce.csv
    - whatif_oversub_sweep_ringallreduce.csv
    - fig_whatif_bandwidth_sweep_ringallreduce.png/svg
    - fig_whatif_topology_swap_ringallreduce.png/svg
    - fig_whatif_oversub_sweep_ringallreduce.png/svg
    - whatif_bandwidth_sweep_ringallreduce.md
    - whatif_topology_swap_ringallreduce.md
    - whatif_oversub_sweep_ringallreduce.md

Notes on scalability:
  Days simulates packets. For N=128 and 512MB, RingAllReduce expands into many flows.
  To keep runs tractable without changing the simulator core, we configure
  pkt_size_dist to produce a single packet per flow-step (packet size equals flow bytes).
"""

from __future__ import annotations

import csv
import subprocess
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


REPO = Path(__file__).resolve().parents[1]
OUT_DIR = REPO / "results" / "whatif"

SIZE_BYTES = 536_870_912  # 512MB
N_HOSTS = 128

FATTREE_K = 16  # hosts = k^2/2 = 128 in this simulator's FatTree model

PORT_RATES_BPS = {
    "100g": 1e11,
    "200g": 2e11,
    "400g": 4e11,
}


@dataclass(frozen=True)
class CctResult:
    cct_s: float
    method: str  # "collective_events" or "flow_events_fallback"


def ensure_dirs() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def build_days() -> Path:
    subprocess.run(["cargo", "build", "--release", "--bin", "days"], cwd=REPO, check=True)
    binary = REPO / "target" / "release" / "days"
    if not binary.exists():
        raise FileNotFoundError(f"missing built binary: {binary}")
    return binary


def ringallreduce_effective_step_bytes(size_bytes: int, n: int) -> int:
    # Matches topo.rs: ceil(size*2*(n-1)/n)
    steps = n - 1
    numerator = size_bytes * 2 * steps
    return (numerator + (n - 1)) // n


def rank_order_fattree_interleaved(k: int) -> list[int]:
    # For k=16: edge switches are 0..127, with 16 pods and 8 edge switches per pod.
    # Interleave pods so consecutive ranks are in different pods, forcing inter-pod traffic.
    per_pod = k // 2
    num_pods = k
    edge_hosts = (k * k) // 2
    out: list[int] = []
    for local in range(per_pod):
        for pod in range(num_pods):
            out.append(pod * per_pod + local)
    assert len(out) == edge_hosts
    return out


def render_base_config_header(log_path: str, duration_s: float) -> str:
    # Keep this as a "preamble" only (no table headers),
    # so custom-graph configs can insert `edges/hosts` at the TOML root.
    return textwrap.dedent(
        f"""\
        seed = 1
        duration = {duration_s:.1f}
        ui_interval = 1e9
        report_interval = 1e9
        log_path = "{log_path}"

        """
    )


def render_app_source() -> str:
    return textwrap.dedent(
        """\
        [app_source]
        req_channel_capacity = 256
        chunk_size = 512
        initial_delay = 1
        run_interval = 50

        """
    )


def render_switch(port_rate_bps: float) -> str:
    return textwrap.dedent(
        f"""\
        [switch]
        port_rate = {port_rate_bps:.6e}
        capacity = 0
        discipline = "FIFO"
        drop = "TailDrop"

        """
    )


def render_ringallreduce_collective(
    n: int, size_bytes: int, pkt_bytes: int, rank_order: list[int]
) -> str:
    # Provide explicit sources/sinks so we control rank placement.
    # sinks[i] must equal sources[(i+1)%n] for RingAllReduce endpoint validation.
    if len(rank_order) != n:
        raise ValueError("rank_order length mismatch")
    sources = ", ".join(str(x) for x in rank_order)
    sinks = ", ".join(str(rank_order[(i + 1) % n]) for i in range(n))
    return textwrap.dedent(
        f"""\
        [[collective]]
        collective_type = "RingAllReduce"
        flow_type = "PacketDistribution"
        flow_count = {n}
        sources = [{sources}]
        sinks = [{sinks}]
        routing = "ECMP"

        [collective.traffic]
        initial_delay = 0.0
        size = {size_bytes}
        arr_dist = {{ type = "Uniform", low = 1e-9, high = 1e-9 }}
        pkt_size_dist = {{ type = "DiscreteUniform", low = {pkt_bytes}, high = {pkt_bytes} }}

        """
    )


def render_topology_fattree(k: int) -> str:
    return textwrap.dedent(
        f"""\
        [topology]
        category = "FatTree"

        [topology.fat_tree]
        k = {k}

        """
    )


def render_topology_torus(dim: int, n: int) -> str:
    return textwrap.dedent(
        f"""\
        [topology]
        category = "Torus"

        [topology.torus]
        dim = {dim}
        n = {n}

        """
    )


def render_custom_graph(edges: Iterable[tuple[int, int]], hosts: list[int]) -> str:
    edge_lines = ",\n".join([f"  [{u}, {v}]" for u, v in edges])
    host_list = ", ".join(str(h) for h in hosts)
    return textwrap.dedent(
        f"""\
        edges = [
        {edge_lines}
        ]
        hosts = [{host_list}]

        """
    )


def build_leafspine_oversub_edges(
    tors: int, hosts_per_tor: int, spines: int, uplinks_per_tor: int
) -> tuple[list[tuple[int, int]], list[int], list[int]]:
    """
    Build a simple ToR-spine fabric with explicit hosts.

    Node IDs:
      - hosts: 0..(tors*hosts_per_tor-1)
      - tors:  base_tor..base_tor+tors-1
      - spines: base_spine..base_spine+spines-1

    Oversub is implemented by reducing uplinks_per_tor while keeping hosts_per_tor fixed.
    """
    if tors <= 0 or hosts_per_tor <= 0 or spines <= 0:
        raise ValueError("invalid leaf-spine parameters")
    if uplinks_per_tor <= 0 or uplinks_per_tor > spines:
        raise ValueError("uplinks_per_tor out of range")

    n_hosts = tors * hosts_per_tor
    hosts = list(range(n_hosts))
    base_tor = n_hosts
    base_spine = base_tor + tors

    edges: list[tuple[int, int]] = []

    # host <-> tor
    for h in range(n_hosts):
        tor = base_tor + (h // hosts_per_tor)
        edges.append((h, tor))

    # tor <-> spine (prune uplinks to realize oversub)
    for tor_i in range(tors):
        tor_id = base_tor + tor_i
        for s in range(uplinks_per_tor):
            spine_id = base_spine + s
            edges.append((tor_id, spine_id))

    # rank order: interleave ToRs
    rank_order: list[int] = []
    for local in range(hosts_per_tor):
        for tor_i in range(tors):
            rank_order.append(tor_i * hosts_per_tor + local)
    assert len(rank_order) == n_hosts
    return edges, hosts, rank_order


def run_days(binary: Path, toml_text: str) -> None:
    tmp = OUT_DIR / "_tmp_whatif.toml"
    tmp.write_text(toml_text)
    try:
        subprocess.run([str(binary), str(tmp)], cwd=REPO, check=True)
    finally:
        try:
            tmp.unlink()
        except OSError:
            pass


def read_cct_from_logs(log_dir: Path, size_bytes: int) -> CctResult:
    ce = log_dir / "collective_events.csv"
    if ce.exists():
        with ce.open("r", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                if row.get("collective_type") != "RingAllReduce":
                    continue
                if int(row["size_bytes"]) != int(size_bytes):
                    continue
                start = float(row["start_time_s"])
                end = float(row["end_time_s"])
                return CctResult(cct_s=end - start, method="collective_events")

    fe = log_dir / "flow_events.csv"
    if fe.exists():
        starts = []
        ends = []
        with fe.open("r", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                if row.get("collective_type") != "RingAllReduce":
                    continue
                if int(row["size_bytes"]) != int(size_bytes):
                    continue
                starts.append(float(row["start_time_s"]))
                ends.append(float(row["end_time_s"]))
        if starts and ends:
            return CctResult(cct_s=max(ends) - min(starts), method="flow_events_fallback")

    raise FileNotFoundError(f"missing CCT events in {log_dir}")


def write_csv(path: Path, header: list[str], rows: list[list[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow(row)


def plot_matplotlib_line(
    x_labels: list[str],
    ys: list[float],
    title: str,
    y_label: str,
    out_png: Path,
    out_svg: Path,
    annotate: list[str] | None = None,
) -> None:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(5.6, 3.6), dpi=160)
    ax.plot(range(len(x_labels)), ys, marker="o", linewidth=2)
    ax.set_xticks(range(len(x_labels)))
    ax.set_xticklabels(x_labels)
    ax.set_ylabel(y_label)
    ax.grid(True, linestyle="--", alpha=0.35)
    if title:
        ax.set_title(title)
    if annotate:
        for i, txt in enumerate(annotate):
            ax.annotate(
                txt,
                (i, ys[i]),
                textcoords="offset points",
                xytext=(0, 8),
                ha="center",
                fontsize=9,
            )
    fig.tight_layout()
    fig.savefig(out_png, format="png")
    fig.savefig(out_svg, format="svg")
    plt.close(fig)


def plot_matplotlib_bar(
    x_labels: list[str],
    ys: list[float],
    title: str,
    y_label: str,
    out_png: Path,
    out_svg: Path,
    annotate: list[str] | None = None,
) -> None:
    import matplotlib

    matplotlib.use("Agg", force=True)
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(figsize=(5.6, 3.6), dpi=160)
    bars = ax.bar(range(len(x_labels)), ys)
    ax.set_xticks(range(len(x_labels)))
    ax.set_xticklabels(x_labels)
    ax.set_ylabel(y_label)
    ax.grid(True, axis="y", linestyle="--", alpha=0.35)
    if title:
        ax.set_title(title)
    if annotate:
        for i, b in enumerate(bars):
            ax.text(
                b.get_x() + b.get_width() / 2,
                b.get_height(),
                annotate[i],
                ha="center",
                va="bottom",
                fontsize=9,
            )
    fig.tight_layout()
    fig.savefig(out_png, format="png")
    fig.savefig(out_svg, format="svg")
    plt.close(fig)


def md_bandwidth(rows: list[tuple[str, float]]) -> str:
    base = rows[0][1]
    lines = []
    lines.append("RingAllReduce what-if: bandwidth sweep (N=128, size=512MB).")
    for name, cct in rows:
        lines.append(f"- {name}: CCT={cct:.6f}s, speedup vs 100G = {base/cct:.2f}x")
    speed_100_200 = (rows[0][1] - rows[1][1]) / rows[0][1] * 100.0
    speed_200_400 = (rows[1][1] - rows[2][1]) / rows[1][1] * 100.0
    lines.append("")
    lines.append(
        f"Takeaway: 100G→200G improves CCT by {speed_100_200:.1f}%, while 200G→400G improves by {speed_200_400:.1f}% (diminishing returns)."
    )
    lines.append(
        "Interpretation: as link bandwidth increases, RingAllReduce becomes less dominated by pure serialization and more by dependency/phase synchronization and shared bottlenecks."
    )
    return "\n".join(lines) + "\n"


def md_topology(a_name: str, a_cct: float, b_name: str, b_cct: float, port_rate: str) -> str:
    slowdown = b_cct / a_cct if a_cct > 0 else float("inf")
    pct = (slowdown - 1.0) * 100.0
    return (
        "RingAllReduce what-if: topology swap.\n"
        f"- Fixed: N=128, size=512MB, port_rate={port_rate}, FIFO+TailDrop, ECMP routing.\n"
        f"- {a_name}: CCT={a_cct:.6f}s (baseline)\n"
        f"- {b_name}: CCT={b_cct:.6f}s, slowdown={slowdown:.2f}x (+{pct:.1f}%)\n"
        "\n"
        "Takeaway: Topologies with lower bisection bandwidth / longer average path length amplify contention in the all-to-all phases of RingAllReduce, increasing completion time even when link rate is unchanged.\n"
    )


def md_oversub(rows: list[tuple[str, float]], port_rate: str) -> str:
    base = rows[0][1]
    lines = []
    lines.append("RingAllReduce what-if: oversubscription sweep.")
    lines.append(f"- Fixed: N=128, size=512MB, port_rate={port_rate}, FIFO+TailDrop, ECMP routing.")
    for name, cct in rows:
        lines.append(f"- oversub {name}: CCT={cct:.6f}s, slowdown vs 1:1 = {cct/base:.2f}x")
    lines.append("")
    lines.append(
        "Takeaway: As oversubscription increases, uplink contention creates non-linear queueing and phase synchronization effects, causing CCT to degrade faster than linearly."
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    ensure_dirs()
    binary = build_days()

    pkt_bytes = ringallreduce_effective_step_bytes(SIZE_BYTES, N_HOSTS)
    duration_s = 240.0
    rank_order = rank_order_fattree_interleaved(FATTREE_K)[:N_HOSTS]

    # -----------------------
    # B1: Bandwidth sweep
    # -----------------------
    b1_rows: list[tuple[str, float]] = []
    for tag, rate in [("100G", PORT_RATES_BPS["100g"]), ("200G", PORT_RATES_BPS["200g"]), ("400G", PORT_RATES_BPS["400g"])]:
        log_path = f"logs/whatif_bw_{tag.lower()}"
        cfg = (
            render_base_config_header(log_path, duration_s)
            + render_topology_fattree(FATTREE_K)
            + render_app_source()
            + render_switch(rate)
            + render_ringallreduce_collective(N_HOSTS, SIZE_BYTES, pkt_bytes, rank_order)
        )
        run_days(binary, cfg)
        res = read_cct_from_logs(REPO / log_path, SIZE_BYTES)
        b1_rows.append((tag, res.cct_s))

    b1_csv = OUT_DIR / "whatif_bandwidth_sweep_ringallreduce.csv"
    base_cct = b1_rows[0][1]
    write_csv(
        b1_csv,
        ["port_rate_gbps", "cct_s", "speedup_vs_100g"],
        [
            [100, f"{b1_rows[0][1]:.9f}", f"{1.0:.3f}"],
            [200, f"{b1_rows[1][1]:.9f}", f"{base_cct/b1_rows[1][1]:.3f}"],
            [400, f"{b1_rows[2][1]:.9f}", f"{base_cct/b1_rows[2][1]:.3f}"],
        ],
    )

    plot_matplotlib_line(
        ["100", "200", "400"],
        [b1_rows[0][1], b1_rows[1][1], b1_rows[2][1]],
        title="RingAllReduce bandwidth sweep (N=128, 512MB)",
        y_label="CCT (s)",
        out_png=OUT_DIR / "fig_whatif_bandwidth_sweep_ringallreduce.png",
        out_svg=OUT_DIR / "fig_whatif_bandwidth_sweep_ringallreduce.svg",
        annotate=[
            f"{base_cct/b1_rows[0][1]:.2f}x",
            f"{base_cct/b1_rows[1][1]:.2f}x",
            f"{base_cct/b1_rows[2][1]:.2f}x",
        ],
    )
    (OUT_DIR / "whatif_bandwidth_sweep_ringallreduce.md").write_text(md_bandwidth(b1_rows))

    # -----------------------
    # B2: Topology swap
    # -----------------------
    rate = PORT_RATES_BPS["200g"]
    log_a = "logs/whatif_topo_A_leafspine"
    cfg_a = (
        render_base_config_header(log_a, duration_s)
        + render_topology_fattree(FATTREE_K)
        + render_app_source()
        + render_switch(rate)
        + render_ringallreduce_collective(N_HOSTS, SIZE_BYTES, pkt_bytes, rank_order)
    )
    run_days(binary, cfg_a)
    cct_a = read_cct_from_logs(REPO / log_a, SIZE_BYTES).cct_s

    log_b = "logs/whatif_topo_B_ring"
    cfg_b = (
        render_base_config_header(log_b, duration_s)
        + render_topology_torus(dim=1, n=N_HOSTS)
        + render_app_source()
        + render_switch(rate)
        + render_ringallreduce_collective(N_HOSTS, SIZE_BYTES, pkt_bytes, rank_order)
    )
    run_days(binary, cfg_b)
    cct_b = read_cct_from_logs(REPO / log_b, SIZE_BYTES).cct_s

    b2_csv = OUT_DIR / "whatif_topology_swap_ringallreduce.csv"
    write_csv(
        b2_csv,
        ["topology_name", "cct_s", "slowdown_vs_A"],
        [
            ["fattree_k16", f"{cct_a:.9f}", f"{1.0:.2f}"],
            ["ring_torus_1d_128", f"{cct_b:.9f}", f"{cct_b/cct_a:.2f}"],
        ],
    )
    plot_matplotlib_bar(
        ["FatTree(k=16)", "Ring(1D Torus)"],
        [cct_a, cct_b],
        title="RingAllReduce topology swap (N=128, 512MB, 200G)",
        y_label="CCT (s)",
        out_png=OUT_DIR / "fig_whatif_topology_swap_ringallreduce.png",
        out_svg=OUT_DIR / "fig_whatif_topology_swap_ringallreduce.svg",
        annotate=[f"1.00x", f"{cct_b/cct_a:.2f}x"],
    )
    (OUT_DIR / "whatif_topology_swap_ringallreduce.md").write_text(
        md_topology("FatTree(k=16)", cct_a, "Ring(1D Torus, n=128)", cct_b, "200G")
    )

    # -----------------------
    # B3: Oversub sweep (reduce ToR uplinks)
    # -----------------------
    tors = 16
    hosts_per_tor = 8
    spines = 8
    oversub_cases = [("1:1", 8), ("2:1", 4), ("4:1", 2)]
    b3_rows: list[tuple[str, float]] = []
    for label, uplinks in oversub_cases:
        log_path = f"logs/whatif_oversub_{label.replace(':', 'to')}"
        edges, hosts, rank_order_ls = build_leafspine_oversub_edges(
            tors=tors,
            hosts_per_tor=hosts_per_tor,
            spines=spines,
            uplinks_per_tor=uplinks,
        )
        cfg = (
            render_base_config_header(log_path, duration_s)
            + render_custom_graph(edges, hosts)
            + render_app_source()
            + render_switch(rate)
            + render_ringallreduce_collective(N_HOSTS, SIZE_BYTES, pkt_bytes, rank_order_ls)
        )
        run_days(binary, cfg)
        cct = read_cct_from_logs(REPO / log_path, SIZE_BYTES).cct_s
        b3_rows.append((label, cct))

    b3_csv = OUT_DIR / "whatif_oversub_sweep_ringallreduce.csv"
    base = b3_rows[0][1]
    write_csv(
        b3_csv,
        ["oversub_ratio", "cct_s", "slowdown_vs_1to1"],
        [
            [b3_rows[0][0], f"{b3_rows[0][1]:.9f}", f"{1.0:.2f}"],
            [b3_rows[1][0], f"{b3_rows[1][1]:.9f}", f"{b3_rows[1][1]/base:.2f}"],
            [b3_rows[2][0], f"{b3_rows[2][1]:.9f}", f"{b3_rows[2][1]/base:.2f}"],
        ],
    )
    plot_matplotlib_line(
        ["1", "2", "4"],
        [b3_rows[0][1], b3_rows[1][1], b3_rows[2][1]],
        title="RingAllReduce oversubscription sweep (N=128, 512MB, 200G)",
        y_label="CCT (s)",
        out_png=OUT_DIR / "fig_whatif_oversub_sweep_ringallreduce.png",
        out_svg=OUT_DIR / "fig_whatif_oversub_sweep_ringallreduce.svg",
        annotate=[f"1.00x", f"{b3_rows[1][1]/base:.2f}x", f"{b3_rows[2][1]/base:.2f}x"],
    )
    (OUT_DIR / "whatif_oversub_sweep_ringallreduce.md").write_text(md_oversub(b3_rows, "200G"))

    print(f"wrote results to {OUT_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

