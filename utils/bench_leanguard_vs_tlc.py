#!/usr/bin/env python3
import argparse
import json
import statistics
import subprocess
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

try:
    import tomllib  # py311+
except ModuleNotFoundError:
    import tomli as tomllib  # py310



@dataclass(frozen=True)
class ProtocolSpec:
    protocol: str
    trace_csv: str
    checker: str
    tlc_module: str


PROTOCOLS: list[ProtocolSpec] = [
    ProtocolSpec(
        protocol="aqm", trace_csv="aqm_events.csv", checker="aqm_check", tlc_module="AqmTrace.tla"
    ),
    ProtocolSpec(
        protocol="pfc", trace_csv="pfc_events.csv", checker="pfc_check", tlc_module="PfcTrace.tla"
    ),
    ProtocolSpec(
        protocol="dcqcn",
        trace_csv="dcqcn_events.csv",
        checker="dcqcn_check",
        tlc_module="DcqcnTrace.tla",
    ),
    ProtocolSpec(
        protocol="wfq", trace_csv="wfq_events.csv", checker="wfq_check", tlc_module="WfqTrace.tla"
    ),
    ProtocolSpec(
        protocol="drr", trace_csv="drr_events.csv", checker="drr_check", tlc_module="DrrTrace.tla"
    ),
    ProtocolSpec(
        protocol="cubic",
        trace_csv="cubic_events.csv",
        checker="cubic_check",
        tlc_module="CubicTrace.tla",
    ),
]


@dataclass(frozen=True)
class OneRun:
    config: str
    log_path: str
    protocol: str
    trace_csv: str
    events: int
    checker: str
    checker_ms: int
    tlc_module: str
    tlc_cmd_ms: Optional[int]
    tlc_total_ms: Optional[int]
    tlc_status: Optional[str]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Benchmark LeanGuard checkers vs TLC baseline across protocols."
    )
    p.add_argument(
        "--configs",
        nargs="+",
        default=[
            "configs/dcqcn_simple.toml",
            "configs/dcqcn_multi.toml",
            "configs/dcqcn_1s.toml",
            "configs/dcqcn_2s.toml",
            "configs/dcqcn_10s.toml",
            "configs/pfc.toml",
            "configs/wfq_simple.toml",
            "configs/drr_simple.toml",
            "configs/cubic_simple.toml",
        ],
        help="Config TOML paths to benchmark (requires logs already exist).",
    )
    p.add_argument(
        "--protocols",
        nargs="+",
        default=[p.protocol for p in PROTOCOLS],
        choices=[p.protocol for p in PROTOCOLS],
        help="Protocols to include in the benchmark.",
    )
    p.add_argument("--reps", type=int, default=5, help="Repetitions per config.")
    p.add_argument(
        "--leanguard-run",
        default="target/debug/leanguard-run",
        help="Path to the leanguard-run binary.",
    )
    p.add_argument(
        "--checker-dir",
        default="lean/.lake/build/bin",
        help="Directory containing LeanGuard checker executables.",
    )
    p.add_argument(
        "--tlc-jar",
        default="/tmp/leanguard_refs/tla2tools_v1.7.4.jar",
        help="Path to tla2tools.jar for TLC.",
    )
    p.add_argument(
        "--out",
        default="",
        help="Output JSON path (default: logs/bench_leanguard_vs_tlc_<date>.json).",
    )
    return p.parse_args()


def mean(xs: list[float]) -> float:
    return statistics.mean(xs)


def stdev(xs: list[float]) -> float:
    return statistics.stdev(xs) if len(xs) >= 2 else 0.0


def count_events(csv_path: Path) -> int:
    # Events = lines - 1 header.
    with csv_path.open("rb") as f:
        lines = sum(1 for _ in f)
    return max(0, lines - 1)


def run_one(
    leanguard_run: Path, cfg_path: Path, checker_dir: Path, tlc_jar: Path
) -> dict[str, Any]:
    cmd = [
        str(leanguard_run),
        "--config",
        str(cfg_path),
        "--mode",
        "check-only",
        "--checker-dir",
        str(checker_dir),
        "--tlc-check",
        "--tlc-jar",
        str(tlc_jar),
    ]
    out = subprocess.check_output(cmd)
    return json.loads(out)


def find_checker_runtime_ms(summary: dict[str, Any], checker: str) -> Optional[int]:
    for r in summary.get("checker_results", []):
        if r.get("checker") == checker:
            rt = r.get("runtime_ms")
            return int(rt) if rt is not None else None
    return None


def find_tlc_result(summary: dict[str, Any], tlc_module: str) -> Optional[dict[str, Any]]:
    tlc_results = summary.get("tlc_results") or []
    for r in tlc_results:
        module_path = r.get("module", "")
        if module_path.endswith("/" + tlc_module) or module_path.endswith("\\" + tlc_module) or (
            module_path.endswith(tlc_module)
        ):
            return r
    return None


def main() -> None:
    args = parse_args()

    leanguard_run = Path(args.leanguard_run)
    checker_dir = Path(args.checker_dir)
    tlc_jar = Path(args.tlc_jar)

    if not leanguard_run.is_file():
        raise SystemExit(f"Missing leanguard-run binary: {leanguard_run}")
    if not checker_dir.is_dir():
        raise SystemExit(f"Missing checker dir: {checker_dir}")
    if not tlc_jar.is_file():
        raise SystemExit(f"Missing TLC jar: {tlc_jar}")

    configs = [Path(c) for c in args.configs]
    for c in configs:
        if not c.is_file():
            raise SystemExit(f"Missing config: {c}")

    selected_protocols = set(args.protocols)
    specs = [p for p in PROTOCOLS if p.protocol in selected_protocols]

    runs: list[OneRun] = []
    for cfg_path in configs:
        cfg_data = tomllib.loads(cfg_path.read_text())
        log_path = Path(cfg_data.get("log_path", ""))
        if not log_path.is_dir():
            raise SystemExit(
                f"Missing log_path for {cfg_path}: {log_path}. "
                "Run the simulation first to generate logs."
            )

        available_traces = {p.name for p in log_path.glob("*_events.csv") if p.is_file()}

        for _ in range(args.reps):
            v = run_one(leanguard_run, cfg_path, checker_dir, tlc_jar)

            for spec in specs:
                if spec.trace_csv not in available_traces:
                    continue

                trace_path = log_path / spec.trace_csv
                if not trace_path.is_file():
                    continue
                event_count = count_events(trace_path)
                if event_count <= 0:
                    continue

                rt = find_checker_runtime_ms(v, spec.checker)
                if rt is None:
                    raise SystemExit(
                        f"Missing {spec.checker} runtime_ms in output for {cfg_path}"
                    )

                tlc = find_tlc_result(v, spec.tlc_module)
                if tlc is None:
                    raise SystemExit(
                        f"Missing TLC result for {spec.tlc_module} in output for {cfg_path}"
                    )

                runs.append(
                    OneRun(
                        config=str(cfg_path),
                        log_path=str(log_path),
                        protocol=spec.protocol,
                        trace_csv=spec.trace_csv,
                        events=event_count,
                        checker=spec.checker,
                        checker_ms=int(rt),
                        tlc_module=spec.tlc_module,
                        tlc_cmd_ms=tlc.get("runtime_ms"),
                        tlc_total_ms=tlc.get("total_runtime_ms"),
                        tlc_status=tlc.get("status"),
                    )
                )

    by_key: dict[tuple[str, str], list[OneRun]] = {}
    for r in runs:
        by_key.setdefault((r.protocol, r.config), []).append(r)

    timestamp = datetime.now().isoformat(timespec="seconds")
    print(f"Benchmark timestamp: {timestamp}")
    print(f"Repetitions per config: {args.reps}")
    print()

    header = [
        "protocol",
        "config",
        "events",
        "checker_ms_mean",
        "checker_ms_stdev",
        "tlc_total_ms_mean",
        "tlc_total_ms_stdev",
        "tlc_cmd_ms_mean",
        "tlc_cmd_ms_stdev",
        "tlc_total/lean_ratio_mean",
        "tlc_statuses",
    ]
    print("\t".join(header))

    summary: list[dict[str, Any]] = []
    for (protocol, cfg), rs in sorted(by_key.items()):
        dc = [r.checker_ms for r in rs]
        tlc_total = [r.tlc_total_ms for r in rs if r.tlc_total_ms is not None]
        tlc_cmd = [r.tlc_cmd_ms for r in rs if r.tlc_cmd_ms is not None]
        statuses = sorted({r.tlc_status for r in rs})

        ratio = [t / d for (t, d) in zip(tlc_total, dc) if d]

        row = {
            "protocol": protocol,
            "config": cfg,
            "trace_csv": rs[0].trace_csv,
            "events": rs[0].events,
            "checker": rs[0].checker,
            "checker_ms_mean": mean(dc),
            "checker_ms_stdev": stdev(dc),
            "tlc_module": rs[0].tlc_module,
            "tlc_total_ms_mean": mean(tlc_total) if tlc_total else None,
            "tlc_total_ms_stdev": stdev(tlc_total) if tlc_total else None,
            "tlc_cmd_ms_mean": mean(tlc_cmd) if tlc_cmd else None,
            "tlc_cmd_ms_stdev": stdev(tlc_cmd) if tlc_cmd else None,
            "tlc_total_over_lean_ratio_mean": mean(ratio) if ratio else None,
            "tlc_statuses": statuses,
        }
        summary.append(row)

        print(
            "\t".join(
                [
                    protocol,
                    cfg,
                    str(row["events"]),
                    f"{row['checker_ms_mean']:.1f}",
                    f"{row['checker_ms_stdev']:.1f}",
                    f"{row['tlc_total_ms_mean']:.1f}" if row["tlc_total_ms_mean"] else "n/a",
                    f"{row['tlc_total_ms_stdev']:.1f}" if row["tlc_total_ms_stdev"] else "n/a",
                    f"{row['tlc_cmd_ms_mean']:.1f}" if row["tlc_cmd_ms_mean"] else "n/a",
                    f"{row['tlc_cmd_ms_stdev']:.1f}" if row["tlc_cmd_ms_stdev"] else "n/a",
                    f"{row['tlc_total_over_lean_ratio_mean']:.1f}"
                    if row["tlc_total_over_lean_ratio_mean"]
                    else "n/a",
                    ",".join(str(s) for s in statuses),
                ]
            )
        )

    out_path = Path(args.out) if args.out else None
    if out_path is None:
        Path("logs").mkdir(exist_ok=True)
        out_path = Path("logs") / f"bench_leanguard_vs_tlc_{datetime.now().date().isoformat()}.json"
    out_path.write_text(
        json.dumps(
            {
                "timestamp": timestamp,
                "reps": args.reps,
                "protocols": sorted(selected_protocols),
                "leanguard_run": str(leanguard_run),
                "checker_dir": str(checker_dir),
                "tlc_jar": str(tlc_jar),
                "runs": [r.__dict__ for r in runs],
                "summary": summary,
            },
            indent=2,
        )
    )
    print()
    print(f"Wrote: {out_path}")


if __name__ == "__main__":
    main()
