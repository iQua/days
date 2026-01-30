#!/usr/bin/env python3
import argparse
import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ExportPaths:
    bench_json: Path
    fault_json: Path
    out_md: Path
    out_bench_runs_csv: Path
    out_bench_summary_csv: Path
    out_fault_runs_csv: Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Export experiment artifacts (bench + fault-injection) to Markdown and CSV."
    )
    p.add_argument(
        "--bench-json",
        default="",
        help="Path to bench_leanguard_vs_tlc_*.json (default: pick latest under logs/).",
    )
    p.add_argument(
        "--fault-json",
        default="",
        help="Path to fault_injection_agreement_*.json (default: pick latest under logs/).",
    )
    p.add_argument(
        "--out-md",
        default="ideas/results.md",
        help="Markdown output path (default: ideas/results.md).",
    )
    p.add_argument(
        "--out-dir",
        default="logs",
        help="Directory for CSV exports (default: logs).",
    )
    return p.parse_args()


def find_latest(glob_pattern: str) -> Path:
    candidates = list(Path(".").glob(glob_pattern))
    candidates = [p for p in candidates if p.is_file()]
    if not candidates:
        raise SystemExit(f"No files match: {glob_pattern}")
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except Exception as e:
        raise SystemExit(f"Failed to parse JSON {path}: {e}") from e


def fmt_mean_stdev(mean: float | None, stdev: float | None) -> str:
    if mean is None:
        return ""
    if stdev is None:
        return f"{mean:.1f}"
    return f"{mean:.1f} ± {stdev:.1f}"


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def export_bench(bench: dict[str, Any], out_dir: Path) -> tuple[Path, Path]:
    runs = bench.get("runs") or []
    summary = bench.get("summary") or []

    date = str(bench.get("timestamp", "")).split("T")[0] or "unknown_date"
    runs_csv = out_dir / f"bench_leanguard_vs_tlc_{date}_runs.csv"
    summary_csv = out_dir / f"bench_leanguard_vs_tlc_{date}_summary.csv"

    runs_out: list[dict[str, Any]] = []
    for r in runs:
        runs_out.append(dict(r))

    summary_out: list[dict[str, Any]] = []
    for r in summary:
        row = dict(r)
        statuses = row.get("tlc_statuses")
        if isinstance(statuses, list):
            row["tlc_statuses"] = ",".join(str(s) for s in statuses)
        summary_out.append(row)

    write_csv(runs_csv, runs_out)
    write_csv(summary_csv, summary_out)
    return runs_csv, summary_csv


def export_fault(fault: dict[str, Any], out_dir: Path) -> Path:
    runs = fault.get("runs") or []
    date = str(fault.get("timestamp", "")).split("T")[0] or "unknown_date"
    runs_csv = out_dir / f"fault_injection_agreement_{date}.csv"

    flat_runs: list[dict[str, Any]] = []
    for r in runs:
        row = dict(r)
        lean_failure = row.pop("lean_failure", None) or {}
        if isinstance(lean_failure, dict):
            row["lean_failure_line"] = lean_failure.get("csv_line")
            row["lean_failure_time_ns"] = lean_failure.get("time_ns")
            row["lean_failure_event_id"] = lean_failure.get("event_id")
            row["lean_failure_kind"] = lean_failure.get("kind")
        else:
            row["lean_failure_line"] = None
            row["lean_failure_time_ns"] = None
            row["lean_failure_event_id"] = None
            row["lean_failure_kind"] = None
        flat_runs.append(row)

    write_csv(runs_csv, flat_runs)
    return runs_csv


def md_escape(s: str) -> str:
    return s.replace("|", "\\|")


def render_markdown(bench: dict[str, Any], fault: dict[str, Any], paths: ExportPaths) -> str:
    bench_rows = list(bench.get("summary") or [])
    fault_rows = list(fault.get("runs") or [])

    bench_ts = str(bench.get("timestamp", ""))
    fault_ts = str(fault.get("timestamp", ""))

    lines: list[str] = []
    lines.append("# Experiment results\n")

    lines.append("## Benchmarks (LeanGuard vs TLC)\n")
    lines.append(f"- Timestamp: `{bench_ts}`")
    lines.append(f"- Raw data: `{paths.bench_json}`")
    lines.append(f"- CSV (runs): `{paths.out_bench_runs_csv}`")
    lines.append(f"- CSV (summary): `{paths.out_bench_summary_csv}`\n")

    lines.append(
        "| Protocol | Config | Events | checker_ms | tlc_total_ms | tlc_cmd_ms | tlc_total/checker | TLC statuses |"
    )
    lines.append("|---|---|---:|---:|---:|---:|---:|---|")
    for r in bench_rows:
        lines.append(
            "| "
            + " | ".join(
                [
                    md_escape(str(r.get("protocol", ""))),
                    md_escape(str(r.get("config", ""))),
                    str(r.get("events", "")),
                    fmt_mean_stdev(
                        r.get("checker_ms_mean"), r.get("checker_ms_stdev")
                    ),
                    fmt_mean_stdev(
                        r.get("tlc_total_ms_mean"), r.get("tlc_total_ms_stdev")
                    ),
                    fmt_mean_stdev(r.get("tlc_cmd_ms_mean"), r.get("tlc_cmd_ms_stdev")),
                    f"{(r.get('tlc_total_over_lean_ratio_mean') or 0):.1f}",
                    md_escape(",".join(r.get("tlc_statuses") or [])),
                ]
            )
            + " |"
        )

    lines.append("\n## Fault-injection agreement (LeanGuard vs TLC)\n")
    lines.append(f"- Timestamp: `{fault_ts}`")
    lines.append(f"- Raw data: `{paths.fault_json}`")
    lines.append(f"- CSV: `{paths.out_fault_runs_csv}`\n")

    lines.append("| Protocol | Case | Lean | TLC | Lean first failure | TLC first failure |")
    lines.append("|---|---|---|---|---|---|")
    for r in fault_rows:
        lean_failure = r.get("lean_failure") or {}
        lean_key = ""
        if lean_failure:
            t = lean_failure.get("time_ns")
            e = lean_failure.get("event_id")
            k = lean_failure.get("kind")
            lean_key = f"line {lean_failure.get('csv_line')} ({t},{e},{k})"

        tlc_key = ""
        if r.get("tlc_failure_time_ns") is not None or r.get("tlc_failure_event_id") is not None:
            tlc_key = (
                f"idx {r.get('tlc_failure_index')} "
                f"({r.get('tlc_failure_time_ns')},{r.get('tlc_failure_event_id')},{r.get('tlc_failure_kind')})"
            )

        lines.append(
            "| "
            + " | ".join(
                [
                    md_escape(str(r.get("protocol", ""))),
                    md_escape(str(r.get("case", ""))),
                    md_escape(str(r.get("lean_status", ""))),
                    md_escape(str(r.get("tlc_status", ""))),
                    md_escape(lean_key),
                    md_escape(tlc_key),
                ]
            )
            + " |"
        )

    lines.append(
        "\n## Notes / caveats\n\n"
        "- The TLC baseline for `CubicTrace.tla` is validated against `configs/cubic_simple.toml`.\n"
        "- `configs/benchmarks/leanguard/tcp_cubic_micro.toml` currently produces a TLC **reject** for `CubicTrace.tla` (the fixed-point approximation drifts on longer traces), so it should not be used as an “accept” benchmark without adjusting either the config or the spec.\n"
    )

    return "\n".join(lines).rstrip() + "\n"


def resolve_paths(args: argparse.Namespace) -> ExportPaths:
    bench_json = Path(args.bench_json) if args.bench_json else find_latest("logs/bench_leanguard_vs_tlc_*.json")
    fault_json = Path(args.fault_json) if args.fault_json else find_latest("logs/fault_injection_agreement_*.json")

    bench = load_json(bench_json)
    fault = load_json(fault_json)
    out_dir = Path(args.out_dir)

    runs_csv, summary_csv = export_bench(bench, out_dir)
    fault_csv = export_fault(fault, out_dir)

    return ExportPaths(
        bench_json=bench_json,
        fault_json=fault_json,
        out_md=Path(args.out_md),
        out_bench_runs_csv=runs_csv,
        out_bench_summary_csv=summary_csv,
        out_fault_runs_csv=fault_csv,
    )


def main() -> None:
    args = parse_args()
    paths = resolve_paths(args)

    bench = load_json(paths.bench_json)
    fault = load_json(paths.fault_json)

    md = render_markdown(bench, fault, paths)
    paths.out_md.parent.mkdir(parents=True, exist_ok=True)
    paths.out_md.write_text(md)
    print(f"Wrote: {paths.out_md}")
    print(f"Wrote: {paths.out_bench_runs_csv}")
    print(f"Wrote: {paths.out_bench_summary_csv}")
    print(f"Wrote: {paths.out_fault_runs_csv}")


if __name__ == "__main__":
    main()
