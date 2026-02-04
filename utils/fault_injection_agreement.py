#!/usr/bin/env python3
import argparse
import json
import re
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


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
class FailureInfo:
    csv_line: int
    time_ns: Optional[int]
    event_id: Optional[int]
    kind: Optional[str]


@dataclass(frozen=True)
class OneCase:
    protocol: str
    case: str
    base_trace: str
    mutated_trace: str
    lean_status: str
    lean_runtime_ms: Optional[int]
    lean_peak_rss_kb: Optional[int]
    lean_failure: Optional[FailureInfo]
    tlc_status: str
    tlc_cmd_runtime_ms: Optional[int]
    tlc_total_runtime_ms: Optional[int]
    tlc_peak_rss_kb: Optional[int]
    tlc_failure_index: Optional[int]
    tlc_failure_time_ns: Optional[int]
    tlc_failure_event_id: Optional[int]
    tlc_failure_kind: Optional[str]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run fault-injection agreement checks between LeanGuard and the TLC baseline."
    )
    p.add_argument(
        "--logs-root",
        default="logs",
        help="Root directory to search for existing *_events.csv traces.",
    )
    p.add_argument(
        "--protocols",
        nargs="+",
        default=[p.protocol for p in PROTOCOLS],
        choices=[p.protocol for p in PROTOCOLS],
        help="Protocols to include in the suite.",
    )
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
        "--measure-rss",
        action="store_true",
        help="Ask leanguard-run to sample peak RSS (adds overhead).",
    )
    p.add_argument(
        "--out",
        default="",
        help="Output JSON path (default: logs/fault_injection_agreement_<date>.json).",
    )
    return p.parse_args()


def count_lines(path: Path) -> int:
    with path.open("rb") as f:
        return sum(1 for _ in f)


def find_smallest_trace(logs_root: Path, trace_csv: str) -> Path:
    candidates: list[tuple[int, Path]] = []
    for p in logs_root.glob(f"*/{trace_csv}"):
        if not p.is_file():
            continue
        lines = count_lines(p)
        if lines <= 1:
            continue
        candidates.append((lines, p))
    if not candidates:
        raise SystemExit(f"No usable trace found under {logs_root}: {trace_csv}")
    candidates.sort(key=lambda t: t[0])
    return candidates[0][1]


def load_csv_lines(path: Path) -> list[str]:
    lines = path.read_text().splitlines()
    if not lines:
        raise ValueError(f"empty CSV: {path}")
    return lines


def parse_header_indices(header: str) -> dict[str, int]:
    cols = header.split(",")
    return {c: i for (i, c) in enumerate(cols)}


def get_field(lines: list[str], idx: dict[str, int], csv_line: int, col: str) -> str:
    row = lines[csv_line - 1].split(",")
    return row[idx[col]]


def set_field(lines: list[str], idx: dict[str, int], csv_line: int, col: str, value: str) -> None:
    parts = lines[csv_line - 1].split(",")
    parts[idx[col]] = value
    lines[csv_line - 1] = ",".join(parts)


def find_first_data_line_with_kind(lines: list[str], idx: dict[str, int], kind: str) -> int:
    if "kind" not in idx:
        raise ValueError("CSV has no 'kind' column")
    for csv_line in range(2, len(lines) + 1):
        parts = lines[csv_line - 1].split(",")
        if parts[idx["kind"]] == kind:
            return csv_line
    raise ValueError(f"no row with kind={kind}")


def parse_lean_first_failure(stderr: str) -> Optional[int]:
    # Typical: "REJECT: line 12: ..."
    m = re.search(r"REJECT:\s*line\s+(\d+)\s*:", stderr)
    if m:
        return int(m.group(1))

    # Parse failures can also be "line 12: ..." without the REJECT prefix (older tooling).
    m = re.search(r"\bline\s+(\d+)\s*:", stderr)
    if m:
        return int(m.group(1))

    return None


def failure_info_from_csv(path: Path, csv_line: int) -> FailureInfo:
    lines = load_csv_lines(path)
    idx = parse_header_indices(lines[0])

    def as_int(col: str) -> Optional[int]:
        if col not in idx:
            return None
        raw = get_field(lines, idx, csv_line, col).strip()
        return int(raw) if raw else None

    def as_str(col: str) -> Optional[str]:
        if col not in idx:
            return None
        raw = get_field(lines, idx, csv_line, col).strip()
        return raw if raw else None

    return FailureInfo(
        csv_line=csv_line,
        time_ns=as_int("time_ns"),
        event_id=as_int("event_id"),
        kind=as_str("kind"),
    )


def run_leanguard_run(
    leanguard_run: Path,
    config_path: Path,
    checker_dir: Path,
    tlc_jar: Path,
    measure_rss: bool,
) -> dict[str, Any]:
    cmd = [
        str(leanguard_run),
        "--config",
        str(config_path),
        "--mode",
        "check-only",
        "--checker-dir",
        str(checker_dir),
        "--tlc-check",
        "--tlc-jar",
        str(tlc_jar),
    ]
    if measure_rss:
        cmd.append("--measure-rss")
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, check=False)
    if not proc.stdout:
        raise SystemExit(f"leanguard-run produced no stdout (exit={proc.returncode}): {cmd}")
    return json.loads(proc.stdout)


def find_checker(summary: dict[str, Any], checker: str) -> Optional[dict[str, Any]]:
    for r in summary.get("checker_results", []):
        if r.get("checker") == checker:
            return r
    return None


def find_tlc(summary: dict[str, Any], tlc_module: str) -> Optional[dict[str, Any]]:
    for r in summary.get("tlc_results", []) or []:
        module = r.get("module", "")
        if module.endswith("/" + tlc_module) or module.endswith("\\" + tlc_module) or module.endswith(
            tlc_module
        ):
            return r
    return None


def mutate_case(protocol: str, base: Path, out: Path, case: str) -> None:
    lines = load_csv_lines(base)
    idx = parse_header_indices(lines[0])

    if case == "accept_smoke":
        out.write_text("\n".join(lines) + "\n")
        return

    if protocol == "dcqcn" and case == "alpha_mismatch_first_row":
        # `TraceData.tla` rescales ppb → permille (divide by 1_000_000) and the TLC spec
        # uses a ±1 tolerance in the rescaled units. Change by 2e6 so TLC must reject too.
        set_field(lines, idx, 2, "alpha_ppb", "2000000")
    elif protocol == "dcqcn" and case == "cnp_size_mismatch":
        line = find_first_data_line_with_kind(lines, idx, "cnp_sent")
        set_field(lines, idx, line, "cnp_size_b", "65")
    elif protocol == "aqm" and case == "invalid_ecn_mark":
        set_field(lines, idx, 2, "ecn_before", "not_ect")
        set_field(lines, idx, 2, "ecn_after", "ce")
    elif protocol == "pfc" and case == "pfc_recv_sender_mismatch":
        line = find_first_data_line_with_kind(lines, idx, "pfc_recv")
        cur = get_field(lines, idx, line, "sender_id").strip()
        set_field(lines, idx, line, "sender_id", str(int(cur) + 1 if cur else 1))
    elif protocol == "wfq" and case == "finish_time_mismatch":
        line = find_first_data_line_with_kind(lines, idx, "schedule")
        cur = get_field(lines, idx, line, "finish_time_ns").strip()
        # `TraceData.tla` rescales ns → µs (rounded). A +1ns change can vanish after rescaling
        # and WFQ uses a ±1µs tolerance, so perturb by 5µs to force a TLC reject.
        set_field(lines, idx, line, "finish_time_ns", str(int(cur) + 5000 if cur else 5000))
    elif protocol == "drr" and case == "deficit_mismatch":
        line = find_first_data_line_with_kind(lines, idx, "schedule")
        cur = get_field(lines, idx, line, "deficit_bytes").strip()
        set_field(lines, idx, line, "deficit_bytes", str(int(cur) + 1 if cur else 1))
    elif protocol == "cubic" and case == "cwnd_mismatch":
        set_field(lines, idx, 2, "cwnd_bytes", "999999")
    else:
        raise ValueError(f"unknown mutation: {protocol}:{case}")

    out.write_text("\n".join(lines) + "\n")


def cases_for_protocol(protocol: str) -> list[str]:
    if protocol == "dcqcn":
        return ["accept_smoke", "alpha_mismatch_first_row", "cnp_size_mismatch"]
    if protocol == "aqm":
        return ["accept_smoke", "invalid_ecn_mark"]
    if protocol == "pfc":
        return ["accept_smoke", "pfc_recv_sender_mismatch"]
    if protocol == "wfq":
        return ["accept_smoke", "finish_time_mismatch"]
    if protocol == "drr":
        return ["accept_smoke", "deficit_mismatch"]
    if protocol == "cubic":
        return ["accept_smoke", "cwnd_mismatch"]
    raise ValueError(f"unknown protocol: {protocol}")


def main() -> None:
    args = parse_args()

    leanguard_run = Path(args.leanguard_run)
    checker_dir = Path(args.checker_dir)
    tlc_jar = Path(args.tlc_jar)
    logs_root = Path(args.logs_root)

    if not leanguard_run.is_file():
        raise SystemExit(f"Missing leanguard-run binary: {leanguard_run}")
    if not checker_dir.is_dir():
        raise SystemExit(f"Missing checker dir: {checker_dir}")
    if not tlc_jar.is_file():
        raise SystemExit(f"Missing TLC jar: {tlc_jar}")
    if not logs_root.is_dir():
        raise SystemExit(f"Missing logs root: {logs_root}")

    selected_protocols = set(args.protocols)
    specs = [p for p in PROTOCOLS if p.protocol in selected_protocols]

    timestamp = datetime.now().isoformat(timespec="seconds")
    runs: list[OneCase] = []

    for spec in specs:
        base = find_smallest_trace(logs_root, spec.trace_csv)
        for case in cases_for_protocol(spec.protocol):
            with tempfile.TemporaryDirectory(prefix=f"fault_{spec.protocol}_") as tmp:
                tmp_path = Path(tmp)
                log_path = tmp_path / "logs"
                log_path.mkdir(parents=True, exist_ok=True)

                mutated = log_path / spec.trace_csv
                mutate_case(spec.protocol, base, mutated, case)

                # traces.json drives checker/spec selection.
                (log_path / "traces.json").write_text(
                    json.dumps({"version": 1, "traces": [spec.trace_csv]})
                )
                config_path = tmp_path / "case.toml"
                config_path.write_text(
                    f'log_path = "{log_path.as_posix()}"\nthreading = "single"\n'
                )

                summary = run_leanguard_run(
                    leanguard_run, config_path, checker_dir, tlc_jar, args.measure_rss
                )

                checker = find_checker(summary, spec.checker)
                if checker is None:
                    raise SystemExit(f"Missing checker result: {spec.checker} ({spec.protocol})")

                tlc = find_tlc(summary, spec.tlc_module)
                if tlc is None:
                    raise SystemExit(f"Missing TLC result: {spec.tlc_module} ({spec.protocol})")

                lean_status = str(checker.get("status"))
                lean_rt = checker.get("runtime_ms")
                lean_rss = checker.get("peak_rss_kb")
                lean_failure: Optional[FailureInfo] = None
                if lean_status == "reject":
                    csv_line = parse_lean_first_failure(str(checker.get("stderr", "")))
                    if csv_line is not None:
                        lean_failure = failure_info_from_csv(mutated, csv_line)

                tlc_status = str(tlc.get("status"))
                tlc_cmd_rt = tlc.get("runtime_ms")
                tlc_total_rt = tlc.get("total_runtime_ms")
                tlc_rss = tlc.get("peak_rss_kb")
                tlc_failure = tlc.get("first_failure") or {}

                runs.append(
                    OneCase(
                        protocol=spec.protocol,
                        case=case,
                        base_trace=str(base),
                        mutated_trace=str(mutated),
                        lean_status=lean_status,
                        lean_runtime_ms=int(lean_rt) if lean_rt is not None else None,
                        lean_peak_rss_kb=int(lean_rss) if lean_rss is not None else None,
                        lean_failure=lean_failure,
                        tlc_status=tlc_status,
                        tlc_cmd_runtime_ms=int(tlc_cmd_rt) if tlc_cmd_rt is not None else None,
                        tlc_total_runtime_ms=int(tlc_total_rt) if tlc_total_rt is not None else None,
                        tlc_peak_rss_kb=int(tlc_rss) if tlc_rss is not None else None,
                        tlc_failure_index=(
                            int(tlc_failure.get("index"))
                            if tlc_failure.get("index") is not None
                            else None
                        ),
                        tlc_failure_time_ns=(
                            int(tlc_failure.get("time_ns"))
                            if tlc_failure.get("time_ns") is not None
                            else None
                        ),
                        tlc_failure_event_id=(
                            int(tlc_failure.get("event_id"))
                            if tlc_failure.get("event_id") is not None
                            else None
                        ),
                        tlc_failure_kind=(
                            str(tlc_failure.get("kind"))
                            if tlc_failure.get("kind") is not None
                            else None
                        ),
                    )
                )

    out_path = Path(args.out) if args.out else None
    if out_path is None:
        Path("logs").mkdir(exist_ok=True)
        out_path = Path("logs") / f"fault_injection_agreement_{datetime.now().date().isoformat()}.json"

    out = {
        "timestamp": timestamp,
        "protocols": [s.protocol for s in specs],
        "leanguard_run": str(leanguard_run),
        "checker_dir": str(checker_dir),
        "tlc_jar": str(tlc_jar),
        "measure_rss": bool(args.measure_rss),
        "runs": [
            {
                **r.__dict__,
                "lean_failure": r.lean_failure.__dict__ if r.lean_failure is not None else None,
            }
            for r in runs
        ],
    }
    out_path.write_text(json.dumps(out, indent=2))

    # Print a small summary table.
    print(f"Timestamp: {timestamp}")
    print(f"Wrote: {out_path}")
    print()
    print(
        "\t".join(
            [
                "protocol",
                "case",
                "lean",
                "tlc",
                "lean_line",
                "lean_key",
                "tlc_key",
            ]
        )
    )
    for r in runs:
        lean_line = str(r.lean_failure.csv_line) if r.lean_failure else ""
        lean_key = (
            f"{r.lean_failure.time_ns},{r.lean_failure.event_id}" if r.lean_failure else ""
        )
        tlc_key = (
            f"{r.tlc_failure_time_ns},{r.tlc_failure_event_id}"
            if r.tlc_failure_time_ns is not None and r.tlc_failure_event_id is not None
            else ""
        )
        print(
            "\t".join(
                [
                    r.protocol,
                    r.case,
                    r.lean_status,
                    r.tlc_status,
                    lean_line,
                    lean_key,
                    tlc_key,
                ]
            )
        )


if __name__ == "__main__":
    main()
