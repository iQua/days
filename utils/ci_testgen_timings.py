#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


PROTOCOL_SEED_CONFIGS: dict[str, str] = {
    "aqm": "configs/simple.toml",
    "dcqcn": "configs/dcqcn_simple.toml",
    "pfc": "configs/pfc.toml",
    "wfq": "configs/wfq_simple.toml",
    "drr": "configs/drr_simple.toml",
    "cubic": "configs/cubic_simple.toml",
}


@dataclass(frozen=True)
class TimedRun:
    seconds: float
    stdout: str


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def run_checked(argv: list[str], *, cwd: Path) -> TimedRun:
    start = time.perf_counter()
    completed = subprocess.run(
        argv,
        cwd=str(cwd),
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    elapsed = time.perf_counter() - start
    if completed.returncode != 0:
        tail = "\n".join(completed.stderr.splitlines()[-40:])
        raise RuntimeError(
            f"Command failed ({completed.returncode}): {' '.join(argv)}\n"
            f"--- stderr (tail) ---\n{tail}\n"
        )
    return TimedRun(seconds=elapsed, stdout=completed.stdout)


def parse_json_stdout(stdout: str) -> dict:
    try:
        return json.loads(stdout)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON output: {e}\n--- stdout ---\n{stdout}\n") from e


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def append_csv_row(path: Path, row: dict[str, object]) -> None:
    ensure_parent(path)
    write_header = not path.exists()
    with path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(
        description="Run leanguard-testgen end-to-end for one protocol and emit a CSV timing row."
    )
    p.add_argument(
        "--protocol",
        required=True,
        choices=sorted(PROTOCOL_SEED_CONFIGS.keys()),
        help="Target protocol to run.",
    )
    p.add_argument(
        "--budget",
        type=int,
        default=1,
        help="Campaign budget (number of generated cases). Default: 1.",
    )
    p.add_argument(
        "--rng-seed",
        type=int,
        default=1,
        help="RNG seed for campaign selection/mutation. Default: 1.",
    )
    p.add_argument(
        "--max-calibration-iters",
        type=int,
        default=0,
        help="Max calibration iterations per case. Default: 0.",
    )
    p.add_argument(
        "--out",
        type=Path,
        required=True,
        help="CSV output path (will be created/appended).",
    )
    p.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Repo root (default: inferred from script location).",
    )
    p.add_argument(
        "--testgen-bin",
        type=Path,
        default=Path("target/release/leanguard-testgen"),
        help="Path to leanguard-testgen binary (default: target/release/leanguard-testgen).",
    )
    p.add_argument(
        "--leanguard-run",
        type=Path,
        default=Path("target/release/leanguard-run"),
        help="Path to leanguard-run binary (default: target/release/leanguard-run).",
    )
    p.add_argument(
        "--checker-dir",
        type=Path,
        default=Path("lean/.lake/build/bin"),
        help="Path to LeanGuard checker binaries (default: lean/.lake/build/bin).",
    )
    args = p.parse_args(argv)

    repo_root = args.repo_root.resolve()
    testgen_bin = (repo_root / args.testgen_bin).resolve()
    leanguard_run = (repo_root / args.leanguard_run).resolve()
    checker_dir = (repo_root / args.checker_dir).resolve()

    seed_rel = PROTOCOL_SEED_CONFIGS[args.protocol]
    seed_src = (repo_root / seed_rel).resolve()
    if not seed_src.exists():
        raise RuntimeError(f"Seed config missing: {seed_src}")

    if not testgen_bin.exists():
        raise RuntimeError(
            f"Missing {testgen_bin}. Build first, e.g.:\n"
            f"  cargo build --release --features lean,dcqcn,l2_pfc "
            f"--bin leanguard-run --bin leanguard-testgen\n"
        )
    if not leanguard_run.exists():
        raise RuntimeError(f"Missing {leanguard_run}. (Did you build --bin leanguard-run?)")
    if not checker_dir.exists():
        raise RuntimeError(
            f"Missing checker dir: {checker_dir}. Build Lean checkers first, e.g.:\n"
            f"  (cd lean && lake build)\n"
        )

    git_sha = os.environ.get("GITHUB_SHA") or os.environ.get("GIT_SHA") or ""
    run_id = os.environ.get("GITHUB_RUN_ID") or ""
    runner_os = os.environ.get("RUNNER_OS") or ""
    lean_build_s = os.environ.get("CI_LEAN_BUILD_S") or ""
    rust_build_s = os.environ.get("CI_RUST_BUILD_S") or ""

    with tempfile.TemporaryDirectory(prefix=f"testgen_ci_{args.protocol}_") as tmp:
        tmp_path = Path(tmp)
        corpus_root = tmp_path / "corpus"
        seeds_src_dir = tmp_path / "seeds_src"
        seeds_src_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(seed_src, seeds_src_dir / seed_src.name)

        common_prefix = [
            str(testgen_bin),
            "--corpus-root",
            str(corpus_root),
            "--checker-dir",
            str(checker_dir),
            "--leanguard-run",
            str(leanguard_run),
        ]

        seed_index = run_checked(common_prefix + ["seed-index", str(seeds_src_dir)], cwd=repo_root)
        _seed_index_json = parse_json_stdout(seed_index.stdout)

        campaign = run_checked(
            common_prefix
            + [
                "campaign",
                "--protocol",
                args.protocol,
                "--budget",
                str(args.budget),
                "--rng-seed",
                str(args.rng_seed),
                "--max-calibration-iters",
                str(args.max_calibration_iters),
            ],
            cwd=repo_root,
        )
        campaign_json = parse_json_stdout(campaign.stdout)

    row: dict[str, object] = {
        "timestamp_utc": utc_now_iso(),
        "git_sha": git_sha,
        "run_id": run_id,
        "runner_os": runner_os,
        "lean_build_s": lean_build_s,
        "rust_build_s": rust_build_s,
        "protocol": args.protocol,
        "seed_config": seed_rel,
        "budget": args.budget,
        "rng_seed": args.rng_seed,
        "max_calibration_iters": args.max_calibration_iters,
        "seed_index_s": round(seed_index.seconds, 6),
        "campaign_s": round(campaign.seconds, 6),
        "total_s": round(seed_index.seconds + campaign.seconds, 6),
        "attempted": campaign_json.get("attempted"),
        "accepted": campaign_json.get("accepted"),
        "rejected": campaign_json.get("rejected"),
        "errors": campaign_json.get("errors"),
        "dry_run": campaign_json.get("dry_run"),
    }
    append_csv_row(args.out, row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
