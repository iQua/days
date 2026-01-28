#!/usr/bin/env python3
import argparse
import csv
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path


RE_SIZE = re.compile(r"(?m)^(size\s*=\s*)\d+\s*$")
RE_LOG_PATH = re.compile(r'(?m)^(log_path\s*=\s*)"(.*)"\s*$')


def load_collective_cct(log_path: Path, collective_type: str, size_bytes: int) -> float:
    events = log_path / "collective_events.csv"
    if not events.exists():
        raise FileNotFoundError(f"missing {events}")

    with events.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("collective_type") != collective_type:
                continue
            if int(row["size_bytes"]) != int(size_bytes):
                continue
            start = float(row["start_time_s"])
            end = float(row["end_time_s"])
            if end < start:
                raise ValueError(f"bad times: start={start} end={end} in {events}")
            return end - start

    raise RuntimeError(
        f"no matching {collective_type} row for size_bytes={size_bytes} in {events}"
    )


def render_config(template_path: Path, size_bytes: int, log_path: str) -> str:
    s = template_path.read_text()
    if not RE_SIZE.search(s):
        raise RuntimeError(f"cannot find `size = ...` in {template_path}")
    if not RE_LOG_PATH.search(s):
        raise RuntimeError(f"cannot find `log_path = ...` in {template_path}")

    s = RE_SIZE.sub(rf"\g<1>{int(size_bytes)}", s, count=1)
    s = RE_LOG_PATH.sub(rf'\g<1>"{log_path}"', s, count=1)
    return s


def run_one(binary: Path, config_text: str) -> None:
    with tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False) as tf:
        tf.write(config_text)
        tf.flush()
        tmp_path = tf.name
    try:
        subprocess.run([str(binary), tmp_path], check=True)
    finally:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass


def sweep(tp: int, sizes: list[int], collective_type: str) -> None:
    repo = Path(__file__).resolve().parents[1]
    if collective_type == "RingAllReduce":
        template = repo / "configs" / f"precision_tp{tp}.toml"
        out_csv = repo / f"sim_tp{tp}.csv"
        log_root = f"logs/precision_tp{tp}"
    elif collective_type == "Broadcast":
        template = repo / "configs" / f"bcast_tp{tp}.toml"
        out_csv = repo / f"sim_bcast_tp{tp}.csv"
        log_root = f"logs/bcast_tp{tp}"
    else:
        raise ValueError(f"unsupported collective_type: {collective_type}")

    subprocess.run(["cargo", "build", "--release", "--bin", "days"], cwd=repo, check=True)
    binary = repo / "target" / "release" / "days"
    if not binary.exists():
        raise FileNotFoundError(f"missing built binary {binary}")

    rows = []
    for size_bytes in sizes:
        log_path = f"{log_root}/size_{size_bytes}"
        cfg = render_config(template, size_bytes, log_path)
        run_one(binary, cfg)
        cct = load_collective_cct(repo / log_path, collective_type, size_bytes)
        rows.append((size_bytes, cct))

    with out_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["size_bytes", "cct_s"])
        for size_bytes, cct in rows:
            w.writerow([int(size_bytes), f"{cct:.9f}"])

    print(f"wrote {out_csv}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--tp",
        type=int,
        choices=[2, 3],
        required=True,
        help="tensor parallel degree (2 or 3 hosts)",
    )
    ap.add_argument(
        "--collective-type",
        type=str,
        choices=["RingAllReduce", "Broadcast"],
        default="RingAllReduce",
        help="collective type to sweep (default: RingAllReduce)",
    )
    ap.add_argument(
        "--template",
        type=str,
        default="",
        help="optional TOML template path (defaults depend on --collective-type)",
    )
    ap.add_argument(
        "--out",
        type=str,
        default="",
        help="optional output CSV path (defaults depend on --collective-type)",
    )
    ap.add_argument(
        "--log-root",
        type=str,
        default="",
        help="optional log root directory (defaults depend on --collective-type)",
    )
    ap.add_argument(
        "--sizes",
        type=str,
        default="4096B,16384B,65536B,262144B,1048576B,4194304B,8388608B,16777216B,33554432B,67108864B,134217728B,268435456B,536870912B",
        help="comma-separated sizes in bytes (NCCL-aligned default list)",
    )
    args = ap.parse_args()

    def parse_size(x: str) -> int:
        x = x.strip().upper()
        m = re.fullmatch(r"(\d+)(B|KB|MB|GB|KIB|MIB|GIB)", x)
        if not m:
            raise ValueError(f"bad size token: {x}")
        n = int(m.group(1))
        unit = m.group(2)
        if unit == "B":
            return n
        if unit == "KB":
            return n * 10**3
        if unit == "MB":
            return n * 10**6
        if unit == "GB":
            return n * 10**9
        if unit == "KIB":
            return n * 2**10
        if unit == "MIB":
            return n * 2**20
        if unit == "GIB":
            return n * 2**30
        raise ValueError(f"unsupported unit: {unit}")

    sizes = [parse_size(tok) for tok in args.sizes.split(",") if tok.strip()]

    # Allow overriding template/output without changing default sweep behavior.
    repo = Path(__file__).resolve().parents[1]
    collective_type = args.collective_type
    if args.template:
        template_path = Path(args.template)
        if not template_path.is_absolute():
            template_path = (repo / template_path).resolve()
    else:
        if collective_type == "RingAllReduce":
            template_path = repo / "configs" / f"precision_tp{args.tp}.toml"
        elif collective_type == "Broadcast":
            template_path = repo / "configs" / f"bcast_tp{args.tp}.toml"
        else:
            raise ValueError(f"unsupported collective_type: {collective_type}")

    if args.out:
        out_path = Path(args.out)
        if not out_path.is_absolute():
            out_path = (repo / out_path).resolve()
    else:
        if collective_type == "RingAllReduce":
            out_path = repo / f"sim_tp{args.tp}.csv"
        elif collective_type == "Broadcast":
            out_path = repo / f"sim_bcast_tp{args.tp}.csv"
        else:
            raise ValueError(f"unsupported collective_type: {collective_type}")

    if args.log_root:
        log_root = args.log_root
    else:
        if collective_type == "RingAllReduce":
            log_root = f"logs/precision_tp{args.tp}"
        elif collective_type == "Broadcast":
            log_root = f"logs/bcast_tp{args.tp}"
        else:
            raise ValueError(f"unsupported collective_type: {collective_type}")

    # Inline sweep implementation to use overrides.
    subprocess.run(["cargo", "build", "--release", "--bin", "days"], cwd=repo, check=True)
    binary = repo / "target" / "release" / "days"
    if not binary.exists():
        raise FileNotFoundError(f"missing built binary {binary}")

    rows = []
    for size_bytes in sizes:
        log_path = f"{log_root}/size_{size_bytes}"
        cfg = render_config(template_path, size_bytes, log_path)
        run_one(binary, cfg)
        cct = load_collective_cct(repo / log_path, collective_type, size_bytes)
        rows.append((size_bytes, cct))

    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["size_bytes", "cct_s"])
        for size_bytes, cct in rows:
            w.writerow([int(size_bytes), f"{cct:.9f}"])

    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

