#!/usr/bin/env python3
"""
Calibrate Days simulated CCT against NCCL real measurements.

Inputs (repo root):
  - real_tp2_time.csv / real_tp3_time.csv  (time_us_mean)
  - sim_tp2.csv / sim_tp3.csv              (cct_s)

Outputs (repo root):
  - configs/precision_tp2_calib.toml
  - configs/precision_tp3_calib.toml
  - sim_tp2_calib.csv
  - sim_tp3_calib.csv
  - compare_tp2.csv
  - compare_tp3.csv
  - calib_tp2_time.png
  - calib_tp3_time.png
"""

from __future__ import annotations

import csv
import statistics
import subprocess
from dataclasses import dataclass
from pathlib import Path


NCCL_SIZES = [
    4096,
    16384,
    65536,
    262144,
    1048576,
    4194304,
    8388608,
    16777216,
    33554432,
    67108864,
    134217728,
    268435456,
    536870912,
]

BIG_CUTOFF = 16777216  # 16MB


@dataclass(frozen=True)
class Point:
    size: int
    t_real: float
    t_sim: float


def read_real(path: Path) -> dict[int, float]:
    out: dict[int, float] = {}
    with path.open("r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            size = int(row["size_bytes"])
            t_real = float(row["time_us_mean"]) / 1e6
            out[size] = t_real
    return out


def read_sim(path: Path) -> dict[int, float]:
    out: dict[int, float] = {}
    with path.open("r", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            size = int(row["size_bytes"])
            t_sim = float(row["cct_s"])
            out[size] = t_sim
    return out


def join_points(real: dict[int, float], sim: dict[int, float]) -> list[Point]:
    pts = []
    for s in NCCL_SIZES:
        if s in real and s in sim:
            pts.append(Point(s, real[s], sim[s]))
    return pts


def median_scale(points: list[Point]) -> float:
    ratios = [p.t_real / p.t_sim for p in points if p.size >= BIG_CUTOFF and p.t_sim > 0]
    if not ratios:
        raise RuntimeError("no big-message points to compute scale")
    return float(statistics.median(ratios))


def fit_L(points: list[Point], scale: float) -> float:
    # Fit constant latency on small messages (<16MB). Robust: median residual.
    residuals = [p.t_real - (p.t_sim * scale) for p in points if p.size < BIG_CUTOFF]
    if not residuals:
        return 0.0
    L = float(statistics.median(residuals))
    return max(0.0, L)


def patch_port_rate(config_text: str, new_rate: float) -> str:
    import re

    pat = re.compile(r"(?m)^(port_rate\s*=\s*)([0-9eE+.\-]+)\s*$")
    if not pat.search(config_text):
        raise RuntimeError("cannot find port_rate = ... in config")
    return pat.sub(lambda m: f"{m.group(1)}{new_rate:.6e}", config_text, count=1)


def write_compare(
    out_path: Path, points: list[Point], scale: float, L: float
) -> None:
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["size_bytes", "t_real", "t_sim", "t_sim_scaled", "t_pred", "rel_err"])
        for p in points:
            t_sim_scaled = p.t_sim * scale
            t_pred = t_sim_scaled + L
            rel_err = (t_pred - p.t_real) / p.t_real if p.t_real > 0 else 0.0
            w.writerow(
                [
                    p.size,
                    f"{p.t_real:.9f}",
                    f"{p.t_sim:.9f}",
                    f"{t_sim_scaled:.9f}",
                    f"{t_pred:.9f}",
                    f"{rel_err:.6f}",
                ]
            )


def plot_time(out_png: Path, points: list[Point], scale: float, L: float, title: str) -> None:
    # Avoid heavy plotting dependencies (matplotlib/numpy can crash in some environments).
    # Emit a minimal SVG log2(size) vs log10(time) plot.
    import math

    xs = [p.size for p in points]
    y_real = [p.t_real for p in points]
    y_pred = [(p.t_sim * scale) + L for p in points]

    # log transforms (guard 0)
    x_log2 = [math.log2(x) for x in xs]
    y_real_log = [math.log10(max(v, 1e-12)) for v in y_real]
    y_pred_log = [math.log10(max(v, 1e-12)) for v in y_pred]

    w, h = 820, 460
    pad_l, pad_r, pad_t, pad_b = 70, 20, 40, 55
    x0, x1 = min(x_log2), max(x_log2)
    y0, y1 = min(min(y_real_log), min(y_pred_log)), max(max(y_real_log), max(y_pred_log))
    if abs(x1 - x0) < 1e-9:
        x1 = x0 + 1.0
    if abs(y1 - y0) < 1e-9:
        y1 = y0 + 1.0

    def sx(x: float) -> float:
        return pad_l + (x - x0) * (w - pad_l - pad_r) / (x1 - x0)

    def sy(y: float) -> float:
        # SVG y downwards
        return pad_t + (y1 - y) * (h - pad_t - pad_b) / (y1 - y0)

    def poly(points_xy: list[tuple[float, float]]) -> str:
        pts = " ".join(f"{sx(a):.2f},{sy(b):.2f}" for a, b in points_xy)
        return pts

    real_pts = poly(list(zip(x_log2, y_real_log)))
    pred_pts = poly(list(zip(x_log2, y_pred_log)))

    svg = f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">
  <rect x="0" y="0" width="{w}" height="{h}" fill="white"/>
  <text x="{w/2:.1f}" y="24" text-anchor="middle" font-family="sans-serif" font-size="16">{title}</text>

  <!-- axes -->
  <line x1="{pad_l}" y1="{pad_t}" x2="{pad_l}" y2="{h-pad_b}" stroke="#222" stroke-width="1"/>
  <line x1="{pad_l}" y1="{h-pad_b}" x2="{w-pad_r}" y2="{h-pad_b}" stroke="#222" stroke-width="1"/>
  <text x="{w/2:.1f}" y="{h-18}" text-anchor="middle" font-family="sans-serif" font-size="13">log2(size_bytes)</text>
  <text x="18" y="{h/2:.1f}" text-anchor="middle" font-family="sans-serif" font-size="13" transform="rotate(-90 18 {h/2:.1f})">log10(time_s)</text>

  <!-- lines -->
  <polyline fill="none" stroke="#1f77b4" stroke-width="2" points="{real_pts}"/>
  <polyline fill="none" stroke="#ff7f0e" stroke-width="2" points="{pred_pts}"/>

  <!-- legend -->
  <rect x="{w-280}" y="{pad_t}" width="260" height="56" fill="white" stroke="#ccc"/>
  <line x1="{w-265}" y1="{pad_t+18}" x2="{w-235}" y2="{pad_t+18}" stroke="#1f77b4" stroke-width="2"/>
  <text x="{w-225}" y="{pad_t+22}" font-family="sans-serif" font-size="12">real (time_us_mean)</text>
  <line x1="{w-265}" y1="{pad_t+38}" x2="{w-235}" y2="{pad_t+38}" stroke="#ff7f0e" stroke-width="2"/>
  <text x="{w-225}" y="{pad_t+42}" font-family="sans-serif" font-size="12">pred = t_sim*scale + L</text>
</svg>
"""

    out_png.write_text(svg)


def run_sweep(repo: Path, tp: int, template: Path, out_csv: Path) -> None:
    sizes_arg = ",".join([f"{s}B" for s in NCCL_SIZES])
    subprocess.run(
        [
            "python3",
            "utils/precision_sweep.py",
            "--tp",
            str(tp),
            "--template",
            str(template),
            "--out",
            str(out_csv),
            "--sizes",
            sizes_arg,
        ],
        cwd=repo,
        check=True,
    )


def calibrate_one(repo: Path, tp: int) -> None:
    real_path = repo / f"real_tp{tp}_time.csv"
    sim_path = repo / f"sim_tp{tp}.csv"
    cfg_path = repo / "configs" / f"precision_tp{tp}.toml"
    cfg_calib_path = repo / "configs" / f"precision_tp{tp}_calib.toml"
    sim_calib_path = repo / f"sim_tp{tp}_calib.csv"
    compare_path = repo / f"compare_tp{tp}.csv"
    plot_path = repo / f"calib_tp{tp}_time.svg"

    real = read_real(real_path)
    sim = read_sim(sim_path)
    pts = join_points(real, sim)
    if len(pts) != len(NCCL_SIZES):
        raise RuntimeError(f"tp{tp}: expected {len(NCCL_SIZES)} points, got {len(pts)}")

    scale = median_scale(pts)

    cfg_txt = cfg_path.read_text()
    # Extract old port_rate (best-effort) for reporting.
    import re

    m = re.search(r"(?m)^port_rate\s*=\s*([0-9eE+.\-]+)\s*$", cfg_txt)
    old_rate = float(m.group(1)) if m else None

    if old_rate is None:
        raise RuntimeError(f"tp{tp}: cannot parse port_rate from {cfg_path}")
    new_rate = old_rate / scale

    cfg_calib_path.write_text(patch_port_rate(cfg_txt, new_rate))

    # Re-run Days with calibrated port_rate to produce sim_tp*_calib.csv
    if not sim_calib_path.exists():
        run_sweep(repo, tp, cfg_calib_path, sim_calib_path)
    else:
        print(f"tp{tp}: found existing {sim_calib_path.name}, skip rerun")

    # Post-fit latency L on small messages using scaled original sim times.
    L = fit_L(pts, scale)

    # Emit compare table and plot (pred uses scaled original sim + L).
    write_compare(compare_path, pts, scale, L)
    plot_time(
        plot_path,
        pts,
        scale,
        L,
        title=f"TP{tp} calibration (scale={scale:.3f}, L={L*1e6:.1f}us)",
    )

    print(
        f"tp{tp}: scale={scale:.4f}, port_rate {old_rate:.3e} -> {new_rate:.3e}, L={L*1e6:.1f}us"
    )


def main() -> int:
    repo = Path(__file__).resolve().parents[1]
    calibrate_one(repo, 2)
    calibrate_one(repo, 3)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

