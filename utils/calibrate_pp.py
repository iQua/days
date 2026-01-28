#!/usr/bin/env python3
"""
Calibrate Days simulated P2P message time against real measurements.

This script treats each P2P link measurement as a 1-flow Broadcast (PacketDistribution),
so `sim_pp_01.csv` / `sim_pp_12.csv` contain `size_bytes,cct_s`.

Default behavior is to FIT (scale, L) per link:
  - scale: median(t_real/t_sim) over sizes >= 16MB
  - L: median residual over sizes < 16MB

You can alternatively reuse TP3 AllReduce-derived (scale, L) from:
  - real_tp3_time.csv + sim_tp3.csv

Inputs (repo root):
  - real_pp_01_time.csv, sim_pp_01.csv
  - real_pp_12_time.csv, sim_pp_12.csv
  - real_tp3_time.csv, sim_tp3.csv                 (optional for reuse)

Outputs (repo root):
  - compare_pp_01.csv
  - compare_pp_12.csv
  - calib_pp_01_time.svg
  - calib_pp_12_time.svg
"""

from __future__ import annotations

import argparse
import csv
import math
import statistics
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
    residuals = [p.t_real - (p.t_sim * scale) for p in points if p.size < BIG_CUTOFF]
    if not residuals:
        return 0.0
    L = float(statistics.median(residuals))
    return max(0.0, L)


def write_compare(out_path: Path, points: list[Point], scale: float, L: float) -> None:
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


def plot_time(out_path: Path, points: list[Point], scale: float, L: float, title: str) -> None:
    xs = [p.size for p in points]
    y_real = [p.t_real for p in points]
    y_pred = [(p.t_sim * scale) + L for p in points]

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
        return pad_t + (y1 - y) * (h - pad_t - pad_b) / (y1 - y0)

    def poly(points_xy: list[tuple[float, float]]) -> str:
        return " ".join(f"{sx(a):.2f},{sy(b):.2f}" for a, b in points_xy)

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
    out_path.write_text(svg)


def reuse_tp3_allreduce_scale_L(repo: Path) -> tuple[float, float]:
    real = read_real(repo / "real_tp3_time.csv")
    sim = read_sim(repo / "sim_tp3.csv")
    pts = join_points(real, sim)
    if len(pts) != len(NCCL_SIZES):
        raise RuntimeError(f"tp3: expected {len(NCCL_SIZES)} allreduce points, got {len(pts)}")
    scale = median_scale(pts)
    L = fit_L(pts, scale)
    return scale, L


def calibrate_link(repo: Path, link_name: str, mode: str) -> None:
    real_path = repo / f"real_pp_{link_name}_time.csv"
    sim_path = repo / f"sim_pp_{link_name}.csv"
    out_compare = repo / f"compare_pp_{link_name}.csv"
    out_plot = repo / f"calib_pp_{link_name}_time.svg"

    real = read_real(real_path)
    sim = read_sim(sim_path)
    pts = join_points(real, sim)
    if len(pts) != len(NCCL_SIZES):
        raise RuntimeError(
            f"pp_{link_name}: expected {len(NCCL_SIZES)} points, got {len(pts)}"
        )

    if mode == "fit_pp":
        scale = median_scale(pts)
        L = fit_L(pts, scale)
        mode_label = "fit"
    elif mode == "reuse_tp3_allreduce":
        scale, L = reuse_tp3_allreduce_scale_L(repo)
        mode_label = "reused"
    else:
        raise ValueError(f"unknown mode: {mode}")

    write_compare(out_compare, pts, scale, L)
    plot_time(
        out_plot,
        pts,
        scale,
        L,
        title=f"P2P {link_name} ({mode_label} scale={scale:.3f}, L={L*1e6:.1f}us)",
    )
    print(f"pp {link_name}: {mode_label} scale={scale:.4f}, L={L*1e6:.1f}us -> {out_compare.name}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--mode",
        type=str,
        choices=["fit_pp", "reuse_tp3_allreduce"],
        default="fit_pp",
        help="how to get (scale, L): fit_pp (default) or reuse_tp3_allreduce",
    )
    args = ap.parse_args()

    repo = Path(__file__).resolve().parents[1]
    calibrate_link(repo, "01", args.mode)
    calibrate_link(repo, "12", args.mode)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

