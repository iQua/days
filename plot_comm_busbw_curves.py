#!/usr/bin/env python3
"""
Plot paper-ready communication bus bandwidth curves (SimAI Fig.6/7 style).

Bus bandwidth definition (uniform across all primitives):
  busbw_GBs = size_bytes / completion_time_seconds / 1e9

We compute:
  - busbw_real from t_real
  - busbw_pred from t_pred

This is a simple "message bytes / completion time" throughput definition to match
the curve shape in SimAI Fig.6/7, and intentionally does NOT rely on nccl-tests'
reported busbw to avoid algorithm-factor differences across primitives.

Inputs (repo root):
  - AllReduce holdout CSVs (auto-detected):
      - TP2: glob 'compare_*allreduce*tp2*holdout*.csv'
      - TP3: glob 'compare_*allreduce*tp3*holdout*.csv'
  - Broadcast:
      - compare_bcast_tp2_holdout.csv
      - compare_bcast_tp3_holdout.csv
  - P2P:
      - compare_pp_01_holdout.csv
      - compare_pp_12_holdout.csv

Each CSV must contain at least: size_bytes, t_real, t_pred.

Outputs (repo root):
  - fig_comm_busbw_ringallreduce.svg
  - fig_comm_busbw_broadcast.svg
  - fig_comm_busbw_p2p.svg
"""

from __future__ import annotations

import glob
import os
from pathlib import Path

# Force a non-interactive backend (helps stability in headless/sandboxed runs).
import matplotlib

matplotlib.use("Agg", force=True)

import matplotlib.pyplot as plt
import pandas as pd


REPO = Path(__file__).resolve().parent


def find_single(pattern: str) -> Path:
    matches = sorted(glob.glob(str(REPO / pattern)))
    if not matches:
        raise FileNotFoundError(
            f"Missing required AllReduce holdout CSV. Tried pattern `{pattern}` in repo root.\n"
            f"Hint: expected something like `compare_allreduce_tp2_holdout.csv`.\n"
            f"Repo root: {REPO}"
        )
    if len(matches) > 1:
        raise RuntimeError(
            f"Ambiguous matches for pattern `{pattern}`:\n"
            + "\n".join([f"- {Path(m).name}" for m in matches])
            + "\nPlease keep only one matching file or rename extras."
        )
    return Path(matches[0])


def load_compare(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing input CSV: {path}")
    df = pd.read_csv(path)
    needed = {"size_bytes", "t_real", "t_pred"}
    missing = sorted(list(needed - set(df.columns)))
    if missing:
        raise ValueError(f"{path.name}: missing required columns: {missing}")
    df = df[["size_bytes", "t_real", "t_pred"]].copy()
    df["size_bytes"] = pd.to_numeric(df["size_bytes"], errors="raise")
    df["t_real"] = pd.to_numeric(df["t_real"], errors="raise")
    df["t_pred"] = pd.to_numeric(df["t_pred"], errors="raise")

    # Filter invalid rows (shouldn't exist, but keep robust).
    df = df[(df["size_bytes"] > 0) & (df["t_real"] > 0) & (df["t_pred"] > 0)]
    df = df.sort_values("size_bytes")
    return df


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["size_mb"] = out["size_bytes"] / 1e6
    out["busbw_real"] = out["size_bytes"] / out["t_real"] / 1e9
    out["busbw_pred"] = out["size_bytes"] / out["t_pred"] / 1e9
    return out


def plot_pair(ax, df: pd.DataFrame, label: str, color: str) -> None:
    ax.plot(
        df["size_mb"],
        df["busbw_real"],
        linestyle="-",
        marker="o",
        markersize=5,
        linewidth=2,
        color=color,
        label=f"{label} (real)",
    )
    ax.plot(
        df["size_mb"],
        df["busbw_pred"],
        linestyle="--",
        marker="x",
        markersize=5,
        linewidth=2,
        color=color,
        label=f"{label} (days+calib)",
    )


def style_axes(ax) -> None:
    ax.set_xscale("log")
    ax.set_xlabel("Message size (MB)")
    ax.set_ylabel("Bus bandwidth (GB/s)")
    ax.grid(True, which="both", linestyle="--", linewidth=0.5, alpha=0.4)
    ax.legend(frameon=False)


def save_svg(fig, out_name: str) -> None:
    out_path = REPO / out_name
    fig.tight_layout()
    fig.savefig(out_path, format="svg")
    print(f"wrote {out_path}")


def main() -> int:
    # AllReduce (auto-detect, required)
    allreduce_tp2_path = find_single("compare_*allreduce*tp2*holdout*.csv")
    allreduce_tp3_path = find_single("compare_*allreduce*tp3*holdout*.csv")

    allreduce_tp2 = enrich(load_compare(allreduce_tp2_path))
    allreduce_tp3 = enrich(load_compare(allreduce_tp3_path))

    # Broadcast
    bcast_tp2 = enrich(load_compare(REPO / "compare_bcast_tp2_holdout.csv"))
    bcast_tp3 = enrich(load_compare(REPO / "compare_bcast_tp3_holdout.csv"))

    # P2P
    pp_01 = enrich(load_compare(REPO / "compare_pp_01_holdout.csv"))
    pp_12 = enrich(load_compare(REPO / "compare_pp_12_holdout.csv"))

    # A) RingAllReduce
    fig, ax = plt.subplots(figsize=(7.2, 4.2), dpi=140)
    plot_pair(ax, allreduce_tp2, "TP2", color="#1f77b4")
    plot_pair(ax, allreduce_tp3, "TP3", color="#ff7f0e")
    style_axes(ax)
    save_svg(fig, "fig_comm_busbw_ringallreduce.svg")
    plt.close(fig)

    # B) Broadcast
    fig, ax = plt.subplots(figsize=(7.2, 4.2), dpi=140)
    plot_pair(ax, bcast_tp2, "TP2", color="#1f77b4")
    plot_pair(ax, bcast_tp3, "TP3", color="#ff7f0e")
    style_axes(ax)
    save_svg(fig, "fig_comm_busbw_broadcast.svg")
    plt.close(fig)

    # C) P2P (pipeline links)
    fig, ax = plt.subplots(figsize=(7.2, 4.2), dpi=140)
    plot_pair(ax, pp_01, "0→1", color="#1f77b4")
    plot_pair(ax, pp_12, "1→2", color="#ff7f0e")
    style_axes(ax)
    save_svg(fig, "fig_comm_busbw_p2p.svg")
    plt.close(fig)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

