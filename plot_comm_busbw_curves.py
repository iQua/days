#!/usr/bin/env python3
"""
Plot paper-ready communication bus bandwidth curves (SimAI Fig.6/7 style).

Bus bandwidth definition (STRICT, aligned to nccl-tests):
  - busbw_real_GBs is taken directly from nccl-tests CSV: `busbw_GBs_mean`
  - busbw_pred_GBs is derived from completion times:
      busbw_pred = busbw_real * (t_real / t_pred)
    where t_real and t_pred are in seconds (from Days holdout compare CSVs).

Inputs (repo root):
  - nccl-tests CSVs (real busbw):
      - RingAllReduce: real_tp2_time.csv / real_tp3_time.csv
      - Broadcast:     real_bcast_tp2_time.csv / real_bcast_tp3_time.csv
      - P2P:           real_pp_01_time.csv / real_pp_12_time.csv

  - Days holdout compare CSVs (t_real,t_pred in seconds):
      - RingAllReduce holdout CSVs (auto-detected):
          - TP2: glob 'compare_*allreduce*tp2*holdout*.csv'
          - TP3: glob 'compare_*allreduce*tp3*holdout*.csv'
  - Broadcast:
      - compare_bcast_tp2_holdout.csv
      - compare_bcast_tp3_holdout.csv
  - P2P:
      - compare_pp_01_holdout.csv
      - compare_pp_12_holdout.csv

Each holdout compare CSV must contain at least: size_bytes, t_real, t_pred.

Outputs (repo root):
  - fig_comm_busbw_ringallreduce.png / .pdf
  - fig_comm_busbw_broadcast.png / .pdf
  - fig_comm_busbw_p2p.png / .pdf
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


def load_nccl_real(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing nccl-tests CSV: {path}")
    df = pd.read_csv(path)
    needed = {"size_bytes", "time_us_mean", "busbw_GBs_mean"}
    missing = sorted(list(needed - set(df.columns)))
    if missing:
        raise ValueError(f"{path.name}: missing required columns: {missing}")
    df = df[["size_bytes", "time_us_mean", "busbw_GBs_mean"]].copy()
    df["size_bytes"] = pd.to_numeric(df["size_bytes"], errors="raise")
    df["busbw_GBs_mean"] = pd.to_numeric(df["busbw_GBs_mean"], errors="raise")
    df = df[df["size_bytes"] > 0].sort_values("size_bytes")
    return df


def merge_busbw(
    compare_df: pd.DataFrame, nccl_df: pd.DataFrame
) -> pd.DataFrame:
    # Join on size_bytes (exact match).
    out = compare_df.merge(nccl_df[["size_bytes", "busbw_GBs_mean"]], on="size_bytes", how="inner")
    if len(out) != len(compare_df):
        missing = sorted(set(compare_df["size_bytes"]) - set(out["size_bytes"]))
        raise RuntimeError(
            "Size mismatch between compare CSV and nccl-tests CSV.\n"
            f"Missing sizes in nccl-tests busbw: {missing}"
        )

    out = out.copy()
    out["size_mb"] = out["size_bytes"] / 1e6
    out["busbw_real"] = out["busbw_GBs_mean"]
    out["busbw_pred"] = out["busbw_real"] * (out["t_real"] / out["t_pred"])
    out = out.sort_values("size_bytes")
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


def save_png_pdf(fig, base_name: str) -> None:
    out_png = REPO / f"{base_name}.png"
    out_pdf = REPO / f"{base_name}.pdf"
    fig.tight_layout()
    fig.savefig(out_png, format="png")
    fig.savefig(out_pdf, format="pdf")
    print(f"wrote {out_png}")
    print(f"wrote {out_pdf}")


def main() -> int:
    # RingAllReduce / AllReduce (auto-detect compare, required)
    allreduce_tp2_path = find_single("compare_*allreduce*tp2*holdout*.csv")
    allreduce_tp3_path = find_single("compare_*allreduce*tp3*holdout*.csv")

    allreduce_tp2_cmp = load_compare(allreduce_tp2_path)
    allreduce_tp3_cmp = load_compare(allreduce_tp3_path)
    allreduce_tp2_nccl = load_nccl_real(REPO / "real_tp2_time.csv")
    allreduce_tp3_nccl = load_nccl_real(REPO / "real_tp3_time.csv")
    allreduce_tp2 = merge_busbw(allreduce_tp2_cmp, allreduce_tp2_nccl)
    allreduce_tp3 = merge_busbw(allreduce_tp3_cmp, allreduce_tp3_nccl)

    # Broadcast
    bcast_tp2_cmp = load_compare(REPO / "compare_bcast_tp2_holdout.csv")
    bcast_tp3_cmp = load_compare(REPO / "compare_bcast_tp3_holdout.csv")
    bcast_tp2_nccl = load_nccl_real(REPO / "real_bcast_tp2_time.csv")
    bcast_tp3_nccl = load_nccl_real(REPO / "real_bcast_tp3_time.csv")
    bcast_tp2 = merge_busbw(bcast_tp2_cmp, bcast_tp2_nccl)
    bcast_tp3 = merge_busbw(bcast_tp3_cmp, bcast_tp3_nccl)

    # P2P
    pp_01_cmp = load_compare(REPO / "compare_pp_01_holdout.csv")
    pp_12_cmp = load_compare(REPO / "compare_pp_12_holdout.csv")
    pp_01_nccl = load_nccl_real(REPO / "real_pp_01_time.csv")
    pp_12_nccl = load_nccl_real(REPO / "real_pp_12_time.csv")
    pp_01 = merge_busbw(pp_01_cmp, pp_01_nccl)
    pp_12 = merge_busbw(pp_12_cmp, pp_12_nccl)

    # A) RingAllReduce
    fig, ax = plt.subplots(figsize=(7.2, 4.2), dpi=140)
    plot_pair(ax, allreduce_tp2, "TP2", color="#1f77b4")
    plot_pair(ax, allreduce_tp3, "TP3", color="#ff7f0e")
    style_axes(ax)
    save_png_pdf(fig, "fig_comm_busbw_ringallreduce")
    plt.close(fig)

    # B) Broadcast
    fig, ax = plt.subplots(figsize=(7.2, 4.2), dpi=140)
    plot_pair(ax, bcast_tp2, "TP2", color="#1f77b4")
    plot_pair(ax, bcast_tp3, "TP3", color="#ff7f0e")
    style_axes(ax)
    save_png_pdf(fig, "fig_comm_busbw_broadcast")
    plt.close(fig)

    # C) P2P (pipeline links)
    fig, ax = plt.subplots(figsize=(7.2, 4.2), dpi=140)
    plot_pair(ax, pp_01, "0→1", color="#1f77b4")
    plot_pair(ax, pp_12, "1→2", color="#ff7f0e")
    style_axes(ax)
    save_png_pdf(fig, "fig_comm_busbw_p2p")
    plt.close(fig)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

