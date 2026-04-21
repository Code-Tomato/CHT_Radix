#!/usr/bin/env python3
"""Plot benchmark CSVs: hash-table + trace sweeps and radix shared-prefix sweeps.

  python3 scripts/plot.py hash --csv results/experiments_canonical.csv --out results
  python3 scripts/plot.py radix --csv results/radix_sweep_<TS>.csv --out results
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

# --- shared -----------------------------------------------------------------


def save_fig(fig, out_dir: Path, stem: str) -> None:
  fig.tight_layout()
  fig.savefig(out_dir / f"{stem}.pdf")
  plt.close(fig)


def load_csv(csv_path: Path) -> pd.DataFrame:
  df = pd.read_csv(csv_path)
  df = df[df["table"] != "stub"].copy()
  df["threads"] = df["threads"].astype(int)
  if "read_frac_config" in df.columns:
    df["read_frac"] = df["read_frac_config"].astype(float)
  else:
    df["read_frac"] = df["read_frac"].astype(float)
  return df


# --- experiments (hash-table + general sweep) --------------------------------

TABLE_COLORS = {
    "chaining_coarse": "tab:blue",
    "chaining_fine": "tab:orange",
    "cuckoo_optimistic": "tab:green",
    "cuckoo_striped": "tab:red",
    "hopscotch": "tab:purple",
}

TABLE_ORDER = [
    "chaining_coarse",
    "chaining_fine",
    "cuckoo_optimistic",
    "cuckoo_striped",
    "hopscotch",
]
THREADS_ORDER = [1, 2, 4, 8, 16]
READ_FRAC_ORDER = [0.1, 0.5, 1.0]


def aggregate_experiments(df: pd.DataFrame, keys):
  extra = [
      "tp_median", "tp_q25", "tp_q75", "tp_min", "tp_max",
      "p99_median", "p99_q25", "p99_q75", "p999_median",
  ]
  if df.empty:
    return pd.DataFrame(columns=list(keys) + extra)
  rows = []
  for key_tuple, sub in df.groupby(keys):
    if not isinstance(key_tuple, tuple):
      key_tuple = (key_tuple,)
    row = {k: v for k, v in zip(keys, key_tuple)}
    row["tp_median"] = sub["throughput_mops"].median()
    row["tp_q25"] = sub["throughput_mops"].quantile(0.25)
    row["tp_q75"] = sub["throughput_mops"].quantile(0.75)
    row["tp_min"] = sub["throughput_mops"].min()
    row["tp_max"] = sub["throughput_mops"].max()
    row["p99_median"] = sub["p99_ns"].median()
    row["p99_q25"] = sub["p99_ns"].quantile(0.25)
    row["p99_q75"] = sub["p99_ns"].quantile(0.75)
    if "p999_ns" in sub.columns:
      row["p999_median"] = sub["p999_ns"].median()
    else:
      row["p999_median"] = sub["p99_ns"].median()
    rows.append(row)
  return pd.DataFrame(rows)


def plot_throughput_vs_threads(df: pd.DataFrame, out_dir: Path):
  fig, axes = plt.subplots(1, 3, figsize=(14, 4.2), sharey=True)
  workloads = ["uniform", "zipfian", "trace"]
  for ax, workload in zip(axes, workloads):
    subset = df[df["workload"] == workload]
    if workload in ("uniform", "zipfian"):
      subset = subset[subset["read_frac"].between(0.9, 1.0)]
    if workload == "uniform":
      subset = subset[subset["key_space"] >= 1_000_000]
    agg = aggregate_experiments(subset, ["table", "threads"])
    for table in TABLE_ORDER:
      td = agg[agg["table"] == table].sort_values("threads")
      if td.empty:
        continue
      color = TABLE_COLORS.get(table)
      ax.plot(td["threads"], td["tp_median"], marker="o", color=color, label=table)
      ax.fill_between(td["threads"], td["tp_q25"], td["tp_q75"], color=color, alpha=0.15)
    ax.set_title(workload)
    ax.set_xlabel("threads")
    ax.set_xscale("log", base=2)
    ax.set_xticks(THREADS_ORDER)
    ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    ax.grid(True, alpha=0.3)
  axes[0].set_ylabel("throughput (Mops/s)")
  leg_handles, _ = axes[2].get_legend_handles_labels()
  if leg_handles:
    axes[2].legend(loc="best", fontsize=8,
                   title="shaded band = q25-q75 of 3 repeats")
  save_fig(fig, out_dir, "throughput_vs_threads")


def plot_p99_vs_threads(df: pd.DataFrame, out_dir: Path):
  fig, axes = plt.subplots(1, 3, figsize=(14, 4.2), sharey=True)
  workloads = ["uniform", "zipfian", "trace"]
  for ax, workload in zip(axes, workloads):
    subset = df[df["workload"] == workload]
    if workload in ("uniform", "zipfian"):
      subset = subset[subset["read_frac"].between(0.9, 1.0)]
    if workload == "uniform":
      subset = subset[subset["key_space"] >= 1_000_000]
    agg = aggregate_experiments(subset, ["table", "threads"])
    for table in TABLE_ORDER:
      td = agg[agg["table"] == table].sort_values("threads")
      if td.empty:
        continue
      color = TABLE_COLORS.get(table)
      ax.plot(td["threads"], td["p99_median"] / 1000.0,
              marker="o", color=color, label=table)
      ax.fill_between(td["threads"],
                      td["p99_q25"] / 1000.0,
                      td["p99_q75"] / 1000.0,
                      color=color, alpha=0.15)
    ax.set_title(workload)
    ax.set_xlabel("threads")
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_xticks(THREADS_ORDER)
    ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
    ax.grid(True, which="both", alpha=0.3)
  axes[0].set_ylabel("p99 latency (us, log)")
  leg_handles, _ = axes[2].get_legend_handles_labels()
  if leg_handles:
    axes[2].legend(loc="best", fontsize=8,
                   title="shaded band = q25-q75 of 3 repeats")
  save_fig(fig, out_dir, "p99_vs_threads")


def plot_throughput_vs_read_fraction(df: pd.DataFrame, out_dir: Path):
  subset = df[(df["workload"] == "uniform") & (df["threads"] == 8) &
              (df["key_space"] >= 1_000_000)]
  agg = aggregate_experiments(subset, ["table", "read_frac"])

  fig, ax = plt.subplots(figsize=(7, 4.2))
  for table in TABLE_ORDER:
    td = agg[agg["table"] == table].copy()
    td["read_frac_cat"] = pd.Categorical(
        td["read_frac"], categories=READ_FRAC_ORDER, ordered=True)
    td = td.sort_values("read_frac_cat")
    if td.empty:
      continue
    color = TABLE_COLORS.get(table)
    ax.plot(td["read_frac"], td["tp_median"], marker="o",
            color=color, label=table)
    ax.errorbar(td["read_frac"], td["tp_median"],
                yerr=[td["tp_median"] - td["tp_min"],
                      td["tp_max"] - td["tp_median"]],
                fmt="none", ecolor=color, alpha=0.5, capsize=3)
  ax.set_xlabel("read fraction")
  ax.set_ylabel("throughput (Mops/s)")
  ax.set_xticks(READ_FRAC_ORDER)
  ax.grid(True, alpha=0.3)
  ax.legend(loc="best", fontsize=8,
            title="whiskers = [min, max] over 3 repeats")
  save_fig(fig, out_dir, "throughput_vs_read_fraction")


def plot_throughput_cache_resident(df: pd.DataFrame, out_dir: Path):
  subset = df[(df["workload"] == "uniform") & (df["threads"] == 8) &
              (df["read_frac"].between(0.9, 1.0))].copy()
  if subset.empty:
    print("No data for cache-resident plot; skipping.")
    return

  subset["regime"] = subset["key_space"].apply(
      lambda k: "100K keys" if k <= 500_000 else "10M keys")
  agg = aggregate_experiments(subset, ["table", "regime"])

  tables = [t for t in TABLE_ORDER if t in agg["table"].unique()]
  if not tables:
    return
  regimes = ["100K keys", "10M keys"]
  x = list(range(len(tables)))
  width = 0.35

  fig, ax = plt.subplots(figsize=(8.0, 4.4))
  for i, regime in enumerate(regimes):
    heights = []
    mins = []
    maxs = []
    for t in tables:
      row = agg[(agg["table"] == t) & (agg["regime"] == regime)]
      if row.empty:
        heights.append(0.0)
        mins.append(0.0)
        maxs.append(0.0)
        continue
      heights.append(float(row["tp_median"].iloc[0]))
      mins.append(float(row["tp_min"].iloc[0]))
      maxs.append(float(row["tp_max"].iloc[0]))
    offset = (i - 0.5) * width
    ax.bar([xi + offset for xi in x], heights, width, label=regime)
    for xi, h, lo, hi in zip(x, heights, mins, maxs):
      ax.plot([xi + offset, xi + offset], [lo, hi], color="black", alpha=0.5)
  ax.set_xticks(x)
  ax.set_xticklabels(tables, rotation=15, ha="right")
  ax.set_ylabel("throughput (Mops/s)")
  ax.set_title("cache regime: key_space 100K vs 10M (uniform, 8 threads, 95% read)")
  ax.grid(True, axis="y", alpha=0.3)
  ax.legend(loc="best", fontsize=8)
  save_fig(fig, out_dir, "throughput_cache_resident")


def cmd_hash(args: argparse.Namespace) -> int:
  args.out.mkdir(parents=True, exist_ok=True)
  df = load_csv(args.csv)
  plot_throughput_vs_threads(df, args.out)
  plot_p99_vs_threads(df, args.out)
  plot_throughput_vs_read_fraction(df, args.out)
  plot_throughput_cache_resident(df, args.out)
  print(f"Wrote plots into {args.out}")
  return 0


# --- radix (shared-prefix sweep) ---------------------------------------------

RADIX_TABLE_ORDER = ["cuckoo_optimistic", "chaining_fine",
                     "radix_tree_coarse", "radix_tree_fine"]
RADIX_TABLE_COLORS = {
    "cuckoo_optimistic": "tab:green",
    "chaining_fine": "tab:orange",
    "radix_tree_coarse": "tab:brown",
    "radix_tree_fine": "tab:red",
}
PREFIX_COUNTS = [10, 100, 1000, 10000]
UNIVERSE = 100_000


def shared_prefix_df(df: pd.DataFrame) -> pd.DataFrame:
  sp = df[df["workload"] == "shared-prefix"].copy()
  sp["prefix_count"] = (UNIVERSE // sp["key_space"]).astype(int)
  return sp


def aggregate_radix(df: pd.DataFrame, keys):
  if df.empty:
    return pd.DataFrame(columns=list(keys) + ["tp_median", "tp_min", "tp_max"])
  rows = []
  for k, sub in df.groupby(keys):
    if not isinstance(k, tuple):
      k = (k,)
    r = {kk: vv for kk, vv in zip(keys, k)}
    r["tp_median"] = sub["throughput_mops"].median()
    r["tp_min"] = sub["throughput_mops"].min()
    r["tp_max"] = sub["throughput_mops"].max()
    rows.append(r)
  return pd.DataFrame(rows)


def plot_radix_tp_vs_prefix_count(df: pd.DataFrame, out_dir: Path):
  sp = shared_prefix_df(df)
  sp = sp[sp["threads"] == 8]
  if sp.empty:
    print("No data for radix throughput vs prefix_count (need shared-prefix rows at 8 threads); skipping.")
    return
  agg = aggregate_radix(sp, ["table", "prefix_count"])
  fig, ax = plt.subplots(figsize=(7.5, 4.5))
  for t in RADIX_TABLE_ORDER:
    td = agg[agg["table"] == t].sort_values("prefix_count")
    if td.empty:
      continue
    c = RADIX_TABLE_COLORS.get(t)
    ax.plot(td["prefix_count"], td["tp_median"], marker="o", color=c, label=t)
    low = (td["tp_median"] - td["tp_min"]).to_numpy(dtype=float)
    high = (td["tp_max"] - td["tp_median"]).to_numpy(dtype=float)
    ax.errorbar(td["prefix_count"], td["tp_median"],
                yerr=[low, high],
                fmt="none", ecolor=c, alpha=0.4, capsize=3)
  ax.set_xscale("log")
  ax.set_xticks(PREFIX_COUNTS)
  ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())
  ax.set_xlabel("prefix_count (fewer = more shared prefix)")
  ax.set_ylabel("throughput (Mops/s) at 8 threads, 95% read")
  ax.grid(True, alpha=0.3)
  ax.legend(loc="best", fontsize=9,
            title="whiskers = [min, max] over 3 repeats")
  save_fig(fig, out_dir, "radix_throughput_vs_prefix_count")


def plot_radix_scaling_1t_8t(df: pd.DataFrame, out_dir: Path):
  sp = shared_prefix_df(df)
  sp = sp[sp["prefix_count"] == 100]
  agg = aggregate_radix(sp, ["table", "threads"])
  tables = [t for t in RADIX_TABLE_ORDER if t in agg["table"].unique()]
  if not tables:
    print("No data for radix 1T vs 8T plot (need shared-prefix rows at prefix_count=100); skipping.")
    return
  x = list(range(len(tables)))
  width = 0.35
  fig, ax = plt.subplots(figsize=(8, 4.5))
  for i, n in enumerate([1, 8]):
    heights, mins, maxs = [], [], []
    for t in tables:
      r = agg[(agg["table"] == t) & (agg["threads"] == n)]
      if r.empty:
        heights.append(0.0)
        mins.append(0.0)
        maxs.append(0.0)
      else:
        heights.append(float(r["tp_median"].iloc[0]))
        mins.append(float(r["tp_min"].iloc[0]))
        maxs.append(float(r["tp_max"].iloc[0]))
    offset = (i - 0.5) * width
    ax.bar([xi + offset for xi in x], heights, width, label=f"{n} thread(s)")
    for xi, h, lo, hi in zip(x, heights, mins, maxs):
      ax.plot([xi + offset, xi + offset], [lo, hi], color="black", alpha=0.5)
  ax.set_xticks(x)
  ax.set_xticklabels(tables, rotation=15, ha="right")
  ax.set_ylabel("throughput (Mops/s)")
  ax.set_title("1T vs 8T at prefix_count=100, 95% read, shared-prefix")
  ax.grid(True, axis="y", alpha=0.3)
  ax.legend(loc="best", fontsize=9)
  save_fig(fig, out_dir, "radix_scaling_1t_8t")


def cmd_radix(args: argparse.Namespace) -> int:
  args.out.mkdir(parents=True, exist_ok=True)
  df = load_csv(args.csv)
  plot_radix_tp_vs_prefix_count(df, args.out)
  plot_radix_scaling_1t_8t(df, args.out)
  print(f"Wrote plots to {args.out}")
  return 0


def main() -> int:
  parser = argparse.ArgumentParser(description=__doc__)
  sub = parser.add_subparsers(dest="cmd", required=True)

  p_hash = sub.add_parser("hash", help="Hash-table + trace sweep figures (4 PDFs).")
  p_hash.add_argument("--csv", type=Path, required=True)
  p_hash.add_argument("--out", type=Path, default=Path("results"))
  p_hash.set_defaults(func=cmd_hash)

  p_rx = sub.add_parser("radix", help="Radix shared-prefix sweep figures (2 PDFs).")
  p_rx.add_argument("--csv", type=Path, required=True)
  p_rx.add_argument("--out", type=Path, default=Path("results"))
  p_rx.set_defaults(func=cmd_radix)

  args = parser.parse_args()
  return args.func(args)


if __name__ == "__main__":
  raise SystemExit(main())
