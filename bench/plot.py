"""Render results/benchmark.json as results/benchmark.png (two panels, one y-axis each)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # never open a window
import matplotlib.pyplot as plt  # noqa: E402

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"
SERIES = {"skip_locked": "#2a78d6", "naive": "#eb6834"}
LABELS = {"skip_locked": "FOR UPDATE SKIP LOCKED", "naive": "plain FOR UPDATE"}


def _style(ax, title: str, ylabel: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, loc="left", color=INK, fontsize=11)
    ax.set_xlabel("worker processes", color=INK_2)
    ax.set_ylabel(ylabel, color=INK_2)
    ax.set_xscale("log", base=2)
    ax.tick_params(colors=INK_2)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(GRID)


def plot(data: dict, out: Path) -> None:
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 4.2), facecolor=SURFACE)

    for mode, color in SERIES.items():
        rows = [r for r in data["throughput"] if r["mode"] == mode]
        xs = [r["workers"] for r in rows]
        ys = [r["jobs_per_second"] for r in rows]
        ax1.plot(xs, ys, color=color, linewidth=2, marker="o", markersize=6, label=LABELS[mode])
        ax1.annotate(
            f"{ys[-1]:.0f}",
            (xs[-1], ys[-1]),
            xytext=(6, 0),
            textcoords="offset points",
            va="center",
            color=INK_2,
            fontsize=9,
        )
    runs = data["config"]["repeats"]
    jobs = data["config"]["jobs"]
    _style(
        ax1,
        f"Throughput, no-op jobs\nmedian of {runs} runs, {jobs} jobs each",
        "jobs / second",
    )
    ax1.set_ylim(bottom=0)
    ax1.legend(frameon=False, labelcolor=INK_2, fontsize=9)

    rows = [r for r in data["latency"] if r["mode"] == "skip_locked"]
    xs = [r["workers"] for r in rows]
    for key, color in (("p50_ms", "#2a78d6"), ("p95_ms", "#eb6834"), ("p99_ms", "#1baf7a")):
        ys = [r[key] for r in rows]
        ax2.plot(
            xs,
            ys,
            color=color,
            linewidth=2,
            marker="o",
            markersize=6,
            label=key.removesuffix("_ms"),
        )
    rate = rows[0]["offered_rate_per_second"] if rows else 0
    poll = data["config"]["latency_poll"]
    _style(
        ax2,
        f"Enqueue-to-start latency, SKIP LOCKED\n"
        f"{rate:.0f} jobs/s offered, {poll * 1000:.0f} ms idle poll",
        "milliseconds",
    )
    ax2.set_ylim(bottom=0)
    ax2.legend(frameon=False, labelcolor=INK_2, fontsize=9)

    for ax in (ax1, ax2):
        ax.set_xticks(xs, [str(x) for x in xs])
    fig.tight_layout()
    fig.savefig(out, dpi=130, facecolor=SURFACE)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=Path, default=Path("results/benchmark.json"))
    parser.add_argument("--out", type=Path, default=Path("results/benchmark.png"))
    args = parser.parse_args()
    plot(json.loads(args.data.read_text(encoding="utf-8")), args.out)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
