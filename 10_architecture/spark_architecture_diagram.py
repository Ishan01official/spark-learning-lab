"""
Spark runtime architecture diagram -> PNG + SVG for README / docs.

Usage:
    pip install matplotlib
    python spark_architecture_diagram.py            # writes spark_architecture.png / .svg
    python spark_architecture_diagram.py --dark     # dark theme variant

Edit the BOXES / ARROWS / NOTES lists to change the diagram.
Coordinates are in a 1540 x 1270 canvas, origin top-left (like Excalidraw).
"""
import argparse

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

# ---------------------------------------------------------------- palette
LIGHT = dict(
    bg="#ffffff", text="#1e1e1e", muted="#5c636a",
    driver_zone="#e7edff", worker_zone="#f1f3f5", exec_zone="#e3f7e7",
    blue="#a5d8ff", purple="#d0bfff", teal="#c3fae8", orange="#ffd8a8",
    green="#b2f2bb", yellow="#fff3bf",
    s_blue="#4a9eed", s_purple="#8b5cf6", s_teal="#06b6d4", s_orange="#f59e0b",
    s_green="#22c55e", s_red="#ef4444", s_grey="#868e96",
)
DARK = dict(
    bg="#1e1e2e", text="#e5e5e5", muted="#a0a0a0",
    driver_zone="#1f2a44", worker_zone="#2a2a3a", exec_zone="#1b3326",
    blue="#1e3a5f", purple="#2d1b69", teal="#1a4d4d", orange="#5c3d1a",
    green="#1a4d2e", yellow="#4d4419",
    s_blue="#4a9eed", s_purple="#a78bfa", s_teal="#22d3ee", s_orange="#f59e0b",
    s_green="#22c55e", s_red="#f87171", s_grey="#6b7280",
)

W, H = 1540, 1240


def build(c):
    """Return (zones, boxes, arrows, notes) using palette c."""
    zones = [  # x, y, w, h, fill, stroke, title, title_color
        (0, 90, 1540, 240, c["driver_zone"], c["s_blue"], "DRIVER NODE  (1 process - the brain)", c["s_blue"]),
        (20, 550, 740, 270, c["worker_zone"], c["s_grey"], "Worker node 1", c["muted"]),
        (800, 550, 740, 270, c["worker_zone"], c["s_grey"], "Worker node 2", c["muted"]),
        (40, 590, 700, 210, c["exec_zone"], c["s_green"], "Executor 1 (JVM) - runs tasks, holds data", c["s_green"]),
        (820, 590, 700, 210, c["exec_zone"], c["s_green"], "Executor 2 (JVM) - runs tasks, holds data", c["s_green"]),
    ]

    boxes = [  # x, y, w, h, fill, stroke, text, fontsize
        (30, 145, 330, 150, c["blue"], c["s_blue"],
         "SparkSession / SparkContext\nentry point for your code;\nbuilds the logical plan\n(lazy until an action)", 11),
        (400, 145, 330, 150, c["purple"], c["s_purple"],
         "DAG Scheduler\naction -> job -> STAGES,\ncut at every shuffle\n(wide dependency)", 11),
        (770, 145, 330, 150, c["purple"], c["s_purple"],
         "Task Scheduler\nstage -> 1 TASK per partition,\nplaces tasks near data,\nretries failed tasks", 11),
        (1170, 145, 340, 150, c["teal"], c["s_teal"],
         "BlockManager Master\ncatalogue of where every\ncached / shuffle block lives", 11),
        (30, 360, 380, 110, c["orange"], c["s_orange"],
         "Cluster Manager\nYARN | Kubernetes | Standalone |\nDatabricks / EMR / Glue", 11),
        (520, 900, 500, 90, c["teal"], c["s_teal"], "Storage\nS3 | HDFS | ADLS | GCS", 12),
    ]
    # executor internals (same layout on both executors)
    for ox in (0, 780):
        boxes += [
            (60 + ox, 635, 200, 65, c["teal"], c["s_teal"], "BlockManager", 10.5),
            (60 + ox, 715, 200, 65, c["yellow"], c["s_orange"], "Cache\n(memory / disk)", 10.5),
            (290 + ox, 635, 210, 65, c["green"], c["s_green"], "Task slot 1 (core)", 10.5),
            (515 + ox, 635, 210, 65, c["green"], c["s_green"], "Task slot 2 (core)", 10.5),
            (290 + ox, 715, 210, 65, c["green"], c["s_green"], "Task slot 3 (core)", 10.5),
            (515 + ox, 715, 210, 65, c["green"], c["s_green"], "Task slot 4 (core)", 10.5),
        ]

    arrows = [  # list of points, color, dashed, two_way
        ([(360, 220), (400, 220)], c["text"], False, False),
        ([(730, 220), (770, 220)], c["text"], False, False),
        ([(850, 295), (410, 385)], c["s_orange"], False, False),            # 1 request
        ([(220, 470), (220, 550)], c["s_orange"], False, False),            # 2 launch
        ([(410, 440), (1170, 440), (1170, 550)], c["s_orange"], False, False),
        ([(940, 295), (520, 550)], c["s_purple"], True, False),             # 3 ship tasks
        ([(1020, 295), (1020, 550)], c["s_purple"], True, False),
        ([(1420, 295), (1420, 588)], c["s_teal"], True, False),  # tracks blocks
        ([(760, 760), (800, 760)], c["s_red"], False, True),                 # 5 shuffle
        ([(390, 820), (590, 900)], c["s_teal"], False, True),                # 4 read/write
        ([(1170, 820), (950, 900)], c["s_teal"], False, True),
    ]

    notes = [  # x, y, text, color, size, weight
        (770, 22, "Apache Spark - Runtime Architecture", c["text"], 20, "bold"),
        (770, 60, "one Driver plans the work, many Executors do it, a Cluster Manager hands out machines",
         c["muted"], 12, "normal"),
        (560, 345, "1. requests executors", c["s_orange"], 11, "bold"),
        (290, 510, "2. launches", c["s_orange"], 11, "bold"),
        (760, 427, "2. launches", c["s_orange"], 11, "bold"),
        (1095, 365, "3. ships tasks\n+ code / jars", c["s_purple"], 11, "bold"),
        (1330, 470, "tracks blocks\n(in every\nexecutor's\nBlockManager)", c["s_teal"], 11, "bold"),
        (780, 850, "5. shuffle blocks\n(executor <-> executor)", c["s_red"], 11, "bold"),
        (370, 885, "4. read / write\npartitions", c["s_teal"], 11, "bold"),
    ]

    footer = (
        "HOW ONE JOB RUNS\n"
        "1. Your code calls an ACTION (count, write, collect) -> Driver builds a DAG and asks the Cluster Manager for executors.\n"
        "2. Cluster Manager LAUNCHES executor JVMs on worker nodes.\n"
        "3. DAG Scheduler splits the job into stages at shuffles; Task Scheduler sends 1 task per partition to free slots.\n"
        "4. Tasks read input partitions from storage, transform them, and write results / cache them.\n"
        "5. At a stage boundary executors exchange SHUFFLE blocks; BlockManager Master tracks where each block lives.\n"
        "Rule of thumb: parallelism = executors x cores; aim for ~2-4 partitions per core."
    )
    return zones, boxes, arrows, notes, footer


def rbox(ax, x, y, w, h, fill, stroke, lw=1.6, alpha=1.0, z=1):
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h, boxstyle="round,pad=0,rounding_size=12",
        facecolor=fill, edgecolor=stroke, linewidth=lw, alpha=alpha, zorder=z))


def draw(dark=False, out="spark_architecture"):
    c = DARK if dark else LIGHT
    zones, boxes, arrows, notes, footer = build(c)

    fig = plt.figure(figsize=(W / 100, H / 100), dpi=100)
    fig.patch.set_facecolor(c["bg"])
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(-10, W + 10)
    ax.set_ylim(H, -10)  # invert y: top-left origin
    ax.axis("off")

    for x, y, w, h, fill, stroke, title, tcol in zones:
        rbox(ax, x, y, w, h, fill, stroke, lw=1.2, z=0)
        ax.text(x + 20, y + 20, title, color=tcol, fontsize=12.5, weight="bold", va="center", zorder=1)

    for x, y, w, h, fill, stroke, text, fs in boxes:
        rbox(ax, x, y, w, h, fill, stroke, z=2)
        lines = text.split("\n")
        lh = fs * 2.0  # line height in canvas units
        y0 = y + h / 2 - (len(lines) - 1) * lh / 2
        for i, line in enumerate(lines):
            ax.text(x + w / 2, y0 + i * lh, line, ha="center", va="center", color=c["text"],
                    fontsize=fs + (1 if i == 0 and len(lines) > 1 else 0),
                    weight="bold" if i == 0 else "normal", zorder=3)

    for pts, col, dashed, two_way in arrows:
        ls = (0, (5, 4)) if dashed else "-"
        for i in range(len(pts) - 1):
            last = i == len(pts) - 2
            first = i == 0
            style = "-|>" if last and not (two_way and first) else "-"
            if two_way and first and last:
                style = "<|-|>"
            ax.add_patch(FancyArrowPatch(
                pts[i], pts[i + 1], arrowstyle=style, mutation_scale=16,
                color=col, linewidth=1.8, linestyle=ls, shrinkA=0, shrinkB=0, zorder=4))

    for x, y, text, col, fs, wt in notes:
        ax.text(x, y, text, ha="center", va="center", color=col, fontsize=fs, weight=wt,
                zorder=5, bbox=dict(facecolor=c["bg"], edgecolor="none", pad=1.5, alpha=0.85)
                if fs < 14 else None)

    rbox(ax, 0, 1030, 1540, 200, c["yellow"], c["s_orange"], z=2)
    head, body = footer.split("\n", 1)
    ax.text(25, 1058, head, color=c["text"], fontsize=13, weight="bold", va="center", zorder=3)
    ax.text(25, 1080, body, color=c["text"], fontsize=11, va="top", linespacing=1.6, zorder=3)

    suffix = "_dark" if dark else ""
    for ext in ("png", "svg"):
        fig.savefig(f"{out}{suffix}.{ext}", dpi=150 if ext == "png" else 100,
                    facecolor=c["bg"], bbox_inches="tight", pad_inches=0.2)
    plt.close(fig)
    print(f"wrote {out}{suffix}.png and {out}{suffix}.svg")


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--dark", action="store_true", help="dark theme")
    p.add_argument("--out", default="spark_architecture", help="output file name (no extension)")
    # parse_known_args: Databricks / Jupyter add their own args (e.g. "-f kernel.json"); ignore them
    a, _ = p.parse_known_args()
    draw(dark=a.dark, out=a.out)
