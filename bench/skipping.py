"""How much does file layout change what a query has to read?

Writes 2M rows as 200 files in arrival order (uniform random x and y), then
rewrites the table twice: sorted by x, then Z-ordered on (x, y). All three
layouts stay readable through time travel, so the same queries run against
each version.

    python bench/skipping.py [--rows 2000000] [--files 200] [--queries 50]
"""

import argparse
import os
import shutil
import statistics
import time

import numpy as np
import pyarrow as pa

from tinylake import Table

DOMAIN = 1_000_000


def query_box(rng, cols, width):
    where = []
    for c in cols:
        lo = int(rng.integers(0, DOMAIN - width))
        where += [(c, ">=", lo), (c, "<", lo + width)]
    return where


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=2_000_000)
    ap.add_argument("--files", type=int, default=200)
    ap.add_argument("--queries", type=int, default=50)
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()

    root = os.path.join(os.path.dirname(__file__), "data", "events")
    shutil.rmtree(root, ignore_errors=True)
    rng = np.random.default_rng(args.seed)
    table = Table.create(root, pa.schema([("x", "int64"), ("y", "int64"), ("v", "double")]))
    data = pa.table({
        "x": rng.integers(0, DOMAIN, args.rows),
        "y": rng.integers(0, DOMAIN, args.rows),
        "v": rng.random(args.rows),
    })
    per_file = args.rows // args.files

    t0 = time.perf_counter()
    layouts = {"arrival order": table.append(data, rows_per_file=per_file)}
    t1 = time.perf_counter()
    layouts["sorted by x"] = table.optimize(target_rows=per_file, zorder_by=["x"])
    t2 = time.perf_counter()
    layouts["Z-order (x, y)"] = table.optimize(target_rows=per_file, zorder_by=["x", "y"])
    t3 = time.perf_counter()
    print(f"{args.rows:,} rows in {args.files} files; write {t1 - t0:.1f}s, "
          f"sort {t2 - t1:.1f}s, z-order {t3 - t2:.1f}s\n")

    width = DOMAIN // 100  # each range covers 1% of its column
    shapes = {
        "x in 1% range": ["x"],
        "y in 1% range": ["y"],
        "x and y in 1% ranges": ["x", "y"],
    }
    print("| Layout | Query | Files read (mean) | Rows returned (mean) | Scan ms (median) |")
    print("|---|---|---|---|---|")
    for name, version in layouts.items():
        for label, cols in shapes.items():
            qrng = np.random.default_rng(args.seed + 1)
            reads, hits, ms = [], [], []
            for _ in range(args.queries):
                where = query_box(qrng, cols, width)
                s = time.perf_counter()
                out, stats = table.scan(where, version=version)
                ms.append((time.perf_counter() - s) * 1000)
                reads.append(stats["files_read"])
                hits.append(out.num_rows)
            print(f"| {name} | {label} | {statistics.mean(reads):.1f} / {args.files} "
                  f"| {statistics.mean(hits):,.0f} | {statistics.median(ms):.1f} |")


if __name__ == "__main__":
    main()
