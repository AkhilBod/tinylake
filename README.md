# tinylake

A tiny Delta Lake clone in Python. Point it at a folder and you get a table of
Parquet files with commits, multiple writers, time travel, compaction, Z-order
and file skipping.

I built it to figure out how Delta tables pull off transactions when Parquet
files can't even be edited. It's about 840 lines with tests and benchmark, and
it only needs `pyarrow` and `numpy`. Longer write-up:
[The bug that sent me building a tiny data lake](https://akhilbod.github.io/writing/tinylake.html).

## Try it

```
pip install -e ".[test]"
pytest
```

```python
import pyarrow as pa
from tinylake import Table

t = Table.create("events", pa.schema([("id", "int64"), ("x", "double"), ("tag", "string")]))
t.append(pa.table({"id": [1, 2, 3], "x": [0.5, 1.0, 1.5], "tag": ["a", "b", "a"]}))
t.delete([("tag", "==", "b")])
t.optimize(zorder_by=["id", "x"])

t.read([("x", ">=", 1.0)])           # latest version
t.read(version=1)                     # what it looked like after the first append
rows, stats = t.scan([("id", "<", 2)])
print(stats)                          # {'files_read': 1, 'files_skipped': 0}
t.history()                           # CREATE, APPEND, DELETE, OPTIMIZE
```

## How it works

### The log (`log.py`, `snapshot.py`)

Next to the data files there's a `_log/` folder. Each commit is one numbered
JSON file, one action per line:

- `metaData`: the schema
- `add`: a new data file, with row count and min/max for every column
- `remove`: a file that's no longer part of the table
- `commitInfo`: what ran, which version it read, when

To get version N, replay commits 0 to N and keep whatever files are left.
Data files never change after they're written. A delete writes new files and
removes the old ones in a single commit, and the old files stay on disk until
`vacuum()`, which is why old versions are still readable.

Every 10th commit also writes a checkpoint with the full file list, so loading
a snapshot replays 9 commits at most. Checkpoints are only a cache. A test
deletes them and checks the replayed table matches.

The log needs one thing from storage: create a file only if it isn't there
yet. Locally that's a temp file plus `os.link()`, which fails when the name is
taken. S3 has conditional PUTs for the same job. Readers never see a
half-written commit, and only one of two writers can win a given version.

### Concurrent writers (`table.py`)

Nobody locks. A writer reads the latest version, writes its Parquet files,
then tries to create the next commit. If it loses, it reads the commit that
beat it:

- Schema changed: conflict.
- A file this writer read or wanted to remove got removed: conflict. That
  catches two deletes on the same rows, and a delete racing a compaction.
- The other commit only added files: retry at the next version. No work is
  redone.

Failed writers raise `ConcurrentModificationError`. Their Parquet files are
left behind with no commit pointing at them, and `vacuum()` deletes them.

This is a cut-down version of Delta's WriteSerializable isolation. Appends
never conflict with each other, so lots of processes can append at once. One
test runs 8 processes making 120 commits together and checks every row lands
exactly once. Heads up: a delete that loses a race to an append gets ordered
after it, so the appended rows don't get deleted. Delta does this on purpose
too.

### File skipping (`predicate.py`)

A filter is a list of `(column, op, value)` that all have to hold. Before
opening a file, the scan checks its min/max and skips it if no row could
match. Rows where the filter hits a null never match, and deletes keep them.

### Compaction and Z-order (`zorder.py`)

`optimize()` merges files smaller than `target_rows`. Pass `zorder_by` and it
rewrites everything in Z-order on those columns: each column becomes its rank,
the bits get interleaved, and rows that are close in all the columns end up in
the same files. Rewritten files are marked `dataChange: false` since the rows
didn't change, only where they live.

## Benchmark

`python bench/skipping.py` writes 2M rows with random `x` and `y` into 200
files, then rewrites them sorted by `x`, then Z-ordered on `(x, y)`. Time
travel keeps all three layouts around, so the same 50 random queries run on
each. Every range covers 1% of its column. Apple M3, seed 7:

| Layout | Query | Files read (mean) | Rows returned (mean) | Scan ms (median) |
|---|---|---|---|---|
| arrival order | x in 1% range | 200.0 / 200 | 19,991 | 65.3 |
| arrival order | y in 1% range | 200.0 / 200 | 19,985 | 65.7 |
| arrival order | x and y in 1% ranges | 200.0 / 200 | 201 | 69.3 |
| sorted by x | x in 1% range | 3.0 / 200 | 19,991 | 2.7 |
| sorted by x | y in 1% range | 200.0 / 200 | 19,985 | 66.2 |
| sorted by x | x and y in 1% ranges | 3.0 / 200 | 201 | 2.2 |
| Z-order (x, y) | x in 1% range | 31.0 / 200 | 19,991 | 12.3 |
| Z-order (x, y) | y in 1% range | 21.3 / 200 | 19,985 | 8.4 |
| Z-order (x, y) | x and y in 1% ranges | 3.2 / 200 | 201 | 2.7 |

What I took from it:

- Arrival order skips nothing. Every file has a bit of everything, so the
  stats can't rule any of them out.
- Sorting by `x` cuts `x` queries from 200 files to 3, about 25x faster, and
  doesn't help `y` queries at all.
- Z-order is the compromise: 31 files for `x`, 21 for `y`.
- I expected Z-order to win the two-column query. It tied with the sort,
  because the `x` range alone already gets a sorted table down to 3 files.
  Z-order is worth it when you can't predict which column people filter on.

## Missing

- Partitioning, schema evolution, MERGE, deletion vectors
- Stats beyond min/max (no null counts, no bloom filters)
- An object store backend. It's one local folder.
- Types other than int32, int64, float, double, string and bool
