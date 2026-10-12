# tinylake

A small table format on top of Parquet files, in the style of Delta Lake. It
gives a directory of Parquet files ACID commits, concurrent writers, time
travel, compaction, Z-ordering, and min/max data skipping.

It is about 840 lines of Python (library, tests and benchmark) on top of
`pyarrow` and `numpy`. It exists to understand how a lakehouse table works
underneath, so it favors code you can read in one sitting over features.

## Quick start

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
t.read(version=1)                     # time travel
rows, stats = t.scan([("id", "<", 2)])
print(stats)                          # {'files_read': 1, 'files_skipped': 0}
t.history()                           # CREATE, APPEND, DELETE, OPTIMIZE
```

## How it works

### The log (`tinylake/log.py`, `tinylake/snapshot.py`)

A table is a directory of Parquet files plus `_log/`. Version N is the commit
file `_log/000...N.json`, one JSON action per line:

- `metaData`: the schema
- `add`: a data file, with its row count and per-column min/max
- `remove`: a data file that is no longer part of the table
- `commitInfo`: operation name, the version the writer read, a timestamp

The live files at version N are whatever replaying commits 0..N leaves
behind. Data files are never modified. A delete writes new files and
removes old ones in the same commit, which is what makes time travel free:
reading an old version means replaying fewer commits.

Every 10 commits a writer also saves a checkpoint, the full file list at that
version, so loading a snapshot replays at most 9 commits after the newest
checkpoint. Checkpoints are only a cache. Deleting them gives the same
snapshot (there is a test for that).

The one thing the log needs from storage is "create this file only if it
does not exist yet". On a local filesystem that is a write to a temp file
followed by `os.link()`, which fails if the target exists. Readers never see
a half-written commit, and of two writers racing for version N, exactly one
wins. Object stores give the same guarantee with conditional PUTs.

### Concurrency (`tinylake/table.py`)

Writers are optimistic. A transaction reads the latest snapshot, does all of
its work (writing new Parquet files and deciding which old ones to remove),
then tries to create the next commit. If it loses that race, it reads the
winner's commit and decides whether its own changes still make sense on top:

- If the winner changed the schema, that's a conflict.
- If the winner removed a file this transaction read or wants to remove,
  that's a conflict. That covers two deletes of the same rows, or a delete
  racing a compaction of the files it was rewriting.
- If the winner only added files, there's no conflict. The loser retries at
  the next version without redoing any work.

This is a simplified version of Delta Lake's WriteSerializable isolation.
Blind appends never conflict, so many processes can append at once. One test
runs 8 processes making 120 commits together, and every row lands exactly
once. A delete that loses a race to an append is ordered after it, so the
appended rows survive, which is the documented trade-off of that isolation
level.

On a conflict the transaction raises `ConcurrentModificationError`. Its new
Parquet files are left as orphans with no commit pointing at them, which is
harmless. `vacuum()` cleans them up.

### Reads and data skipping (`tinylake/predicate.py`)

Predicates are conjunctions of `(column, op, value)`. Each `add` action
carries min/max stats, so a scan first throws away every file whose ranges
prove it can't match, then filters rows inside the files that remain. Rows
where the predicate is null (null inputs) never match, and a delete keeps
them.

### Compaction and Z-order (`tinylake/zorder.py`)

`optimize()` merges files smaller than `target_rows`. With `zorder_by`, it
rewrites every file in Z-order (Morton order) on those columns. Each column
is replaced by its rank, scaled to a fixed number of bits, and the bits of
all the columns are interleaved, so rows close in every column end up close
together. Rewritten files are marked `dataChange: false`, since the rows are
the same and only the layout changed.

## Results

`python bench/skipping.py` writes 2M rows with uniform random `x` and `y`
into 200 files in arrival order. It then rewrites the table sorted by `x`,
then Z-ordered on `(x, y)`. All three layouts stay readable through time
travel, so the same 50 random queries run against each. Each range covers 1%
of its column. Measured on an Apple M3, seed 7:

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

What the numbers say:

- In arrival order, every file spans almost the whole domain of every column,
  so min/max stats skip nothing. Every query reads all 200 files.
- Sorting by `x` is the best layout for any query that filters on `x`: 3
  files instead of 200, about 25x faster. It does nothing at all for a query
  that filters only on `y`.
- Z-order gives up some of the `x` win (31 files instead of 3) to make `y`
  queries 10x cheaper as well. It is the layout to pick when you don't know
  which column the next query will filter on.
- For the two-column query, Z-order did not beat a plain sort here. The 1%
  range on `x` alone already narrows a sorted table to 3 files, so there was
  nothing left to win. Z-order pays off when each filter on its own is too
  wide to prune much.

## Not included

- Partitioning, schema evolution, column mapping, deletion vectors, MERGE.
- Stats are only min/max; no null counts, no bloom filters.
- A single directory on one filesystem. The commit protocol assumes atomic
  put-if-absent, which a local disk and S3 conditional writes both provide,
  but there is no object-store backend here.
- Supported column types: int32, int64, float, double, string, bool.
