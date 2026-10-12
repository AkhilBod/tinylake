"""Table operations and optimistic concurrency control.

Every write follows the same shape:

1. Read the latest snapshot (the "read version").
2. Do the work: write new Parquet files, decide which old files to remove.
3. Try to create commit file read_version + 1.
4. If another writer got there first, read their commit and decide whether
   ours still makes sense on top of it. If it does, try the next version. If
   not, raise ConcurrentModificationError and leave the table untouched.

Step 4 is what makes concurrent appends cheap: two blind appends never
conflict, so the loser just retries at the next version without redoing any
work. The rules are a simplified version of Delta Lake's WriteSerializable
isolation:

- A schema change by the winner conflicts with everything.
- If the winner removed a file this transaction read or wants to remove, it
  conflicts (two deletes of the same rows, or a delete racing compaction).
- Files the winner only added are ignored. A delete that loses a race to an
  append is ordered after the append in the log, and the appended rows stay.
"""

import os
import time
import uuid

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from . import log, snapshot
from .log import ConcurrentModificationError
from .predicate import may_match, row_mask, validate
from .zorder import zorder_sort

TYPES = {
    "int32": pa.int32(),
    "int64": pa.int64(),
    "float": pa.float32(),
    "double": pa.float64(),
    "string": pa.string(),
    "bool": pa.bool_(),
}


def _schema_to_log(schema):
    out = []
    for f in schema:
        name = str(f.type)
        if name not in TYPES:
            raise TypeError(f"column {f.name!r}: unsupported type {f.type}")
        out.append([f.name, name])
    return out


def _schema_from_log(fields):
    return pa.schema([(name, TYPES[t]) for name, t in fields])


def _stats(data):
    lo, hi = {}, {}
    for name in data.column_names:
        mm = pc.min_max(data[name]).as_py()
        lo[name], hi[name] = mm["min"], mm["max"]
    return {"min": lo, "max": hi}


class Transaction:
    def __init__(self, table, operation):
        self.table = table
        self.operation = operation
        self.snapshot = table.snapshot()
        self.read_version = self.snapshot.version
        self.read_files = set()  # files whose contents this transaction depends on
        self.actions = []

    def add_file(self, data, data_change=True):
        path = f"part-{uuid.uuid4().hex}.parquet"
        pq.write_table(data, os.path.join(self.table.root, path))
        self.actions.append({"add": {
            "path": path,
            "rows": data.num_rows,
            "size": os.path.getsize(os.path.join(self.table.root, path)),
            "stats": _stats(data),
            "dataChange": data_change,
        }})

    def remove_file(self, path, data_change=True):
        self.read_files.add(path)
        self.actions.append({"remove": {"path": path, "dataChange": data_change}})

    def _check(self, winner_version):
        for action in log.read_commit(self.table.root, winner_version):
            if "metaData" in action:
                raise ConcurrentModificationError(
                    f"{self.operation}: schema changed in version {winner_version}")
            if "remove" in action and action["remove"]["path"] in self.read_files:
                raise ConcurrentModificationError(
                    f"{self.operation}: {action['remove']['path']} was removed by "
                    f"version {winner_version}, which this transaction read")

    def commit(self, max_attempts=1000):
        """Returns the committed version, or None if there was nothing to commit."""
        if not self.actions:
            return None
        info = {"commitInfo": {
            "operation": self.operation,
            "readVersion": self.read_version,
            "timestamp": int(time.time() * 1000),
        }}
        version = self.read_version + 1
        for _ in range(max_attempts):
            if log.write_commit(self.table.root, version, [info] + self.actions):
                self.table._maybe_checkpoint(version)
                return version
            self._check(version)
            version += 1
        raise ConcurrentModificationError(f"{self.operation}: gave up after {max_attempts} attempts")


class Table:
    def __init__(self, root):
        self.root = root

    @classmethod
    def create(cls, root, schema):
        os.makedirs(log.log_dir(root), exist_ok=True)
        actions = [
            {"commitInfo": {"operation": "CREATE", "readVersion": -1,
                            "timestamp": int(time.time() * 1000)}},
            {"metaData": {"schema": _schema_to_log(schema)}},
        ]
        if not log.write_commit(root, 0, actions):
            raise FileExistsError(f"table already exists at {root}")
        return cls(root)

    def snapshot(self, version=None):
        return snapshot.load(self.root, version)

    def schema(self, version=None):
        return _schema_from_log(self.snapshot(version).schema)

    def version(self):
        return log.latest_version(self.root)

    def history(self):
        return [
            dict(version=v, **next(a["commitInfo"] for a in log.read_commit(self.root, v)
                                   if "commitInfo" in a))
            for v in log.list_versions(self.root)
        ]

    def _maybe_checkpoint(self, version):
        if version % log.CHECKPOINT_EVERY == 0:
            log.write_checkpoint(self.root, version, self.snapshot(version).to_state())

    # ----- reads -----

    def scan(self, where=None, columns=None, version=None):
        """Returns (rows, stats). stats counts files read vs. skipped by min/max."""
        snap = self.snapshot(version)
        schema = _schema_from_log(snap.schema)
        validate(where, schema.names)
        need = None
        if columns is not None:
            need = list(dict.fromkeys(list(columns) + [c for c, _, _ in where or []]))
        parts, read, skipped = [], 0, 0
        for f in snap.files.values():
            if not may_match(f["stats"], where):
                skipped += 1
                continue
            read += 1
            data = pq.read_table(os.path.join(self.root, f["path"]), columns=need)
            if where:
                data = data.filter(row_mask(data, where))
            parts.append(data)
        out_schema = schema if need is None else pa.schema([schema.field(c) for c in need])
        rows = pa.concat_tables(parts) if parts else out_schema.empty_table()
        if columns is not None:
            rows = rows.select(list(columns))
        return rows, {"files_read": read, "files_skipped": skipped}

    def read(self, where=None, columns=None, version=None):
        return self.scan(where, columns, version)[0]

    # ----- writes -----
    # Each write has a plan_* form that does the work and returns an
    # uncommitted Transaction. Tests use it to interleave writers.

    def plan_append(self, data, rows_per_file=None):
        txn = Transaction(self, "APPEND")
        data = data.cast(_schema_from_log(txn.snapshot.schema))
        step = rows_per_file or max(data.num_rows, 1)
        for start in range(0, data.num_rows, step):
            txn.add_file(data.slice(start, step))
        return txn

    def append(self, data, rows_per_file=None):
        return self.plan_append(data, rows_per_file).commit()

    def plan_delete(self, where):
        """Copy-on-write delete: rewrite each file that holds matching rows."""
        txn = Transaction(self, "DELETE")
        validate(where, [n for n, _ in txn.snapshot.schema])
        for f in txn.snapshot.files.values():
            if not may_match(f["stats"], where):
                continue
            data = pq.read_table(os.path.join(self.root, f["path"]))
            mask = row_mask(data, where)
            hits = pc.sum(mask).as_py() or 0
            if hits == 0:
                continue
            txn.remove_file(f["path"])
            if hits < data.num_rows:
                txn.add_file(data.filter(pc.invert(mask)))
        return txn

    def delete(self, where):
        return self.plan_delete(where).commit()

    def plan_optimize(self, target_rows=100_000, zorder_by=None):
        """Compact small files into ~target_rows files.

        With zorder_by, every file is rewritten in Z-order on those columns so
        their min/max ranges get tight enough to skip files. Rewritten files are
        marked dataChange=False: the rows are the same, only the layout moved.
        """
        txn = Transaction(self, "OPTIMIZE")
        files = list(txn.snapshot.files.values())
        if not zorder_by:
            files = [f for f in files if f["rows"] < target_rows]
            if len(files) < 2:
                return txn
        if not files:
            return txn
        data = pa.concat_tables(
            [pq.read_table(os.path.join(self.root, f["path"])) for f in files])
        if zorder_by:
            validate([(c, "==", None) for c in zorder_by], data.column_names)
            data = zorder_sort(data, zorder_by)
        for f in files:
            txn.remove_file(f["path"], data_change=False)
        for start in range(0, data.num_rows, target_rows):
            txn.add_file(data.slice(start, target_rows), data_change=False)
        return txn

    def optimize(self, target_rows=100_000, zorder_by=None):
        return self.plan_optimize(target_rows, zorder_by).commit()

    def vacuum(self, keep_versions):
        """Delete data files not referenced by any of the last `keep_versions` versions.

        Time travel to versions older than that stops working afterwards.
        """
        latest = self.version()
        live = set()
        for v in range(max(0, latest - keep_versions + 1), latest + 1):
            live.update(self.snapshot(v).files)
        removed = 0
        for name in os.listdir(self.root):
            if name.startswith("part-") and name not in live:
                os.remove(os.path.join(self.root, name))
                removed += 1
        return removed
