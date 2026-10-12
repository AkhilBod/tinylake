import pyarrow as pa
import pytest

from conftest import rows


def ids(t):
    return sorted(t["id"].to_pylist())


def test_append_and_scan(table):
    table.append(rows(0, 100), rows_per_file=10)
    assert len(table.snapshot().files) == 10
    out, stats = table.scan([("id", ">=", 40), ("id", "<", 45)])
    assert ids(out) == [40, 41, 42, 43, 44]
    assert stats == {"files_read": 1, "files_skipped": 9}


def test_scan_projection(table):
    table.append(rows(0, 10))
    out = table.read([("x", ">", 3.0)], columns=["tag"])
    assert out.column_names == ["tag"]
    assert out.num_rows == 3


def test_append_casts_to_table_schema(table):
    table.append(pa.table({"id": pa.array([1], pa.int32()), "x": [1.0], "tag": ["a"]}))
    assert table.read().schema.field("id").type == pa.int64()


def test_delete_rewrites_only_touched_files(table):
    table.append(rows(0, 100), rows_per_file=10)
    before = set(table.snapshot().files)
    table.delete([("id", ">=", 15), ("id", "<", 30)])
    after = set(table.snapshot().files)
    # file 10-19 is rewritten, file 20-29 is dropped whole, the other 8 are untouched
    assert len(before - after) == 2
    assert len(after - before) == 1
    assert ids(table.read()) == list(range(15)) + list(range(30, 100))


def test_delete_keeps_null_rows(table):
    table.append(pa.table({"id": [1, 2, 3], "x": [1.0, None, 3.0], "tag": ["a", "b", "c"]}))
    table.delete([("x", ">", 0.0)])
    assert ids(table.read()) == [2]


def test_delete_with_no_match_commits_nothing(table):
    table.append(rows(0, 10))
    assert table.delete([("id", ">", 1000)]) is None
    assert table.version() == 1


def test_optimize_compacts_and_keeps_rows(table):
    for i in range(20):
        table.append(rows(i * 5, 5))
    table.optimize(target_rows=40)
    snap = table.snapshot()
    assert [f["rows"] for f in snap.files.values()] == [40, 40, 20]
    assert ids(table.read()) == list(range(100))
    adds = [a for a in __import__("tinylake").log.read_commit(table.root, table.version()) if "add" in a]
    assert all(a["add"]["dataChange"] is False for a in adds)


def test_zorder_tightens_ranges_on_both_columns(table):
    import random
    rng = random.Random(1)
    n = 20_000
    data = pa.table({
        "id": [rng.randrange(1_000_000) for _ in range(n)],
        "x": [float(rng.randrange(1_000_000)) for _ in range(n)],
        "tag": ["t"] * n,
    })
    table.append(data, rows_per_file=1000)
    query = [("id", ">=", 100_000), ("id", "<", 200_000), ("x", ">=", 500_000.0), ("x", "<", 600_000.0)]
    expected = ids(table.read(query))
    _, before = table.scan(query)
    table.optimize(target_rows=1000, zorder_by=["id", "x"])
    out, after = table.scan(query)
    assert ids(out) == expected
    assert before["files_skipped"] == 0
    assert after["files_read"] < before["files_read"] / 3


def test_unknown_column_and_operator(table):
    table.append(rows(0, 3))
    with pytest.raises(KeyError):
        table.read([("nope", "==", 1)])
    with pytest.raises(ValueError):
        table.read([("id", "!=", 1)])


def test_vacuum_breaks_old_versions_only(table):
    table.append(rows(0, 10), rows_per_file=5)
    table.delete([("id", "<", 3)])
    assert table.vacuum(keep_versions=1) == 1
    assert table.read().num_rows == 7
    with pytest.raises(FileNotFoundError):
        table.read(version=1)
