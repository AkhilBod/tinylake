import multiprocessing as mp

import pytest

from conftest import rows
from tinylake import ConcurrentModificationError, Table


def test_racing_appends_both_land(table):
    a = table.plan_append(rows(0, 5))
    b = table.plan_append(rows(5, 5))
    assert a.commit() == 1
    assert b.commit() == 2  # lost the race for version 1, rebased onto it
    assert table.read().num_rows == 10


def test_delete_vs_delete_of_same_file_conflicts(table):
    table.append(rows(0, 10))
    a = table.plan_delete([("id", "<", 3)])
    b = table.plan_delete([("id", ">", 7)])
    a.commit()
    with pytest.raises(ConcurrentModificationError):
        b.commit()
    assert table.read().num_rows == 7  # only a's delete happened


def test_delete_vs_optimize_conflicts(table):
    for i in range(4):
        table.append(rows(i * 5, 5))
    opt = table.plan_optimize(target_rows=100)
    dele = table.plan_delete([("id", "==", 3)])
    opt.commit()
    with pytest.raises(ConcurrentModificationError):
        dele.commit()
    assert table.read().num_rows == 20


def test_delete_on_other_files_does_not_conflict(table):
    table.append(rows(0, 10), rows_per_file=5)
    a = table.plan_delete([("id", "<", 2)])   # touches file 0-4
    b = table.plan_delete([("id", ">", 8)])   # touches file 5-9
    a.commit()
    b.commit()
    assert sorted(table.read()["id"].to_pylist()) == list(range(2, 9))


def test_delete_losing_to_append_keeps_appended_rows(table):
    table.append(rows(0, 5))
    dele = table.plan_delete([("id", "<", 100)])
    table.append(rows(50, 5))
    dele.commit()
    assert sorted(table.read()["id"].to_pylist()) == list(range(50, 55))


def test_schema_change_conflicts_with_append(table):
    from tinylake import log
    txn = table.plan_append(rows(0, 1))
    log.write_commit(table.root, 1, [{"metaData": {"schema": [["id", "int64"]]}}])
    with pytest.raises(ConcurrentModificationError):
        txn.commit()


def _writer(root, worker, commits, per_commit):
    t = Table(root)
    for c in range(commits):
        t.append(rows((worker * commits + c) * per_commit, per_commit))


def test_many_processes_appending(table):
    workers, commits, per_commit = 8, 15, 10
    ctx = mp.get_context("spawn")
    procs = [ctx.Process(target=_writer, args=(table.root, w, commits, per_commit))
             for w in range(workers)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(60)
        assert p.exitcode == 0
    assert table.version() == workers * commits
    got = sorted(table.read()["id"].to_pylist())
    assert got == list(range(workers * commits * per_commit))
