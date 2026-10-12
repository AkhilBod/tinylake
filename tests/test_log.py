import os

import pytest

from conftest import SCHEMA, rows
from tinylake import Table, log


def test_create_twice_fails(table):
    with pytest.raises(FileExistsError):
        Table.create(table.root, SCHEMA)


def test_commit_is_put_if_absent(table):
    assert log.write_commit(table.root, 1, [{"commitInfo": {"operation": "A"}}])
    assert not log.write_commit(table.root, 1, [{"commitInfo": {"operation": "B"}}])
    assert log.read_commit(table.root, 1)[0]["commitInfo"]["operation"] == "A"
    # no temp files left behind by either attempt
    assert not [n for n in os.listdir(log.log_dir(table.root)) if n.startswith(".tmp-")]


def test_time_travel(table):
    table.append(rows(0, 10))
    table.append(rows(10, 10))
    table.delete([("id", "<", 5)])
    assert table.read(version=1).num_rows == 10
    assert table.read(version=2).num_rows == 20
    assert table.read(version=3).num_rows == 15
    with pytest.raises(ValueError):
        table.read(version=4)


def test_checkpoint_matches_full_replay(table):
    for i in range(25):
        table.append(rows(i * 4, 4))
    table.delete([("id", "==", 7)])
    assert os.path.exists(log.checkpoint_path(table.root, 10))
    assert os.path.exists(log.checkpoint_path(table.root, 20))

    from_checkpoint = table.snapshot()
    os.remove(log.checkpoint_path(table.root, 10))
    os.remove(log.checkpoint_path(table.root, 20))
    replayed = table.snapshot()
    assert from_checkpoint.files == replayed.files
    assert replayed.num_rows() == 99


def test_history(table):
    table.append(rows(0, 3))
    table.optimize()
    ops = [h["operation"] for h in table.history()]
    assert ops == ["CREATE", "APPEND"]  # optimize on one file is a no-op, nothing committed
