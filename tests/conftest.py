import pyarrow as pa
import pytest

from tinylake import Table

SCHEMA = pa.schema([("id", "int64"), ("x", "double"), ("tag", "string")])


def rows(start, n):
    ids = list(range(start, start + n))
    return pa.table({"id": ids, "x": [i * 0.5 for i in ids], "tag": [f"t{i % 7}" for i in ids]})


@pytest.fixture
def table(tmp_path):
    return Table.create(str(tmp_path / "t"), SCHEMA)
