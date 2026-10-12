"""Conjunctive predicates: a list of (column, op, value), all of which must hold.

The same predicate is used twice: against per-file min/max stats to skip files
that cannot contain a match, and against the rows of files that might.
"""

import functools

import pyarrow.compute as pc

OPS = {
    "==": pc.equal,
    "<": pc.less,
    "<=": pc.less_equal,
    ">": pc.greater,
    ">=": pc.greater_equal,
}


def validate(where, schema_names):
    for col, op, _ in where or []:
        if op not in OPS:
            raise ValueError(f"unsupported operator {op!r}")
        if col not in schema_names:
            raise KeyError(f"unknown column {col!r}")


def may_match(stats, where):
    """False only when the stats prove no row in the file satisfies `where`."""
    for col, op, value in where or []:
        lo = stats["min"].get(col)
        hi = stats["max"].get(col)
        if lo is None or hi is None:
            continue  # no stats (e.g. all nulls): must read the file
        if op == "==" and not lo <= value <= hi:
            return False
        if op == "<" and not lo < value:
            return False
        if op == "<=" and not lo <= value:
            return False
        if op == ">" and not hi > value:
            return False
        if op == ">=" and not hi >= value:
            return False
    return True


def row_mask(table, where):
    """Boolean mask; rows where the predicate is null (null inputs) count as False."""
    masks = [OPS[op](table[col], value) for col, op, value in where]
    return pc.fill_null(functools.reduce(pc.and_, masks), False)
