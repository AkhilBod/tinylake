"""Z-order (Morton) keys for clustering rows on several columns at once.

Each column is replaced by its rank, scaled to `bits` bits, and the bits of
all columns are interleaved. Sorting by the result keeps rows that are close
in every column close in the file layout, so min/max ranges stay narrow on
each clustered column instead of only the first one.
"""

import numpy as np
import pyarrow.compute as pc


def zorder_keys(table, columns):
    k = len(columns)
    bits = min(32, 64 // k)
    keys = np.zeros(table.num_rows, dtype=np.uint64)
    for i, col in enumerate(columns):
        # rank (dense over sorted order) so skewed columns still use every bit
        ranks = pc.rank(table[col], sort_keys="ascending", tiebreaker="dense")
        r = ranks.to_numpy().astype(np.float64) - 1
        top = max(r.max(), 1)
        scaled = (r / top * ((1 << bits) - 1)).astype(np.uint64)
        for b in range(bits):
            keys |= ((scaled >> np.uint64(b)) & np.uint64(1)) << np.uint64(b * k + i)
    return keys


def zorder_sort(table, columns):
    if len(columns) == 1:
        return table.sort_by(columns[0])
    order = np.argsort(zorder_keys(table, columns), kind="stable")
    return table.take(order)
