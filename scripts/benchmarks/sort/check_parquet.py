# Checks a Parquet sort output: keys in order across all files and the row count.
#   check_parquet.py <dir> <rows>
# Files are read in natural name order, as some writers number a task's files -0, -1, ... -10. Prints VALID=yes or VALID=no.
import os, re, sys
import numpy as np, pyarrow.parquet as pq

d, want = sys.argv[1], int(sys.argv[2])
nat = lambda x: [(0, int(t), "") if t.isdigit() else (1, 0, t) for t in re.split(r"(\d+)", x)]
hi, lo = [], []
for f in sorted((x for x in os.listdir(d) if x.endswith(".parquet")), key=nat):
    for c in pq.read_table(os.path.join(d, f), columns=["key"]).column(0).chunks:
        off = np.frombuffer(c.buffers()[1], dtype=np.int32)[c.offset:c.offset + len(c) + 1]
        assert np.all(np.diff(off) == 10), "keys must be 10 bytes"
        a = np.frombuffer(c.buffers()[2], dtype=np.uint8)[off[0]:off[-1]].reshape(-1, 10)
        hi.append(a[:, :8].copy().view(">u8").ravel())
        lo.append(a[:, 8:].copy().view(">u2").ravel())
hi, lo = np.concatenate(hi), np.concatenate(lo)
ok = bool(np.all((hi[1:] > hi[:-1]) | ((hi[1:] == hi[:-1]) & (lo[1:] >= lo[:-1]))))
print(f"rows={len(hi)} want={want} sorted={ok} VALID={'yes' if ok and len(hi) == want else 'no'}")
