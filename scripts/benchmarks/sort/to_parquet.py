# Converts gensort records to Parquet: one snappy file per input file, columns key (10 B) and payload (90 B).
#   to_parquet.py <gensort dir> <parquet dir>
import os, sys, time
import numpy as np, pyarrow as pa, pyarrow.parquet as pq

src, dst = sys.argv[1], sys.argv[2]
os.makedirs(dst, exist_ok=True)
t0 = time.time()


def binary(a, width):
    # Variable binary, not fixed_size_binary: not every reader handles FIXED_LEN_BYTE_ARRAY.
    offsets = np.arange(0, width * len(a) + 1, width, dtype=np.int32)
    return pa.Array.from_buffers(pa.binary(), len(a), [None, pa.py_buffer(offsets), pa.py_buffer(np.ascontiguousarray(a))])


for f in sorted(os.listdir(src)):
    a = np.fromfile(os.path.join(src, f), dtype=np.uint8).reshape(-1, 100)
    table = pa.table({"key": binary(a[:, :10], 10), "payload": binary(a[:, 10:], 90)})
    pq.write_table(table, os.path.join(dst, f + ".parquet"), compression="snappy")
print(f"converted {src} to {dst} in {time.time() - t0:.1f} s")
