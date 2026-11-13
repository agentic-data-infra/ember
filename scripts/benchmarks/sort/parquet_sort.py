# Ember driver: Parquet read -> orderBy("key") -> Parquet write (snappy).
#   parquet_sort.py <parquet dir> <out dir> <master>     (configuration from EMBER_CONF="k=v k=v ...")
import os, sys, time
from ember import Session
from ember.functions import col

b = Session.builder.appName("ParquetSort").master(sys.argv[3])
for kv in os.environ.get("EMBER_CONF", "").split():
    k, v = kv.split("=", 1)
    b = b.config(k, v)
ember = b.getOrCreate()
t0 = time.monotonic()
n = ember.read.parquet(sys.argv[1]).orderBy(col("key")).write.parquet(sys.argv[2])
print(f"Sorted {n} records ({n * 100} bytes) in {int((time.monotonic() - t0) * 1000)} ms", flush=True)
ember.stop()
