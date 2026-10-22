"""ThresholdSweep in Python: what a quality filter keeps, for a sweep of
thresholds, over a FineWeb-Edu style corpus (parquet with url, text,
int_score and token_count columns). The Python version of
ThresholdSweep.cpp, printing the same lines.

    python examples/threshold_sweep.py <path> [cache|nocache] [master]

With "cache" (the default) the per-document features are cached, so only
the first threshold computes them.
"""

import sys
import time

from ember import Session
from ember.functions import (
    avg,
    col,
    countAll,
    countDistinct,
    length,
    lit,
    lower,
    regexp_extract,
    regexp_replace,
    sum,
)


def main(argv):
    if len(argv) < 2:
        print(
            "Usage: threshold_sweep.py <file or directory> [cache|nocache] [master]",
            file=sys.stderr,
        )
        return 1
    use_cache = len(argv) < 3 or argv[2] != "nocache"

    ember = (
        Session.builder.appName("ThresholdSweep")
        .master(argv[3] if len(argv) > 3 else "local[*]")
        .getOrCreate()
    )

    text = col("text")
    features = ember.read.parquet(argv[1]).select(
        lower(
            regexp_extract(col("url"), r"^[A-Za-z][A-Za-z0-9+.-]*://(?:www\.)?([^/:?#]+)", 1)
        ).alias("domain"),
        col("int_score"),
        col("token_count"),
        (
            length(regexp_replace(text, "[^A-Za-z ]", "")).cast("double")
            / length(text).cast("double")
        ).alias("alpha_ratio"),
    )
    if use_cache:
        features = features.cache()

    for threshold in (0.70, 0.75, 0.80, 0.82, 0.84, 0.86, 0.88, 0.90):
        start = time.monotonic()
        rows = (
            features.filter(col("alpha_ratio") >= lit(threshold))
            .agg(
                countAll().alias("docs"),
                sum(col("token_count")).alias("tokens"),
                countDistinct(col("domain")).alias("domains"),
                avg(col("int_score")).alias("avg_int_score"),
            )
            .collect()
        )
        ms = int((time.monotonic() - start) * 1000)
        print(f"alpha_ratio >= {threshold:g}: {rows[0]} in {ms} ms")

    ember.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
