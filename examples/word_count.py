"""WordCount in Python: counts the words of a text file (or a directory of
text files). The Python version of WordCount.cpp, printing the same table.

    python examples/word_count.py <path> [master] [workers]

Needs the ember package on PYTHONPATH (python/) with its extension built:
see python/README.md.
"""

import sys

from ember import Session
from ember.functions import col, explode, lower, split


def main(argv):
    if len(argv) < 2:
        print("Usage: word_count.py <file or directory> [master] [workers]", file=sys.stderr)
        return 1

    ember = (
        Session.builder.appName("DataframeWordCount")
        .master(argv[2] if len(argv) > 2 else "local[*]")
        .config("ember.cluster.workers", argv[3] if len(argv) > 3 else "1")
        .getOrCreate()
    )

    lines = ember.read.text(argv[1])  # one column: "value"

    words = lines.select(explode(split(lower(col("value")), r"\W+")).alias("word")).filter(
        col("word") != ""
    )

    counts = words.groupBy("word").count().orderBy(col("count").desc())

    counts.show(20, truncate=False)
    ember.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
