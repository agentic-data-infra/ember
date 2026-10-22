"""Ember's Dataframe API for Python.

A thin binding over the C++ API (ember/api): the same Session, Dataframe,
Column and functions, with results returned as pyarrow tables.

    from ember import Session
    from ember.functions import col, explode, lower, split

    ember = Session.builder.appName("WordCount").master("local[*]").getOrCreate()
    lines = ember.read.text(path)
    words = lines.select(explode(split(lower(col("value")), r"\\W+")).alias("word"))
    words.groupBy("word").count().orderBy(col("count").desc()).show()
"""

import atexit

from ember._ember import Column, Dataframe, DataframeReader, GroupedData, Session


def _collect(self):
    """All rows, as a list of tuples."""
    table = self.toArrow()
    return list(zip(*(c.to_pylist() for c in table.columns))) if table.num_columns else []


def _take(self, n):
    return self.limit(n).collect()


def _show(self, n=20, truncate=True):
    print(self.showString(n, truncate))


def _explain(self):
    print(self.explainString())


def _print_schema(self):
    print(self.schemaString())


def _to_pandas(self):
    return self.toArrow().to_pandas()


Dataframe.collect = _collect
Dataframe.take = _take
Dataframe.head = _take
Dataframe.show = _show
Dataframe.explain = _explain
Dataframe.printSchema = _print_schema
Dataframe.toPandas = _to_pandas

__all__ = ["Column", "Dataframe", "DataframeReader", "GroupedData", "Session"]


def _stop_active_session():
    # The active session is held in C++ static state, which outlives the
    # interpreter; stop it while Velox is still intact.
    session = Session.active()
    if session is not None:
        session.stop()


atexit.register(_stop_active_session)
