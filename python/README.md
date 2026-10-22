# python/

Ember's Python API: a thin [pybind11](https://pybind11.readthedocs.io)
binding over the C++ Dataframe API (`ember/api`). The engine stays C++; Python
only builds plans and reads results.

| File | Contents |
|------|----------|
| `EmberModule.cpp` | The extension module `ember._ember`: `Session`, `Dataframe`, `GroupedData`, `Column`, `DataframeReader` and `functions`. |
| `ember/` | The Python package: re-exports the module and adds `collect`, `show`, `explain` and friends on top of `toArrow`. |
| `exports.map` | Linker version script: the module exports only `PyInit__ember`. |
| `tests/` | Tests of the binding. |

## Building

The module needs a Python with pybind11 and pyarrow. By default
`scripts/gen_third_party.py` takes the headers from `.venv` at the repository
root (`--python` picks another interpreter):

```
uv venv -p python3.10 .venv && uv pip install -p .venv/bin/python pybind11 pyarrow
scripts/build_deps.sh          # or scripts/gen_third_party.py after a venv change
buck2 build //python:_ember --out python/ember/_ember.so
PYTHONPATH=python .venv/bin/python -m unittest discover -s python/tests
```

The module is one shared object (~350 MB) that holds Ember, Axiom, Velox and
folly, so Axiom and Velox are built with `-fPIC`
(`CMAKE_POSITION_INDEPENDENT_CODE` in `scripts/build_deps.sh`). Only
`PyInit__ember` is exported, so the static libraries inside never clash with
the ones pyarrow loads, and libgcc_s is linked ahead of libunwind so that
exceptions unwind correctly whether pyarrow or ember is imported first.

## Using it

```python
from ember import Session
from ember.functions import col, countAll, sum

ember = Session.builder.appName("Example").master("local[4]").getOrCreate()
df = ember.read.parquet("docs/")
by_score = df.groupBy("int_score").agg(countAll().alias("docs"), sum("token_count").alias("tokens"))
table = by_score.toArrow()          # pyarrow.Table
rows = by_score.collect()           # list of tuples
by_score.cache()                    # as in C++: use the returned Dataframe
ember.createDataFrame(table)        # a Dataframe over a pyarrow Table
```

Names follow the C++ API (`groupBy`, `orderBy`, `withColumn`, ...), with
Python spellings where C++ differs: `alias()` for `as()`, `&`, `|` and `~`
for `&&`, `||` and `!`, `cast("double")` with a type name, and
`Session.builder` and `session.read` as attributes. A `str` argument is a
column name, except in operators, where it is a literal (`col("w") != ""`).
Actions release the GIL while Ember runs.

Not bound yet: Python UDFs (UDFs are Velox functions registered from C++ and
reachable with `functions.call`), `onStageCompleted`, and plan objects other
than `explainString()`.
