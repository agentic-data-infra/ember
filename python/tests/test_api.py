"""Tests of the Python binding: python -m unittest discover python/tests
(with PYTHONPATH=python and the extension built, see python/README.md)."""

import os
import shutil
import subprocess
import sys
import tempfile
import unittest

import pyarrow as pa

import ember
from ember import Session
from ember.functions import col, countAll, explode, lit, lower, split, sum


class CodeVersionTest(unittest.TestCase):
    def test_rebuilt_extension_changes_stage_keys(self):
        script = """
import re, sys
from ember import Session
ember = Session.builder.appName("CodeVersion").master("local[1]").getOrCreate()
plan = ember.read.text(sys.argv[1]).groupBy("value").count().explainString()
print(re.findall(r"\\[key ([0-9a-f]+)", plan))
ember.stop()
"""
        with tempfile.TemporaryDirectory() as dir:
            package = os.path.join(dir, "ember")
            shutil.copytree(os.path.dirname(ember.__file__), package)
            data = os.path.join(dir, "data.txt")
            with open(data, "w") as file:
                file.write("a\nb\n")

            def keys():
                result = subprocess.run(
                    [sys.executable, "-c", script, data],
                    env={**os.environ, "PYTHONPATH": dir, "GLOG_minloglevel": "2"},
                    capture_output=True, text=True, check=True)
                return result.stdout.strip().splitlines()[-1]

            before = keys()
            self.assertNotEqual(before, "[]")
            self.assertEqual(keys(), before)
            extension = os.path.join(package, "_ember.so")
            stat = os.stat(extension)
            os.utime(extension, (stat.st_atime, stat.st_mtime + 10))
            self.assertNotEqual(keys(), before)


class ApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ember = Session.builder.appName("PythonApiTest").master("local[2]").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.ember.stop()

    def people(self):
        return self.ember.createDataFrame(
            pa.table({"name": ["ann", "bob", "cy", "dee"], "dept": ["a", "b", "a", "b"],
                      "salary": [10, 20, 30, 40]}))

    def test_select_filter_collect(self):
        df = self.people().filter(col("salary") > 15).select("name", (col("salary") * 2).alias("double"))
        self.assertEqual(df.columns, ["name", "double"])
        self.assertEqual(sorted(df.collect()), [("bob", 40), ("cy", 60), ("dee", 80)])

    def test_operators_and_literals(self):
        df = self.people().select(
            (1 + col("salary")).alias("a"),
            ((col("salary") >= 20) & ~(col("name") == "dee")).alias("b"),
            lit("x").alias("c"),
        ).orderBy("a")
        self.assertEqual(df.collect(), [(11, False, "x"), (21, True, "x"), (31, True, "x"), (41, False, "x")])

    def test_group_by(self):
        df = self.people().groupBy("dept").agg(sum("salary").alias("total"), countAll().alias("n"))
        self.assertEqual(sorted(df.collect()), [("a", 40, 2), ("b", 60, 2)])
        self.assertEqual(sorted(self.people().groupBy("dept").count().collect()), [("a", 2), ("b", 2)])

    def test_to_arrow(self):
        table = self.people().orderBy(col("salary").desc()).limit(2).toArrow()
        self.assertIsInstance(table, pa.Table)
        self.assertEqual(table.column("name").to_pylist(), ["dee", "cy"])
        empty = self.people().filter(col("salary") > 100).toArrow()
        self.assertEqual((empty.num_rows, empty.column_names), (0, ["name", "dept", "salary"]))

    def test_join_and_count(self):
        depts = self.ember.createDataFrame(pa.table({"dept": ["a", "b"], "title": ["eng", "ops"]}))
        joined = self.people().join(depts, "dept")
        self.assertEqual(joined.count(), 4)
        self.assertEqual(sorted(joined.select("name", "title").collect())[0], ("ann", "eng"))

    def test_word_count_and_cache(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "input.txt")
            with open(path, "w") as f:
                f.write("the cat\nThe dog, the end\n")
            words = self.ember.read.text(path).select(
                explode(split(lower(col("value")), r"\W+")).alias("word")).filter(col("word") != "")
            counts = words.groupBy("word").count().cache()
            self.assertEqual(counts.orderBy(col("count").desc(), "word").take(2), [("the", 3), ("cat", 1)])
            self.assertEqual(counts.count(), 4)
            self.assertIn("count", counts.showString())
            self.assertTrue(counts.explainString())

    def test_write_parquet(self):
        with tempfile.TemporaryDirectory() as tmp:
            out = os.path.join(tmp, "people")
            self.assertEqual(self.people().write.parquet(out), 4)
            self.assertTrue(any(name.endswith(".parquet") for name in os.listdir(out)))
            back = self.ember.read.parquet(out)
            self.assertEqual(sorted(back.collect()), sorted(self.people().collect()))
            with self.assertRaises(RuntimeError):
                self.people().write.parquet(out)

    def test_errors(self):
        with self.assertRaises(RuntimeError):
            self.people().select("missing").collect()
        with self.assertRaises(ValueError):
            bool(col("a") > 1)
        with self.assertRaises(ValueError):
            self.people().write.stream("train", batch=32)


class RangeSortTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.ember = (
            Session.builder.appName("PythonRangeSortTest")
            .master("local[2]")
            .config("ember.sort.range_partitions", "4")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.ember.stop()

    def test_order_by_collect(self):
        names = [f"same-prefix-{i:04}" for i in range(1000)]
        df = self.ember.createDataFrame(pa.table({"name": names[::-1]}))
        self.assertEqual(df.orderBy("name").collect(), [(name,) for name in names])
        self.assertEqual(df.orderBy(col("name").desc()).collect(), [(name,) for name in names[::-1]])


if __name__ == "__main__":
    unittest.main()
