"""Tests of the Python binding: python -m unittest discover python/tests
(with PYTHONPATH=python and the extension built, see python/README.md)."""

import os
import tempfile
import unittest

import pyarrow as pa

from ember import Session
from ember.functions import col, countAll, explode, lit, lower, split, sum


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

    def test_errors(self):
        with self.assertRaises(RuntimeError):
            self.people().select("missing").collect()
        with self.assertRaises(ValueError):
            bool(col("a") > 1)


if __name__ == "__main__":
    unittest.main()
