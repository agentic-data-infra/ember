"""Checks that the Python examples print the same results as the C++ ones.

Runs WordCount (word_count.py vs //examples:word_count) on a text file or
directory and ThresholdSweep (threshold_sweep.py vs //examples:threshold_sweep)
on a FineWeb-Edu style parquet directory, and compares what they print.

    python examples/check_python_examples.py <text path> <parquet path>

Run from the repository root with the ember package built (python/README.md).
"""

import math
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def cpp_binary(target):
    out = subprocess.run(
        ["buck2", "build", target, "--show-full-output"],
        cwd=ROOT, check=True, capture_output=True, text=True,
    ).stdout
    return out.split()[-1]


def run(cmd):
    env = dict(os.environ, PYTHONPATH=os.path.join(ROOT, "python"))
    return subprocess.run(cmd, cwd=ROOT, env=env, check=True, capture_output=True, text=True).stdout


def table_rows(output):
    """Rows of a show() table: |a|b| lines after the header."""
    rows = [
        [cell.strip() for cell in line.strip("|").split("|")]
        for line in output.splitlines()
        if line.startswith("|")
    ]
    return rows[1:]


def check_word_count(path):
    cpp = table_rows(run([cpp_binary("//examples:word_count"), path]))
    py = table_rows(run([sys.executable, "examples/word_count.py", path]))
    assert cpp and len(cpp) == len(py), (cpp, py)
    # Same counts in the same order; words with a count tied at the cutoff may
    # differ, all others must match.
    assert [r[1] for r in cpp] == [r[1] for r in py], (cpp, py)
    last = cpp[-1][1]
    assert {tuple(r) for r in cpp if r[1] != last} == {tuple(r) for r in py if r[1] != last}
    print(f"word_count: {len(cpp)} rows match")


def sweep_results(output):
    """threshold -> numbers, from 'alpha_ratio >= t: [a,b,c,d] in N ms'."""
    results = {}
    for line in output.splitlines():
        m = re.match(r"alpha_ratio >= ([\d.]+): [\[(](.*)[\])] in \d+ ms", line)
        if m:
            results[float(m.group(1))] = [float(x) for x in m.group(2).split(",")]
    return results


def check_threshold_sweep(path):
    cpp = sweep_results(run([cpp_binary("//examples:threshold_sweep"), path]))
    py = sweep_results(run([sys.executable, "examples/threshold_sweep.py", path]))
    assert len(cpp) == 8 and cpp.keys() == py.keys(), (cpp, py)
    for threshold, values in cpp.items():
        assert all(math.isclose(a, b, rel_tol=1e-9) for a, b in zip(values, py[threshold])), (
            threshold, values, py[threshold])
    print(f"threshold_sweep: {len(cpp)} thresholds match")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    check_word_count(sys.argv[1])
    check_threshold_sweep(sys.argv[2])
