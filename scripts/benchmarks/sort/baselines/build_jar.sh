#!/usr/bin/env bash
# Compiles SortBench.scala against Spark's jars into $SORTBENCH_DATA/sortbench.jar.
# Needs SPARK_HOME (Spark 3.5.5, Scala 2.12) and SCALA_COMPILER_JAR (scala-compiler-2.12.18.jar, from Maven Central).
set -euo pipefail
source "$(dirname "$0")/../lib.sh"
classes=$(mktemp -d)
trap 'rm -rf "$classes"' EXIT
java -cp "$SCALA_COMPILER_JAR:$SPARK_HOME/jars/*" scala.tools.nsc.Main -usejavacp -d "$classes" \
  "$BENCH/baselines/SortBench.scala"
jar cf "$DATA/sortbench.jar" -C "$classes" .
echo "$DATA/sortbench.jar"
