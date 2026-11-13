// Spark sort benchmark jobs for spark-submit:
//   SortBench rdd <gensort dir> <out dir> <partitions>   TeraSort on 100-byte records, raw records out
//   SortBench df  <parquet dir> <out dir>                 orderBy("key"), Parquet out (partitions: spark.sql.shuffle.partitions)
// Prints JOBTIME_MS (after session start) and, for df, the executed plan of the write.
import org.apache.spark.{RangePartitioner, TaskContext}
import org.apache.spark.sql.SparkSession

class Key(val b: Array[Byte]) extends Serializable

object KeyOrd extends Ordering[Key] with Serializable {
  // Unsigned lexicographic order on the 10-byte key.
  def compare(x: Key, y: Key): Int = {
    var i = 0
    while (i < 10) { val c = (x.b(i) & 0xff) - (y.b(i) & 0xff); if (c != 0) return c; i += 1 }
    0
  }
}

object SortBench {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName(s"SortBench-${args(0)}").getOrCreate()
    val (in, out) = (args(1), args(2))
    val t0 = System.nanoTime
    args(0) match {
      case "rdd" =>
        implicit val ord: Ordering[Key] = KeyOrd
        val recs = spark.sparkContext.binaryRecords(in, 100)
          .map(r => (new Key(java.util.Arrays.copyOfRange(r, 0, 10)), java.util.Arrays.copyOfRange(r, 10, 100)))
        val sorted = recs.repartitionAndSortWithinPartitions(new RangePartitioner(args(3).toInt, recs))
        new java.io.File(out).mkdirs()
        sorted.foreachPartition { it =>
          val os = new java.io.BufferedOutputStream(
            new java.io.FileOutputStream(f"$out/part-${TaskContext.getPartitionId}%05d"), 1 << 20)
          it.foreach { case (k, v) => os.write(k.b); os.write(v) }
          os.close()
        }
      case "df" =>
        spark.read.parquet(in).orderBy("key").write.mode("overwrite").parquet(out)
    }
    println(s"JOBTIME_MS ${(System.nanoTime - t0) / 1000000}")
    if (args(0) == "df") {
      println("PLAN_BEGIN")
      spark.sharedState.statusStore.executionsList.lastOption.foreach(e => println(e.physicalPlanDescription))
      println("PLAN_END")
    }
    spark.stop()
  }
}
