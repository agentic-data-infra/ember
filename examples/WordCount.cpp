// DataFrameWordCount: counts the words of a text file (or a directory of text
// files), as Spark's JavaWordCount/DataFrame examples.
//
//   buck2 run //examples:word_count -- <path>

#include <iostream>

#include "ember/Session.h"
#include "ember/functions.h"

using namespace ember;
using namespace ember::functions;

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: word_count <file or directory>" << std::endl;
    return 1;
  }

  auto spark = Session::builder()
                   .appName("DataFrameWordCount")
                   .master("local[*]")
                   .getOrCreate();

  DataFrame lines = spark.read().text(argv[1]); // one column: "value"

  DataFrame words =
      lines.select(explode(split(lower(col("value")), R"(\W+)")).as("word"))
          .filter(col("word") != "");

  DataFrame counts =
      words.groupBy("word").count().orderBy(col("count").desc());

  counts.show(20, /*truncate=*/false);
  spark.stop();
}
