// DataframeWordCount: counts the words of a text file (or a directory of text
// files).
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

  auto ember = Session::builder()
                   .appName("DataframeWordCount")
                   .master("local[*]")
                   .getOrCreate();

  Dataframe lines = ember.read().text(argv[1]); // one column: "value"

  Dataframe words =
      lines.select(explode(split(lower(col("value")), R"(\W+)")).as("word"))
          .filter(col("word") != "");

  Dataframe counts =
      words.groupBy("word").count().orderBy(col("count").desc());

  counts.show(20, /*truncate=*/false);
  ember.stop();
}
