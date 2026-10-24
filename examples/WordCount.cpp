// DataframeWordCount: counts the words of a text file (or a directory of text
// files).
//
//   buck2 run //examples:word_count -- <path> [master] [workers]
//
// master defaults to local[*]. With ember://host:port this program is the
// driver and waits for 'workers' (default 1) ember-worker processes started
// with --driver=host:port.

#include <iostream>

#include "EnvConfig.h"
#include "ember/Session.h"
#include "ember/functions.h"

using namespace ember;
using namespace ember::functions;

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: word_count <file or directory> [master] [workers]"
              << std::endl;
    return 1;
  }

  auto builder = Session::builder();
  builder.appName("DataframeWordCount")
      .master(argc > 2 ? argv[2] : "local[*]")
      .config("ember.cluster.workers", argc > 3 ? argv[3] : "1");
  auto ember = examples::withEnvConfig(builder).getOrCreate();

  Dataframe lines = ember.read().text(argv[1]); // one column: "value"

  Dataframe words =
      lines.select(explode(split(lower(col("value")), R"(\W+)")).as("word"))
          .filter(col("word") != "");

  Dataframe counts =
      words.groupBy("word").count().orderBy(col("count").desc());

  counts.show(20, /*truncate=*/false);
  ember.stop();
}
