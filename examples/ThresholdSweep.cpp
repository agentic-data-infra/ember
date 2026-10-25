// ThresholdSweep: what a quality filter keeps, for a sweep of thresholds, over
// a FineWeb-Edu style corpus (parquet with url, text, int_score and
// token_count columns). This is the loop an agent runs to choose a filter.
//
//   buck2 run //examples:threshold_sweep -- <path> [cache|nocache] [master]
//
// Each document gets an expensive feature, the share of its text that is
// letters or spaces (a regex over the whole text), and the sweep keeps the
// documents at or above each threshold. With "cache" (the default) the
// features are cached, so only the first threshold computes them; with
// "nocache" every threshold reads and scores the corpus again, since the
// filter is pushed below the projection and no stage repeats.

#include <chrono>
#include <iostream>

#include "EnvConfig.h"
#include "ember/Session.h"
#include "ember/functions.h"

using namespace ember;
using namespace ember::functions;

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: threshold_sweep <file or directory> [cache|nocache] "
                 "[master]"
              << std::endl;
    return 1;
  }
  const bool useCache = argc < 3 || std::string(argv[2]) != "nocache";

  auto builder = Session::builder();
  builder.appName("ThresholdSweep")
      .master(argc > 3 ? argv[3] : "local[*]");
  auto ember = examples::withEnvConfig(builder).getOrCreate();

  auto text = col("text");
  Dataframe features = ember.read().parquet(argv[1]).select(
      lower(regexp_extract(
                col("url"), R"(^[A-Za-z][A-Za-z0-9+.-]*://(?:www\.)?([^/:?#]+))", 1))
          .as("domain"),
      col("int_score"),
      col("token_count"),
      (length(regexp_replace(text, "[^A-Za-z ]", "")).cast(facebook::velox::DOUBLE()) /
       length(text).cast(facebook::velox::DOUBLE()))
          .as("alpha_ratio"));
  if (useCache) {
    features = features.cache();
  }

  for (double threshold : {0.70, 0.75, 0.80, 0.82, 0.84, 0.86, 0.88, 0.90}) {
    const auto start = std::chrono::steady_clock::now();
    auto rows = features.filter(col("alpha_ratio") >= lit(threshold))
                    .agg({
                        countAll().as("docs"),
                        sum(col("token_count")).as("tokens"),
                        countDistinct(col("domain")).as("domains"),
                        avg(col("int_score")).as("avg_int_score"),
                    })
                    .collect();
    const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - start)
                        .count();
    std::cout << "alpha_ratio >= " << threshold << ": "
              << rows.at(0).toString() << " in " << ms << " ms" << std::endl;
  }

  ember.stop();
}
