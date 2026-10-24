// DomainReport: per-domain statistics of a FineWeb-Edu style corpus (parquet
// with url, score, int_score and token_count columns), the kind of summary an
// agent looks at before choosing a data filter.
//
//   buck2 run //examples:domain_report -- <path> [master] [workers] [rows]
//
// For each web domain it counts all documents, and the highly educational
// ones (int_score >= 4; FineWeb-Edu itself keeps int_score >= 3), and reports
// how many documents and tokens that stricter filter would keep. Two
// aggregations over the same input are joined on the domain, so the job runs a
// scan stage, two shuffled aggregations and a shuffled join. master and workers are as in WordCount; 'rows' is the
// number of domains to print (default 20).

#include <iostream>

#include "EnvConfig.h"
#include "ember/Session.h"
#include "ember/functions.h"

using namespace ember;
using namespace ember::functions;

constexpr int64_t kMinScore = 4;

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: domain_report <file or directory> [master] [workers] [rows]"
              << std::endl;
    return 1;
  }

  auto builder = Session::builder();
  builder.appName("DomainReport")
      .master(argc > 2 ? argv[2] : "local[*]")
      .config("ember.cluster.workers", argc > 3 ? argv[3] : "1");
  auto ember = examples::withEnvConfig(builder).getOrCreate();

  // Host of the url, lower case, without a leading "www.".
  Dataframe docs = ember.read().parquet(argv[1]).select(
      lower(regexp_extract(
                col("url"), R"(^[A-Za-z][A-Za-z0-9+.-]*://(?:www\.)?([^/:?#]+))", 1))
          .as("domain"),
      col("score"),
      col("int_score"),
      col("token_count"));

  Dataframe all = docs.groupBy("domain").agg(
      countAll().as("docs"), sum(col("token_count")).as("tokens"));

  Dataframe edu = docs.filter(col("int_score") >= kMinScore)
                      .groupBy("domain")
                      .agg(
                          countAll().as("edu_docs"),
                          sum(col("token_count")).as("edu_tokens"),
                          avg(col("score")).as("edu_avg_score"));

  // Domains without educational documents have no row in 'edu'.
  Dataframe report =
      all.join(edu, {"domain"}, "left")
          .select(
              col("domain"),
              col("docs"),
              coalesce({col("edu_docs"), lit(int64_t{0})}).as("edu_docs"),
              round(
                  coalesce({col("edu_docs"), lit(int64_t{0})}).cast(
                      facebook::velox::DOUBLE()) /
                      col("docs").cast(facebook::velox::DOUBLE()),
                  3)
                  .as("keep_rate"),
              coalesce({col("edu_tokens"), lit(int64_t{0})}).as("edu_tokens"),
              col("tokens"),
              round(col("edu_avg_score"), 2).as("edu_avg_score"))
          .orderBy(col("edu_docs").desc(), col("docs").desc(), col("domain"));

  report.show(argc > 4 ? std::stoi(argv[4]) : 20, /*truncate=*/false);

  // Corpus-wide totals: what the int_score >= kMinScore filter keeps.
  docs.agg({
          countAll().as("docs"),
          sum(col("token_count")).as("tokens"),
          countDistinct(col("domain")).as("domains"),
          sum(call("if", col("int_score") >= kMinScore, lit(int64_t{1}), lit(int64_t{0})))
              .as("edu_docs"),
          sum(call("if", col("int_score") >= kMinScore, col("token_count"), lit(int64_t{0})))
              .as("edu_tokens"),
      })
      .show(1, /*truncate=*/false);

  ember.stop();
}
