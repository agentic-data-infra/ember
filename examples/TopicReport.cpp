// TopicReport: an LLM labels the documents of a FineWeb-Edu style corpus
// (parquet with text, int_score and token_count columns), and the program
// reports the topics it found. This is how an agent looks into data that no
// column describes.
//
//   buck2 run //examples:topic_report -- <path> <url> <model> [docs] [master]
//
// <url> is an OpenAI-compatible API, e.g. http://localhost:8080/v1 for
// llama.cpp's llama-server, and <model> the model name it serves. An endpoint
// that needs an API key gets it from $LLM_API_KEY, when that is set.
//
// Cheap filters come before the AI columns, so only [docs] documents (100 by
// default) reach the model. Each gets a topic and an "educational" flag, as
// typed fields; short ones also get a summary, and long ones make no second
// call. The labeled rows are cached, so the report and the summaries ask the
// model once.

#include <cstdlib>
#include <iostream>

#include "EnvConfig.h"
#include "ember/Session.h"
#include "ember/ai.h"
#include "ember/functions.h"

using namespace ember;
using namespace ember::functions;
namespace velox = facebook::velox;

int main(int argc, char** argv) {
  if (argc < 4) {
    std::cerr << "Usage: topic_report <file or directory> <url> <model> [docs] "
                 "[master]"
              << std::endl;
    return 1;
  }
  const int64_t docs = argc > 4 ? std::stoll(argv[4]) : 100;

  auto builder = Session::builder();
  builder.appName("TopicReport")
      .master(argc > 5 ? argv[5] : "local[*]")
      .config("ember.ai.endpoint.llm.url", argv[2])
      .config("ember.ai.endpoint.llm.model", argv[3]);
  if (std::getenv("LLM_API_KEY") != nullptr) {
    builder.config("ember.ai.endpoint.llm.api_key_env", "LLM_API_KEY");
  }
  auto ember = examples::withEnvConfig(builder).getOrCreate();

  auto text = substring(col("text"), 1, 2000);
  auto labeled =
      ember.read()
          .parquet(argv[1])
          .filter(col("int_score") >= 3 && col("token_count") < 1000)
          .limit(docs)
          .withColumn(
              "label",
              ai::generate(
                  concat({lit("Pick the topic of this text from: science, "
                              "health, history, technology, society, nature, "
                              "other. Also say whether it teaches something. "
                              "Text: "),
                          text}),
                  "llm",
                  velox::ROW(
                      {"topic", "educational"},
                      {velox::VARCHAR(), velox::BOOLEAN()})))
          .withColumn(
              "summary",
              when(col("token_count") < 300,
                   ai::generate(
                       concat({lit("Summarize in at most 10 words: "), text}),
                       "llm")
                       .getField("result"))
                  .otherwise(lit("")))
          .cache();

  const auto before = ember.stats();
  labeled
      .filter(col("label").getField("status") == "")
      .withColumn("topic", lower(col("label").getField("topic")))
      .groupBy("topic")
      .agg({countAll().as("docs"),
            sum(col("label").getField("educational").cast(velox::BIGINT()))
                .as("educational")})
      .orderBy(col("docs").desc(), col("topic").asc())
      .show(20);
  labeled.filter(col("summary") != "")
      .select(col("token_count"), col("summary"))
      .show(5, false);
  std::cout << "Failed rows: "
            << labeled.filter(col("label").getField("status") != "").count()
            << std::endl;

  std::cout << "Model use:";
  for (const auto& [name, total] : ember.stats()) {
    const auto it = before.find(name);
    std::cout << " " << name << "="
              << total - (it == before.end() ? 0 : it->second);
  }
  std::cout << std::endl;
  ember.stop();
}
