// WordCount end to end: text files -> DataFrame program -> Axiom plan ->
// Velox, checked against a shell pipeline over the same files.

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>

#include "ember/Session.h"
#include "ember/functions.h"

namespace ember {
namespace {

using namespace ember::functions;
namespace fs = std::filesystem;

class WordCountTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    ember_ = Session::builder()
                 .appName("WordCountTest")
                 .master("local[4]")
                 .getOrCreate();
  }

  static void TearDownTestCase() {
    ember_->stop();
    ember_.reset();
  }

  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_wordcount_{}", ::getpid());
    fs::remove_all(dir_);
    fs::create_directories(dir_);
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  fs::path write(const std::string& name, const std::string& content) {
    auto path = dir_ / name;
    std::ofstream(path, std::ios::binary) << content;
    return path;
  }

  // The example program from examples/WordCount.cpp.
  static DataFrame wordCount(const std::string& path) {
    DataFrame lines = ember_->read().text(path);
    DataFrame words =
        lines.select(explode(split(lower(col("value")), R"(\W+)")).as("word"))
            .filter(col("word") != "");
    return words.groupBy("word").count().orderBy(col("count").desc());
  }

  static std::map<std::string, int64_t> toMap(const std::vector<Row>& rows) {
    std::map<std::string, int64_t> result;
    for (const auto& row : rows) {
      result[row.getString(0)] = row.getLong(1);
    }
    return result;
  }

  // Counts words with tr, sort and uniq -c. \W+ splits on anything but
  // [A-Za-z0-9_], which is what tr -cs keeps.
  static std::map<std::string, int64_t> shellWordCount(const fs::path& path) {
    auto command = fmt::format(
        "find '{}' -type f ! -name '_*' ! -name '.*' -exec cat {{}} + | "
        "LC_ALL=C tr -cs 'A-Za-z0-9_' '\\n' | LC_ALL=C tr 'A-Z' 'a-z' | "
        "grep -v '^$' | LC_ALL=C sort | uniq -c",
        path.string());
    std::map<std::string, int64_t> result;
    auto* pipe = ::popen(command.c_str(), "r");
    VELOX_CHECK_NOT_NULL(pipe);
    char word[4096];
    long count;
    while (::fscanf(pipe, "%ld %4095s", &count, word) == 2) {
      result[word] = count;
    }
    VELOX_CHECK_EQ(::pclose(pipe), 0);
    return result;
  }

  // A corpus of random words in mixed case with punctuation, blank lines and
  // CRLF line ends, spread over several files.
  void writeCorpus(int32_t numFiles, int32_t linesPerFile) {
    const std::vector<std::string> vocabulary = {
        "the", "quick", "Brown", "fox", "JUMPS", "over", "lazy", "dog",
        "ember", "velox", "axiom", "flare", "x1", "snake_case", "a"};
    const std::vector<std::string> separators = {
        " ", "  ", ", ", ". ", "!", " -- ", "\t", "'", "(", ") "};
    std::mt19937 rng(42);
    for (int32_t f = 0; f < numFiles; ++f) {
      std::string content;
      for (int32_t l = 0; l < linesPerFile; ++l) {
        const auto numWords = rng() % 12;
        for (uint32_t w = 0; w < numWords; ++w) {
          content += vocabulary[rng() % vocabulary.size()];
          content += separators[rng() % separators.size()];
        }
        content += (l % 7 == 0) ? "\r\n" : "\n";
      }
      write(fmt::format("part-{:05d}.txt", f), content);
    }
  }

  static inline std::optional<Session> ember_;
  fs::path dir_;
};

TEST_F(WordCountTest, matchesShellPipeline) {
  writeCorpus(/*numFiles=*/5, /*linesPerFile=*/2000);
  // Not data: skipped by the reader and by the reference.
  write("_SUCCESS", "");
  write(".hidden", "ignored words here\n");

  auto counts = wordCount(dir_.string());
  EXPECT_EQ(counts.columns(), (std::vector<std::string>{"word", "count"}));

  auto rows = counts.collect();
  auto expected = shellWordCount(dir_);
  ASSERT_FALSE(expected.empty());
  EXPECT_EQ(toMap(rows), expected);

  // Sorted by count, descending.
  for (size_t i = 1; i < rows.size(); ++i) {
    EXPECT_GE(rows[i - 1].getLong(1), rows[i].getLong(1));
  }
}

TEST_F(WordCountTest, singleFileWithEdgeCases) {
  auto path = write(
      "input.txt",
      "Hello, hello WORLD!\r\n"
      "\n"
      "   \n"
      "...leading punctuation\n"
      "no trailing newline world");
  auto rows = wordCount(path.string()).collect();
  EXPECT_EQ(
      toMap(rows),
      (std::map<std::string, int64_t>{
          {"hello", 2},
          {"world", 2},
          {"leading", 1},
          {"punctuation", 1},
          {"no", 1},
          {"trailing", 1},
          {"newline", 1}}));
  EXPECT_EQ(toMap(rows), shellWordCount(path));
}

TEST_F(WordCountTest, linesSpanningReadBuffers) {
  // Lines longer than and straddling the reader's 4 MB buffer.
  std::string content;
  int64_t expectedLines = 0;
  for (int32_t i = 0; i < 3; ++i) {
    content += std::string(3 << 20, 'a') + " b\n";
    content += "b\n";
    expectedLines += 2;
  }
  auto path = write("long.txt", content);

  auto lines = ember_->read().text(path.string());
  EXPECT_EQ(lines.count(), expectedLines);
  EXPECT_EQ(
      lines.filter(length(col("value")) > 1000).count(), 3);

  auto rows = wordCount(path.string()).collect();
  auto counts = toMap(rows);
  EXPECT_EQ(counts["b"], 6);
  EXPECT_EQ(counts[std::string(3 << 20, 'a')], 3);
}

TEST_F(WordCountTest, planHasMapAndReduceStages) {
  auto path = write("input.txt", "a b a\n");
  auto df = wordCount(path.string());
  auto plan = df.physicalPlan();
  // Scan, split, unnest and partial aggregation, then the final aggregation
  // after a shuffle on 'word', then the sort.
  EXPECT_GE(plan->fragments().size(), 2) << plan->toString();
  LOG(INFO) << df.explainString();
}

TEST_F(WordCountTest, emptyInput) {
  auto path = write("empty.txt", "");
  EXPECT_EQ(wordCount(path.string()).count(), 0);
  EXPECT_EQ(ember_->read().text(path.string()).count(), 0);
}

} // namespace
} // namespace ember
