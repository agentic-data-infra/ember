// WordCount end to end: text files -> Dataframe program -> Axiom plan ->
// Velox, checked against a shell pipeline over the same files.

#include <gtest/gtest.h>

#include "ember/tests/WordCountUtil.h"

namespace ember {
namespace {

using namespace ember::functions;
using test::shellWordCount;
using test::toMap;
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

  static Dataframe wordCount(const std::string& path) {
    return test::wordCount(*ember_, path);
  }

  void writeCorpus(int32_t numFiles, int32_t linesPerFile) {
    test::writeCorpus(dir_, numFiles, linesPerFile);
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
