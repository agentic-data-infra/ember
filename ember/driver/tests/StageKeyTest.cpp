// Stage keys: the same work has the same key, any change keys the stage it is
// in and everything downstream anew, and nondeterministic plans have none.

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>

#include "ember/Session.h"
#include "ember/driver/StageKey.h"
#include "ember/functions.h"
#include "ember/tests/WordCountUtil.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/vector/FlatVector.h"

namespace ember::driver {
namespace {

using namespace ember::functions;
namespace fs = std::filesystem;
namespace velox = facebook::velox;

using Keys = std::vector<std::optional<std::string>>;

class StageKeyTest : public testing::Test {
 protected:
  void SetUp() override {
    ember_ = Session::builder()
                 .appName("StageKeyTest")
                 .master("local[4]")
                 .getOrCreate();
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_stage_key_test_{}", ::getpid());
    fs::remove_all(dir_);
    fs::create_directories(dir_ / "text");
    test::writeCorpus(dir_ / "text", 2, 50);
    rootPool_ = velox::memory::memoryManager()->addRootPool();
    pool_ = rootPool_->addLeafChild("plans");
  }

  void TearDown() override {
    ember_->stop();
    ember_.reset();
    fs::remove_all(dir_);
  }

  StagePlan stages(const Dataframe& df) {
    return planStages(*df.physicalPlan(), pool_.get());
  }

  Keys keys(const Dataframe& df) {
    Keys result;
    for (const auto& stage : stages(df).stages) {
      result.push_back(stage.key);
    }
    return result;
  }

  std::string textPath() const {
    return (dir_ / "text").string() + "/";
  }

  Dataframe wordCount() const {
    return test::wordCount(*ember_, textPath());
  }

  Dataframe people() const {
    return ember_->createDataframe(
        velox::ROW(
            {"name", "dept", "salary", "bonus"},
            {velox::VARCHAR(), velox::VARCHAR(), velox::BIGINT(), velox::BIGINT()}),
        {{"alice", "eng", 100L, 10L},
         {"bob", "eng", 80L, 5L},
         {"carol", "sales", 70L, 7L},
         {"erin", "hr", 60L, 1L}});
  }

  // Writes FineWeb-Edu style rows (url, score, int_score, token_count).
  std::string writeDocs(int64_t numRows) {
    auto writePool = rootPool_->addLeafChild("write");
    auto type = velox::ROW(
        {"url", "score", "int_score", "token_count"},
        {velox::VARCHAR(), velox::DOUBLE(), velox::BIGINT(), velox::BIGINT()});
    auto data = std::dynamic_pointer_cast<velox::RowVector>(
        velox::BaseVector::create(type, numRows, writePool.get()));
    for (int64_t i = 0; i < numRows; ++i) {
      auto url = fmt::format("https://www.host{}.org/{}", i % 7, i);
      data->childAt(0)->asFlatVector<velox::StringView>()->set(
          i, velox::StringView(url));
      data->childAt(1)->asFlatVector<double>()->set(i, (i % 50) / 10.0);
      data->childAt(2)->asFlatVector<int64_t>()->set(i, i % 5);
      data->childAt(3)->asFlatVector<int64_t>()->set(i, 100 + i);
    }
    auto path = (dir_ / "docs.parquet").string();
    velox::dwio::common::WriterOptions options;
    options.memoryPool = rootPool_.get();
    velox::parquet::Writer writer(
        std::make_unique<velox::dwio::common::LocalFileSink>(
            path,
            velox::dwio::common::FileSink::Options{.pool = writePool.get()}),
        options,
        type);
    writer.write(data);
    writer.close();
    return path;
  }

  // The plan of examples/DomainReport.cpp: two aggregations of one scan,
  // joined.
  Dataframe domainReport(const std::string& path) const {
    auto docs = ember_->read().parquet(path).select(
        lower(regexp_extract(
                  col("url"),
                  R"(^[A-Za-z][A-Za-z0-9+.-]*://(?:www\.)?([^/:?#]+))",
                  1))
            .as("domain"),
        col("score"),
        col("int_score"),
        col("token_count"));
    auto all = docs.groupBy("domain").agg(
        countAll().as("docs"), sum(col("token_count")).as("tokens"));
    auto edu = docs.filter(col("int_score") >= 4)
                   .groupBy("domain")
                   .agg(
                       countAll().as("edu_docs"),
                       avg(col("score")).as("edu_avg_score"));
    return all.join(edu, {"domain"}, "left")
        .select(
            col("domain"),
            col("docs"),
            coalesce({col("edu_docs"), lit(int64_t{0})}).as("edu_docs"),
            round(col("edu_avg_score"), 2).as("edu_avg_score"))
        .orderBy(col("edu_docs").desc(), col("domain"));
  }

  // Expects 'a' and 'b' to have the same stages and keys, except for the
  // stages at and after position 'firstChanged'.
  static void expectChangedFrom(
      const Keys& a,
      const Keys& b,
      size_t firstChanged) {
    ASSERT_EQ(a.size(), b.size());
    for (size_t i = 0; i < a.size(); ++i) {
      SCOPED_TRACE(fmt::format("stage {}", i));
      ASSERT_TRUE(a[i].has_value());
      ASSERT_TRUE(b[i].has_value());
      if (i < firstChanged) {
        EXPECT_EQ(*a[i], *b[i]);
      } else {
        EXPECT_NE(*a[i], *b[i]);
      }
    }
  }

  std::optional<Session> ember_;
  fs::path dir_;
  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(StageKeyTest, sameWorkSameKeys) {
  auto df = wordCount();
  auto first = keys(df);
  ASSERT_EQ(first.size(), 3);
  for (const auto& key : first) {
    ASSERT_TRUE(key.has_value());
    EXPECT_EQ(key->size(), 32);
  }
  // Distinct stages, distinct keys.
  EXPECT_NE(*first[0], *first[1]);
  EXPECT_NE(*first[1], *first[2]);

  // The same plan again, the same program as a new Dataframe, and in a new
  // session with the same parallelism (which sets the shuffle's partitions).
  EXPECT_EQ(keys(df), first);
  EXPECT_EQ(keys(wordCount()), first);
  ember_->stop();
  ember_ = Session::builder()
               .appName("StageKeyTest2")
               .master("local[4]")
               .getOrCreate();
  EXPECT_EQ(keys(wordCount()), first);
}

TEST_F(StageKeyTest, changesKeyTheirStageAndDownstream) {
  // Stages: scan to partial count | final count and sort | merge.
  const auto base = keys(wordCount());
  auto words = [&](const Column& word, const Column& filter) {
    return ember_->read()
        .text(textPath())
        .select(explode(split(word, R"(\W+)")).as("word"))
        .filter(filter)
        .groupBy("word")
        .count();
  };

  // Literal, function and column before the shuffle.
  expectChangedFrom(
      base,
      keys(words(lower(col("value")), col("word") != "the")
               .orderBy(col("count").desc())),
      0);
  expectChangedFrom(
      base,
      keys(words(upper(col("value")), col("word") != "")
               .orderBy(col("count").desc())),
      0);
  expectChangedFrom(
      keys(people().groupBy("dept").agg(sum(col("salary")).as("total"))),
      keys(people().groupBy("dept").agg(sum(col("bonus")).as("total"))),
      0);

  // The sort, after the shuffle.
  expectChangedFrom(
      base,
      keys(words(lower(col("value")), col("word") != "")
               .orderBy(col("count").asc())),
      1);

  // Only the result's column names, which belong to the user.
  expectChangedFrom(
      base,
      keys(wordCount().select(col("word").as("w"), col("count"))),
      2);
}

TEST_F(StageKeyTest, inputFilesKeyTheirScans) {
  const auto base = keys(wordCount());
  const auto file = dir_ / "text" / "part-00000.txt";

  fs::last_write_time(file, fs::last_write_time(file) + std::chrono::seconds(5));
  const auto touched = keys(wordCount());
  expectChangedFrom(base, touched, 0);

  std::ofstream(file, std::ios::app) << "one more line\n";
  expectChangedFrom(touched, keys(wordCount()), 0);
}

TEST_F(StageKeyTest, nondeterministicPlansHaveNoKey) {
  auto lines = ember_->read().text(textPath());

  // rand() before the shuffle: no stage has a key.
  auto before = stages(lines.filter(call("rand") < lit(0.5))
                           .groupBy("value")
                           .count());
  ASSERT_GE(before.stages.size(), 2) << before.toString();
  for (const auto& stage : before.stages) {
    EXPECT_FALSE(stage.key.has_value()) << before.toString();
  }
  EXPECT_NE(before.stages[0].noKeyReason.find("rand"), std::string::npos)
      << before.stages[0].noKeyReason;
  EXPECT_NE(before.toString().find("[no key: "), std::string::npos);

  // After the shuffle: the map stage keeps its key.
  auto after = stages(lines.groupBy("value").count().select(
      col("value"), (col("count") * call("rand")).as("x")));
  ASSERT_GE(after.stages.size(), 2) << after.toString();
  EXPECT_TRUE(after.stages[0].key.has_value()) << after.toString();
  EXPECT_FALSE(after.resultStage().key.has_value()) << after.toString();
}

TEST_F(StageKeyTest, domainReportKeysAreStable) {
  auto path = writeDocs(1000);
  auto plan = stages(domainReport(path));
  ASSERT_GE(plan.stages.size(), 4) << plan.toString();
  Keys first;
  for (const auto& stage : plan.stages) {
    ASSERT_TRUE(stage.key.has_value()) << stage.noKeyReason;
    if (stage.shuffleId.has_value()) {
      EXPECT_EQ(stage.nodeKeys.at(stage.plan->id()), *stage.key);
    }
    first.push_back(stage.key);
  }
  EXPECT_EQ(keys(domainReport(path)), first);

  // explain() shows the keys.
  auto explain = domainReport(path).explainString();
  for (const auto& key : first) {
    EXPECT_NE(explain.find("[key " + shortKey(*key) + "]"), std::string::npos)
        << explain;
  }

  // Keying is cheap enough for every job.
  constexpr int32_t kRuns = 100;
  auto start = std::chrono::steady_clock::now();
  for (int32_t i = 0; i < kRuns; ++i) {
    computeStageKeys(plan);
  }
  auto perStage = std::chrono::duration<double, std::micro>(
                      std::chrono::steady_clock::now() - start) /
      (kRuns * plan.stages.size());
  LOG(INFO) << "Keying a DomainReport stage takes " << perStage.count()
            << " us";
  EXPECT_LT(perStage.count(), 1000);
  EXPECT_EQ(plan.stages.back().key, first.back());
}

} // namespace
} // namespace ember::driver
