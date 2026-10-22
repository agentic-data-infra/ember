// Dataframe::cache(): results, plans and stage runs of cached Dataframes.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "ember/Session.h"
#include "ember/driver/DagScheduler.h"
#include "ember/functions.h"

namespace ember {
namespace {

using namespace ember::functions;
namespace fs = std::filesystem;

class CacheTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    ember_ = Session::builder()
                 .appName("CacheTest")
                 .master("local[4]")
                 .onStageCompleted([](const driver::StageAttemptInfo& info) {
                   std::lock_guard<std::mutex> l(mutex_);
                   if (info.stageKey.has_value()) {
                     ranTasks_[*info.stageKey] += info.partitions.size();
                   }
                   totalTasks_ += info.partitions.size();
                 })
                 .getOrCreate();
  }

  static void TearDownTestCase() {
    ember_->stop();
    ember_.reset();
  }

  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_cache_test_{}", ::getpid());
    fs::remove_all(dir_);
    fs::create_directories(dir_);
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  // 'numFiles' files of numbered lines; returns the directory.
  std::string writeLines(int32_t numFiles, int32_t linesPerFile) {
    for (int32_t f = 0; f < numFiles; ++f) {
      std::ofstream out(dir_ / fmt::format("part-{}.txt", f));
      for (int32_t l = 0; l < linesPerFile; ++l) {
        out << "line " << f << " " << l << std::string(l % 17, 'x') << "\n";
      }
    }
    return dir_.string() + "/";
  }

  static int64_t totalTasks() {
    std::lock_guard<std::mutex> l(mutex_);
    return totalTasks_;
  }

  static std::vector<std::string> sorted(const std::vector<Row>& rows) {
    std::vector<std::string> result;
    for (const auto& row : rows) {
      result.push_back(row.toString());
    }
    std::sort(result.begin(), result.end());
    return result;
  }

  static std::optional<Session> ember_;
  static std::mutex mutex_;
  static std::map<std::string, int64_t> ranTasks_;
  static int64_t totalTasks_;
  fs::path dir_;
};

std::optional<Session> CacheTest::ember_;
std::mutex CacheTest::mutex_;
std::map<std::string, int64_t> CacheTest::ranTasks_;
int64_t CacheTest::totalTasks_{0};

TEST_F(CacheTest, readsLikeTheCachedDataframe) {
  auto lines = ember_->read().text(writeLines(3, 50)).select(
      length(col("value")).as("len"), upper(col("value")).as("upper"));
  auto cached = lines.cache();
  EXPECT_EQ(cached.schema()->toString(), lines.schema()->toString());
  EXPECT_EQ(sorted(cached.collect()), sorted(lines.collect()));
  EXPECT_EQ(cached.count(), 150);
  EXPECT_EQ(
      sorted(cached.filter(col("len") > lit(int64_t{12}))
                 .select(col("upper"))
                 .collect()),
      sorted(lines.filter(col("len") > lit(int64_t{12}))
                 .select(col("upper"))
                 .collect()));

  // A cached plan that ends in one task (here a sort) keeps its order.
  auto ordered = lines.orderBy(col("len").desc(), col("upper")).cache();
  EXPECT_EQ(
      ordered.collect(),
      lines.orderBy(col("len").desc(), col("upper")).collect());

  // Joins and unions of cached Dataframes, with themselves too.
  auto counts = cached.groupBy("len").count().cache();
  EXPECT_EQ(
      sorted(cached.join(counts, {"len"}).collect()),
      sorted(lines.join(lines.groupBy("len").count(), {"len"}).collect()));
  EXPECT_EQ(cached.unionAll(cached).count(), 300);
  EXPECT_EQ(cached.unionAll(lines).count(), 300);
}

// After the first action, actions on the cached Dataframe run no tasks for
// it; unpersist() drops the stored result.
TEST_F(CacheTest, computesOnceUntilUnpersisted) {
  auto lines = ember_->read().text(writeLines(4, 50)).select(
      upper(col("value")).as("upper"));
  auto cached = lines.cache();
  // The plan reading it runs a task per stored part, and one to gather.
  auto explain = cached.explainString();
  EXPECT_NE(explain.find("not stored yet"), std::string::npos) << explain;
  EXPECT_EQ(cached.count(), 200);
  explain = cached.explainString();
  EXPECT_NE(explain.find("(stored)"), std::string::npos) << explain;

  auto before = totalTasks();
  EXPECT_EQ(cached.count(), 200);
  const auto readTasks = totalTasks() - before;

  // Dropped, the next action computes it again: one task per file more.
  cached.unpersist();
  before = totalTasks();
  EXPECT_EQ(cached.count(), 200);
  EXPECT_EQ(totalTasks() - before, readTasks + 4);

  // unpersist() on other Dataframes does nothing.
  lines.unpersist();
  before = totalTasks();
  EXPECT_EQ(cached.count(), 200);
  EXPECT_EQ(totalTasks() - before, readTasks);
}

// A nondeterministic cached Dataframe has no key, but is computed once all the
// same: every action sees the same rows.
TEST_F(CacheTest, freezesNondeterministicResults) {
  auto random = ember_->read()
                    .text(writeLines(2, 100))
                    .select(call("rand", std::vector<Column>{}).as("r"));
  auto cached = random.cache();
  auto first = cached.agg({sum(col("r")).as("s")}).collect();
  auto second = cached.agg({sum(col("r")).as("s")}).collect();
  EXPECT_EQ(first, second);
  EXPECT_NE(
      random.agg({sum(col("r")).as("s")}).collect(),
      random.agg({sum(col("r")).as("s")}).collect());
}

} // namespace
} // namespace ember
