// Reading parquet files, written here with Velox's parquet writer.

#include <gtest/gtest.h>

#include <filesystem>

#include "ember/Session.h"
#include "ember/functions.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/vector/FlatVector.h"

namespace ember {
namespace {

using namespace ember::functions;
namespace velox = facebook::velox;
namespace fs = std::filesystem;

class ParquetTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    ember_ = Session::builder()
                 .appName("ParquetTest")
                 .master("local[4]")
                 .getOrCreate();
  }

  static void TearDownTestCase() {
    ember_->stop();
    ember_.reset();
  }

  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_parquet_{}", ::getpid());
    fs::remove_all(dir_);
    fs::create_directories(dir_);
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  // Writes 'numRows' rows (id bigint, url varchar, score bigint) with
  // url = "http://host<id % 3>/<id>" and score = id % 5.
  std::string writeDocs(int64_t numRows) {
    auto rootPool = velox::memory::memoryManager()->addRootPool();
    auto pool = rootPool->addLeafChild("write");
    auto ids = velox::BaseVector::create<velox::FlatVector<int64_t>>(
        velox::BIGINT(), numRows, pool.get());
    auto urls = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        velox::VARCHAR(), numRows, pool.get());
    auto scores = velox::BaseVector::create<velox::FlatVector<int64_t>>(
        velox::BIGINT(), numRows, pool.get());
    for (int64_t i = 0; i < numRows; ++i) {
      ids->set(i, i);
      auto url = fmt::format("http://host{}/{}", i % 3, i);
      urls->set(i, velox::StringView(url));
      scores->set(i, i % 5);
    }
    auto type = velox::ROW(
        {"id", "url", "score"},
        {velox::BIGINT(), velox::VARCHAR(), velox::BIGINT()});
    auto data = std::make_shared<velox::RowVector>(
        pool.get(),
        type,
        nullptr,
        numRows,
        std::vector<velox::VectorPtr>{ids, urls, scores});

    auto path = (dir_ / "docs.parquet").string();
    velox::dwio::common::WriterOptions options;
    options.memoryPool = rootPool.get();
    velox::parquet::Writer writer(
        std::make_unique<velox::dwio::common::LocalFileSink>(
            path, velox::dwio::common::FileSink::Options{.pool = pool.get()}),
        options,
        type);
    writer.write(data);
    writer.close();
    return path;
  }

  static inline std::optional<Session> ember_;
  fs::path dir_;
};

TEST_F(ParquetTest, readSelectFilter) {
  auto df = ember_->read().parquet(writeDocs(100));
  EXPECT_EQ(df.columns(), (std::vector<std::string>{"id", "url", "score"}));
  auto rows = df.filter(col("score") == 4)
                  .select(col("url"))
                  .orderBy(col("url"))
                  .limit(2)
                  .collect();
  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0].getString(0), "http://host0/24");
  EXPECT_EQ(rows[1].getString(0), "http://host0/39");
}

// The same file twice in one plan: Axiom renames the second scan's columns.
TEST_F(ParquetTest, joinOfTwoAggregationsOverOneFile) {
  auto docs = ember_->read().parquet(writeDocs(300)).select(
      regexp_extract(col("url"), R"(^http://([^/]+))", 1).as("host"),
      col("score"));
  auto all = docs.groupBy("host").agg(countAll().as("docs"));
  auto high = docs.filter(col("score") >= 3)
                  .groupBy("host")
                  .agg(countAll().as("high"), sum(col("score")).as("total"));
  auto rows = all.join(high, {"host"}).orderBy(col("host")).collect();

  ASSERT_EQ(rows.size(), 3);
  for (int64_t host = 0; host < 3; ++host) {
    // ids host, host + 3, ...: 100 of them, cycling through every score.
    int64_t high = 0;
    int64_t total = 0;
    for (int64_t id = host; id < 300; id += 3) {
      if (id % 5 >= 3) {
        ++high;
        total += id % 5;
      }
    }
    EXPECT_EQ(rows[host].getString(0), fmt::format("host{}", host));
    EXPECT_EQ(rows[host].getLong(1), 100);
    EXPECT_EQ(rows[host].getLong(2), high);
    EXPECT_EQ(rows[host].getLong(3), total);
  }
}

} // namespace
} // namespace ember
