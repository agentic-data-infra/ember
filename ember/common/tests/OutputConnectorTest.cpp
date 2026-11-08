#include <filesystem>

#include <gtest/gtest.h>

#include "ember/OutputConnector.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace ember {
namespace {

namespace fs = std::filesystem;
namespace velox = facebook::velox;

class OutputConnectorTest : public testing::Test,
                            public velox::test::VectorTestBase {
 protected:
  static void SetUpTestSuite() {
    velox::memory::MemoryManager::testingSetInstance(
        velox::memory::MemoryManager::Options{});
    velox::filesystems::registerLocalFileSystem();
    velox::parquet::registerParquetWriterFactory();
  }

  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_output_{}", ::getpid());
    fs::remove_all(dir_);
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  fs::path dir_;
};

// A Parquet part's buffered row group can be reclaimed while no write is in
// progress: the writer writes it out early, and the part keeps every row.
TEST_F(OutputConnectorTest, parquetRowGroupIsReclaimable) {
  auto root = velox::memory::memoryManager()->addRootPool(
      "query",
      velox::memory::kMaxMemory,
      velox::memory::MemoryReclaimer::create());
  auto connectorPool = root->addAggregateChild(
      "connector", velox::exec::MemoryReclaimer::create());
  auto operatorPool = root->addLeafChild("operator");
  const velox::config::ConfigBase session{
      std::unordered_map<std::string, std::string>{}};
  velox::connector::ConnectorQueryCtx ctx(
      operatorPool.get(),
      connectorPool.get(),
      &session,
      nullptr,
      velox::common::PrefixSortConfig(),
      nullptr,
      nullptr,
      "query",
      "ember://OutputConnectorTest_1/1.0/3.0",
      "0",
      0,
      "");
  OutputConnector connector;
  auto handle = std::make_shared<OutputInsertTableHandle>(
      dir_.string(),
      "parquet",
      std::map<std::string, std::string>{},
      kiln::kDefaultBucket);
  const auto type = velox::ROW({"key", "payload"}, {velox::BIGINT(), velox::VARCHAR()});
  auto sink = connector.createDataSink(
      type, handle, &ctx, velox::connector::CommitStrategy::kNoCommit);

  constexpr int kRows = 100'000;
  auto rows = [&](int first) {
    return makeRowVector(
        {"key", "payload"},
        {makeFlatVector<int64_t>(kRows, [&](auto i) { return first + i; }),
         makeFlatVector<std::string>(
             kRows, [](auto i) { return std::string(100, 'a' + i % 26); })});
  };
  sink->appendData(rows(0));
  const auto reclaimable = root->reclaimableBytes();
  ASSERT_TRUE(reclaimable.has_value());
  EXPECT_GT(*reclaimable, 0);
  const auto before = root->reservedBytes();
  velox::memory::MemoryReclaimer::Stats stats;
  EXPECT_GT(root->reclaim(0, 0, stats), 0);
  EXPECT_LT(root->reservedBytes(), before);
  EXPECT_EQ(stats.numNonReclaimableAttempts, 0);

  sink->appendData(rows(kRows));
  ASSERT_TRUE(sink->finish());
  const auto parts = sink->close();
  ASSERT_EQ(parts.size(), 1);
  const auto part = folly::parseJson(parts[0]);
  EXPECT_EQ(part["rows"].asInt(), 2 * kRows);

  // Two row groups: the reclaimed one and the rest.
  std::vector<fs::path> files;
  for (const auto& entry : fs::directory_iterator(dir_)) {
    files.push_back(entry.path());
  }
  ASSERT_EQ(files.size(), 1);
  velox::dwio::common::ReaderOptions options(pool());
  velox::parquet::ParquetReader reader(
      std::make_unique<velox::dwio::common::BufferedInput>(
          std::make_shared<velox::LocalReadFile>(files[0].string()),
          options.memoryPool()),
      options);
  EXPECT_EQ(reader.numberOfRows(), 2 * kRows);
  EXPECT_EQ(reader.fileMetaData().numRowGroups(), 2);
}

} // namespace
} // namespace ember
