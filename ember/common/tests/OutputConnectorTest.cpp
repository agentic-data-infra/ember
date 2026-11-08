#include <filesystem>

#include <folly/hash/Hash.h>
#include <gtest/gtest.h>

#include "ember/OutputConnector.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
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

  // Writes 'numRows' rows of about 110 bytes through an output data sink
  // whose connector session has 'properties', and returns the row groups of
  // the part.
  int64_t writeRowGroups(
      int numRows,
      std::unordered_map<std::string, std::string> properties) {
    auto root = velox::memory::memoryManager()->addRootPool("query");
    auto connectorPool = root->addAggregateChild("connector");
    auto operatorPool = root->addLeafChild("operator");
    const velox::config::ConfigBase session{std::move(properties)};
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
    const auto type =
        velox::ROW({"key", "payload"}, {velox::BIGINT(), velox::VARCHAR()});
    auto sink = connector.createDataSink(
        type, handle, &ctx, velox::connector::CommitStrategy::kNoCommit);
    constexpr int kBatch = 10'000;
    for (int first = 0; first < numRows; first += kBatch) {
      sink->appendData(makeRowVector(
          {"key", "payload"},
          {makeFlatVector<int64_t>(kBatch, [&](auto i) { return first + i; }),
           // Digits that Snappy does not shrink, as row groups count
           // compressed bytes.
           makeFlatVector<std::string>(kBatch, [&](auto i) {
             std::string payload;
             for (uint64_t h = first + i; payload.size() < 100;) {
               h = folly::hash::twang_mix64(h);
               payload += std::to_string(h);
             }
             return payload.substr(0, 100);
           })}));
    }
    EXPECT_TRUE(sink->finish());
    const auto parts = sink->close();
    EXPECT_EQ(parts.size(), 1);
    EXPECT_EQ(folly::parseJson(parts[0])["rows"].asInt(), numRows);

    std::vector<fs::path> files;
    for (const auto& entry : fs::directory_iterator(dir_)) {
      files.push_back(entry.path());
    }
    EXPECT_EQ(files.size(), 1);
    velox::dwio::common::ReaderOptions options(pool());
    velox::parquet::ParquetReader reader(
        std::make_unique<velox::dwio::common::BufferedInput>(
            std::make_shared<velox::LocalReadFile>(files[0].string()),
            options.memoryPool()),
        options);
    EXPECT_EQ(reader.numberOfRows(), numRows);
    return reader.fileMetaData().numRowGroups();
  }

  fs::path dir_;
};

// A task's share of memory sizes the Parquet row group: a quarter of it, so
// that 200k rows of ~110 bytes in 16 MB make row groups of 4 MB.
TEST_F(OutputConnectorTest, taskMemorySizesRowGroups) {
  EXPECT_GE(
      writeRowGroups(
          200'000, {{std::string(kTaskMemoryBytes), std::to_string(16 << 20)}}),
      4);
}

// Without a share, Velox's row group of 128 MB holds all of them.
TEST_F(OutputConnectorTest, defaultRowGroup) {
  EXPECT_EQ(writeRowGroups(200'000, {}), 1);
}

} // namespace
} // namespace ember
