// dataframe.write().parquet(): every task writes its rows to a part with
// Velox's Parquet writer, read back with read().parquet(). Parts of a
// range-partitioned sort hold the sorted rows in name order.

#include <folly/ScopeGuard.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <random>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "ember/OutputConnector.h"
#include "ember/Session.h"
#include "ember/driver/DagScheduler.h"
#include "ember/functions.h"

namespace ember {
namespace {

using namespace ember::functions;
namespace fs = std::filesystem;
namespace velox = facebook::velox;

namespace axiom_connector = facebook::axiom::connector;

// Records the write stats a commit gets.
class StatsSpy : public OutputConnectorMetadata {
 public:
  axiom_connector::RowsFuture finishWrite(
      const axiom_connector::ConnectorSessionPtr& session,
      const axiom_connector::ConnectorWriteHandlePtr& handle,
      const std::vector<velox::RowVectorPtr>& writeResults,
      velox::RowVectorPtr groupingKeys,
      std::vector<std::vector<axiom_connector::ColumnStatistics>> groupStats)
      override {
    stats = groupStats;
    return OutputConnectorMetadata::finishWrite(
        session,
        handle,
        writeResults,
        std::move(groupingKeys),
        std::move(groupStats));
  }

  std::vector<std::vector<axiom_connector::ColumnStatistics>> stats;
};

class ParquetWriteTest : public testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_parquet_write_test_{}", ::getpid());
    fs::remove_all(dir_);
    fs::create_directories(input());
  }

  void TearDown() override {
    if (ember_.has_value()) {
      ember_->stop();
    }
    fs::remove_all(dir_);
  }

  Session& session(std::map<std::string, std::string> config = {}) {
    auto builder = Session::builder();
    builder.appName("ParquetWriteTest").master("local[4]");
    for (const auto& [key, value] : config) {
      builder.config(key, value);
    }
    ember_ = builder.getOrCreate();
    return *ember_;
  }

  fs::path input() const {
    return dir_ / "input";
  }

  fs::path output() const {
    return dir_ / "output";
  }

  // Writes 'numFiles' gensort files of 'recordsPerFile' random records;
  // returns the records, sorted.
  std::vector<std::string> writeInput(
      int32_t numFiles,
      int32_t recordsPerFile) {
    std::mt19937 random(42);
    std::vector<std::string> records;
    for (int32_t f = 0; f < numFiles; ++f) {
      std::ofstream out(input() / fmt::format("in-{}", f), std::ios::binary);
      for (int32_t r = 0; r < recordsPerFile; ++r) {
        std::string record(100, '\0');
        for (auto& c : record) {
          c = static_cast<char>(random());
        }
        out.write(record.data(), record.size());
        records.push_back(std::move(record));
      }
    }
    std::sort(records.begin(), records.end());
    return records;
  }

  // The names in the output directory, sorted.
  std::vector<std::string> outputNames() const {
    std::vector<std::string> names;
    for (const auto& entry : fs::directory_iterator(output())) {
      names.push_back(entry.path().filename().string());
    }
    std::sort(names.begin(), names.end());
    return names;
  }

  // Key and payload of each row, as one record.
  static std::vector<std::string> records(const std::vector<Row>& rows) {
    std::vector<std::string> records;
    for (const auto& row : rows) {
      records.push_back(
          row[0].value<velox::TypeKind::VARBINARY>() +
          row[1].value<velox::TypeKind::VARBINARY>());
    }
    return records;
  }

  std::optional<Session> ember_;
  fs::path dir_;
};

TEST_F(ParquetWriteTest, sortedWriteRunsWritersInRangeTasks) {
  auto expected = writeInput(3, 1000);
  std::vector<std::vector<int32_t>> stages;
  ember_ = Session::builder()
               .appName("ParquetWriteTest")
               .master("local[4]")
               .config("ember.sort.range_partitions", "6")
               .onStageCompleted([&](const driver::StageAttemptInfo& stage) {
                 stages.push_back(stage.partitions);
               })
               .getOrCreate();

  auto& registry = axiom_connector::ConnectorMetadataRegistry::global();
  const std::string connectorId(kOutputConnectorId);
  auto original = axiom_connector::ConnectorMetadataRegistry::tryGet(connectorId);
  auto spy = std::make_shared<StatsSpy>();
  registry.insert(connectorId, spy, /*overwrite=*/true);
  SCOPE_EXIT {
    registry.insert(connectorId, original, /*overwrite=*/true);
  };

  auto written = ember_->read()
                     .gensort(input().string())
                     .orderBy(col("key"))
                     .write()
                     .parquet(output().string());
  EXPECT_EQ(written, 3000);

  // Six tasks sort and write a range each, and one merges their stats.
  ASSERT_EQ(stages.size(), 3);
  EXPECT_EQ(stages[1].size(), 6);
  EXPECT_EQ(stages[2].size(), 1);
  ASSERT_EQ(spy->stats.size(), 1);
  EXPECT_FALSE(spy->stats[0].empty());
}

TEST_F(ParquetWriteTest, writesSortedPartsInOrder) {
  auto expected = writeInput(3, 1000);
  auto& ember = session({{"ember.sort.range_partitions", "4"}});
  auto written = ember.read()
                     .gensort(input().string())
                     .orderBy(col("key"))
                     .write()
                     .parquet(output().string());
  EXPECT_EQ(written, 3000);

  // One part per range and the marker, and nothing else (no temporary
  // files).
  const std::vector<std::string> parts{
      "part-00000.parquet",
      "part-00001.parquet",
      "part-00002.parquet",
      "part-00003.parquet"};
  auto names = parts;
  names.insert(names.begin(), "_SUCCESS");
  EXPECT_EQ(outputNames(), names);
  std::vector<std::string> sorted;
  for (const auto& name : parts) {
    auto part =
        records(ember.read().parquet((output() / name).string()).collect());
    EXPECT_FALSE(part.empty());
    EXPECT_TRUE(std::is_sorted(part.begin(), part.end()));
    sorted.insert(sorted.end(), part.begin(), part.end());
  }
  EXPECT_EQ(sorted, expected);

  auto all = ember.read().parquet(output().string());
  EXPECT_EQ(all.columns(), (std::vector<std::string>{"key", "payload"}));
  EXPECT_EQ(all.count(), 3000);
}

TEST_F(ParquetWriteTest, roundTripsColumnTypes) {
  writeInput(2, 500);
  auto& ember = session();
  auto df = ember.read().gensort(input().string()).select(
      col("key"),
      lit(int64_t{90}).as("len"),
      lit(45.0).as("half"),
      lit("x").as("tag"));
  EXPECT_EQ(df.write().parquet(output().string()), 1000);

  auto back = ember.read().parquet(output().string());
  EXPECT_EQ(
      back.columns(), (std::vector<std::string>{"key", "len", "half", "tag"}));
  auto rows = back.orderBy(col("key")).collect();
  ASSERT_EQ(rows.size(), 1000);
  for (const auto& row : rows) {
    EXPECT_EQ(row.getLong(1), 90);
    EXPECT_EQ(row.get<double>(2), 45.0);
    EXPECT_EQ(row.getString(3), "x");
  }
}

TEST_F(ParquetWriteTest, writesNoPartsForNoRows) {
  writeInput(2, 100);
  auto written = session()
                     .read()
                     .gensort(input().string())
                     .filter(col("key") == col("payload"))
                     .write()
                     .parquet(output().string());
  EXPECT_EQ(written, 0);
  EXPECT_EQ(outputNames(), std::vector<std::string>{"_SUCCESS"});
}

TEST_F(ParquetWriteTest, failedWriteLeavesNoParts) {
  writeInput(3, 1000);
  // Not whole gensort records: the task reading it fails the job, while the
  // others may have written their parts.
  std::ofstream(input() / "in-bad", std::ios::binary) << std::string(150, 'x');
  EXPECT_ANY_THROW(session()
                       .read()
                       .gensort(input().string())
                       .write()
                       .parquet(output().string()));
  if (fs::exists(output())) {
    EXPECT_TRUE(outputNames().empty());
  }
}

TEST_F(ParquetWriteTest, rejectsExistingParts) {
  writeInput(1, 100);
  auto& ember = session();
  auto df = ember.read().gensort(input().string());
  EXPECT_EQ(df.write().parquet(output().string()), 100);
  EXPECT_THROW(df.write().parquet(output().string()), velox::VeloxUserError);
  EXPECT_EQ(ember.read().parquet(output().string()).count(), 100);
}

} // namespace
} // namespace ember
