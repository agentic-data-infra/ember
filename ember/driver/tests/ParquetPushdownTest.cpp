// Filters reach the Parquet reader: the plans Axiom makes for Parquet scans
// run as they are in a Velox task, so the scan's statistics show what the
// reader skipped.

#include <filesystem>

#include <gtest/gtest.h>

#include "ember/ParquetConnector.h"
#include "ember/Session.h"
#include "ember/driver/StagePlan.h"
#include "ember/functions.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace ember::driver {
namespace {

namespace core = facebook::velox::core;
namespace exec = facebook::velox::exec;
namespace fs = std::filesystem;
namespace hive = facebook::velox::connector::hive;
namespace velox = facebook::velox;

using functions::col;
using functions::lit;

constexpr int64_t kRowGroups = 10;
constexpr int64_t kRowsPerGroup = 100;
constexpr int64_t kRows = kRowGroups * kRowsPerGroup;

class ParquetPushdownTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    ember_ = Session::builder()
                 .appName("ParquetPushdownTest")
                 .master("local[4]")
                 .getOrCreate();
  }

  static void TearDownTestCase() {
    ember_->stop();
    ember_.reset();
  }

  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_parquet_pushdown_{}", ::getpid());
    fs::remove_all(dir_);
    fs::create_directories(dir_);
    rootPool_ = velox::memory::memoryManager()->addRootPool();
    pool_ = rootPool_->addLeafChild("test");
    path_ = writeIds();
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  // kRows rows (id bigint, score bigint) with id ascending and score = id % 5,
  // in row groups of kRowsPerGroup rows.
  std::string writeIds() {
    auto path = (dir_ / "ids.parquet").string();
    velox::dwio::common::WriterOptions options;
    options.memoryPool = rootPool_.get();
    velox::parquet::Writer writer(
        std::make_unique<velox::dwio::common::LocalFileSink>(
            path, velox::dwio::common::FileSink::Options{.pool = pool_.get()}),
        options,
        type());
    for (int64_t group = 0; group < kRowGroups; ++group) {
      auto ids = velox::BaseVector::create<velox::FlatVector<int64_t>>(
          velox::BIGINT(), kRowsPerGroup, pool_.get());
      auto scores = velox::BaseVector::create<velox::FlatVector<int64_t>>(
          velox::BIGINT(), kRowsPerGroup, pool_.get());
      for (int64_t i = 0; i < kRowsPerGroup; ++i) {
        const auto id = group * kRowsPerGroup + i;
        ids->set(i, id);
        scores->set(i, id % 5);
      }
      writer.write(std::make_shared<velox::RowVector>(
          pool_.get(),
          type(),
          nullptr,
          kRowsPerGroup,
          std::vector<velox::VectorPtr>{ids, scores}));
      writer.flush();
    }
    writer.close();
    return path;
  }

  static velox::RowTypePtr type() {
    return velox::ROW({"id", "score"}, {velox::BIGINT(), velox::BIGINT()});
  }

  // The scan Axiom plans for 'df', which reads one file.
  core::TableScanNodePtr scanOf(const Dataframe& df) {
    auto plan = planStages(*df.physicalPlan(), pool_.get());
    EXPECT_EQ(plan.stages[0].scans.size(), 1) << plan.toString();
    return plan.stages[0].scans[0];
  }

  const hive::HiveTableHandle& handleOf(const core::TableScanNodePtr& scan) {
    return *scan->tableHandle()->asChecked<hive::HiveTableHandle>();
  }

  std::shared_ptr<hive::HiveConnectorSplit> split() const {
    return hive::HiveConnectorSplitBuilder(path_)
        .connectorId(kParquetConnectorId)
        .fileFormat(velox::dwio::common::FileFormat::PARQUET)
        .build();
  }

  // Row groups the reader skipped in the scan 'scanId' of a finished task.
  static int64_t skippedRowGroups(
      const std::shared_ptr<exec::Task>& task,
      const core::PlanNodeId& scanId) {
    const auto stats = exec::toPlanStats(task->taskStats());
    const auto& custom = stats.at(scanId).customStats;
    auto it = custom.find("skippedStrides");
    return it == custom.end() ? 0 : it->second.sum;
  }

  static inline std::optional<Session> ember_;
  fs::path dir_;
  std::string path_;
  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
};

TEST_F(ParquetPushdownTest, filterSkipsRowGroups) {
  auto scan = scanOf(ember_->read().parquet(path_).filter(col("id") >= lit(950L)));
  EXPECT_EQ(handleOf(scan).subfieldFilters().size(), 1);

  std::shared_ptr<exec::Task> task;
  auto rows = exec::test::AssertQueryBuilder(scan)
                  .split(split())
                  .copyResults(pool_.get(), task);
  EXPECT_EQ(rows->size(), 50);
  EXPECT_EQ(skippedRowGroups(task, scan->id()), kRowGroups - 1);
}

TEST_F(ParquetPushdownTest, filterOnColumnNotSelected) {
  auto df = ember_->read()
                .parquet(path_)
                .filter(col("id") >= lit(990L))
                .select(col("score"))
                .orderBy(col("score"));
  auto rows = df.collect();
  ASSERT_EQ(rows.size(), 10);
  EXPECT_EQ(rows.front().getLong(0), 0);
  EXPECT_EQ(rows.back().getLong(0), 4);
}

TEST_F(ParquetPushdownTest, filterOverSeveralColumnsStaysInTheScan) {
  auto df = ember_->read().parquet(path_).filter(
      col("id") + col("score") == lit(104L));
  EXPECT_NE(handleOf(scanOf(df)).remainingFilter(), nullptr);
  auto rows = df.collect();
  ASSERT_EQ(rows.size(), 1);
  // id 102 has score 2.
  EXPECT_EQ(rows[0].getLong(0), 102);
}

// A join's build side filters the probe side's scan, here through a table
// column that the scan returns twice.
TEST_F(ParquetPushdownTest, dynamicJoinFilterOnRepeatedColumn) {
  auto column = std::make_shared<hive::HiveColumnHandle>(
      "id",
      hive::HiveColumnHandle::ColumnType::kRegular,
      velox::BIGINT(),
      velox::BIGINT());
  auto handle = std::make_shared<hive::HiveTableHandle>(
      kParquetConnectorId,
      path_,
      velox::common::SubfieldFilters{},
      nullptr,
      type());
  auto ids = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanId;
  auto build = velox::BaseVector::create<velox::FlatVector<int64_t>>(
      velox::BIGINT(), 1, pool_.get());
  build->set(0, 950);
  auto plan =
      exec::test::PlanBuilder(ids)
          .startTableScan()
          .outputType(velox::ROW({"a", "b"}, {velox::BIGINT(), velox::BIGINT()}))
          .tableHandle(handle)
          .assignments({{"a", column}, {"b", column}})
          .endTableScan()
          .capturePlanNodeId(scanId)
          .hashJoin(
              {"b"},
              {"k"},
              exec::test::PlanBuilder(ids)
                  .values({std::make_shared<velox::RowVector>(
                      pool_.get(),
                      velox::ROW({"k"}, {velox::BIGINT()}),
                      nullptr,
                      1,
                      std::vector<velox::VectorPtr>{build})})
                  .planNode(),
              "",
              {"a", "b"})
          .planNode();

  std::shared_ptr<exec::Task> task;
  auto rows = exec::test::AssertQueryBuilder(plan)
                  .split(scanId, split())
                  .copyResults(pool_.get(), task);
  ASSERT_EQ(rows->size(), 1);
  EXPECT_EQ(rows->childAt(0)->asFlatVector<int64_t>()->valueAt(0), 950);
  EXPECT_EQ(rows->childAt(1)->asFlatVector<int64_t>()->valueAt(0), 950);
  EXPECT_EQ(skippedRowGroups(task, scanId), kRowGroups - 1);
}

} // namespace
} // namespace ember::driver
