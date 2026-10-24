// Ember's scheduler and shuffle end to end: stage plans, task messages,
// retries, recomputing lost shuffle outputs, byte-range splits of text and
// Parquet files, packing small files, spills, and agreement with Axiom's
// in-process LocalRunner.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <random>

#include <folly/coro/BlockingWait.h>

#include "axiom/runner/LocalRunner.h"
#include "ember/Session.h"
#include "ember/driver/DagScheduler.h"
#include "ember/driver/LocalBackend.h"
#include "ember/driver/MapOutputTracker.h"
#include "ember/functions.h"
#include "ember/shuffle/KilnShuffle.h"
#include "ember/shuffle/ShuffleConfig.h"
#include "ember/shuffle/ShuffleRead.h"
#include "ember/shuffle/ShuffleWrite.h"
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "kiln/client/Client.h"
#include "kiln/local/LocalCluster.h"

namespace ember::driver {
namespace {

using namespace ember::functions;
namespace axiom_connector = facebook::axiom::connector;
namespace core = facebook::velox::core;
namespace fs = std::filesystem;
namespace runner = facebook::axiom::runner;
namespace velox = facebook::velox;

// Wraps a backend: counts launches and fails the attempts 'fail' picks.
class FlakyBackend : public SchedulerBackend {
 public:
  using FailIf = std::function<bool(const TaskDescription&)>;

  FlakyBackend(std::shared_ptr<SchedulerBackend> backend, FailIf fail)
      : backend_{std::move(backend)}, fail_{std::move(fail)} {}

  int32_t totalSlots() const override {
    return backend_->totalSlots();
  }

  void launchTask(std::string task, ResultCallback onResult) override {
    auto description = TaskDescription::deserialize(task);
    {
      std::lock_guard<std::mutex> l(mutex_);
      ++launches_[description.stageId];
    }
    if (fail_ != nullptr && fail_(description)) {
      TaskResult result;
      result.taskId = description.taskId();
      result.state = TaskState::kFailed;
      result.error = "injected failure";
      onResult(result.serialize());
      return;
    }
    backend_->launchTask(std::move(task), std::move(onResult));
  }

  void setExecutorListener(ExecutorListener listener) override {
    std::lock_guard<std::mutex> l(mutex_);
    listener_ = std::move(listener);
  }

  void removeShuffle(int32_t shuffleId) override {
    backend_->removeShuffle(shuffleId);
  }

  /// Reports the executor at 'location' lost, then back, as if it restarted.
  void restartExecutor(const std::string& location) {
    ExecutorListener listener;
    {
      std::lock_guard<std::mutex> l(mutex_);
      listener = listener_;
    }
    listener.onExecutorLost(location);
    listener.onExecutorAdded(location);
  }

  void stop() override {
    backend_->stop();
  }

  int32_t launches(int32_t stageId) {
    std::lock_guard<std::mutex> l(mutex_);
    return launches_[stageId];
  }

  int32_t totalLaunches() {
    std::lock_guard<std::mutex> l(mutex_);
    int32_t total = 0;
    for (const auto& [stage, count] : launches_) {
      total += count;
    }
    return total;
  }

 private:
  const std::shared_ptr<SchedulerBackend> backend_;
  const FailIf fail_;
  std::mutex mutex_;
  std::map<int32_t, int32_t> launches_;
  ExecutorListener listener_;
};

class SchedulerTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    ember_ = Session::builder()
                 .appName("SchedulerTest")
                 .master("local[4]")
                 .getOrCreate();
    // Axiom's LocalRunner reads its exchanges in memory.
    velox::exec::ExchangeSource::registerFactory(
        velox::exec::test::createLocalExchangeSource);
    kiln_ = new kiln::local::LocalCluster(kiln::local::LocalClusterOptions{
        .dramServers = 1, .localFsServers = 0});
  }

  static void TearDownTestCase() {
    ember_->stop();
    ember_.reset();
    delete kiln_;
    kiln_ = nullptr;
  }

  // Config for map outputs in the test's Kiln cluster.
  static std::map<std::string, std::string> kilnConfig() {
    return {
        {std::string(shuffle::kShuffleStore), "kiln"},
        {std::string(shuffle::kKilnMetadata), kilnMetadata()}};
  }

  static std::string kilnMetadata() {
    return kiln_->metadataEndpoint().toString();
  }

  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_scheduler_test_{}", ::getpid());
    fs::remove_all(dir_);
    fs::create_directories(dir_ / "input");
    rootPool_ = velox::memory::memoryManager()->addRootPool();
    pool_ = rootPool_->addLeafChild("results");
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  using OnStage =
      std::function<void(const StageAttemptInfo&, FlakyBackend& backend)>;

  // A scheduler over its own local executor, with 'config'. 'onStage' sees
  // every stage attempt.
  std::pair<std::unique_ptr<DagScheduler>, std::shared_ptr<FlakyBackend>>
  scheduler(
      std::map<std::string, std::string> config = {},
      FlakyBackend::FailIf fail = nullptr,
      OnStage onStage = nullptr) {
    auto backend = std::make_shared<FlakyBackend>(
        std::make_shared<LocalBackend>(LocalBackendOptions{
            .numSlots = 4,
            .numThreads = 8,
            .localDirectory = (dir_ / "local").string()}),
        std::move(fail));
    auto dag = std::make_unique<DagScheduler>(
        backend,
        DagSchedulerOptions{
            .appName = "SchedulerTest",
            .parallelism = 4,
            .config = std::move(config),
            .onStageCompleted =
                [onStage = std::move(onStage),
                 raw = backend.get()](const StageAttemptInfo& info) {
                  if (onStage != nullptr) {
                    onStage(info, *raw);
                  }
                }});
    return {std::move(dag), backend};
  }

  // Random words over several files; returns the input directory.
  std::string writeCorpus(int32_t numFiles, int32_t linesPerFile) {
    const std::vector<std::string> vocabulary = {
        "the", "quick", "Brown", "fox", "JUMPS", "over", "lazy", "dog",
        "ember", "velox", "axiom", "x1", "snake_case", "a"};
    std::mt19937 rng(7);
    for (int32_t f = 0; f < numFiles; ++f) {
      std::ofstream out(dir_ / "input" / fmt::format("part-{}.txt", f));
      for (int32_t l = 0; l < linesPerFile; ++l) {
        const auto numWords = rng() % 10;
        for (uint32_t w = 0; w < numWords; ++w) {
          out << vocabulary[rng() % vocabulary.size()] << (w % 3 ? " " : ", ");
        }
        out << "\n";
      }
    }
    return (dir_ / "input").string() + "/";
  }

  // Writes rows (id bigint, score bigint), score = id % 7, in row groups of
  // 'rowsPerGroup' rows; returns the file's path.
  std::string writeParquet(int64_t numGroups, int64_t rowsPerGroup) {
    auto pool = rootPool_->addLeafChild("write");
    auto path = (dir_ / "input" / "rows.parquet").string();
    velox::dwio::common::WriterOptions options;
    options.memoryPool = rootPool_.get();
    options.flushPolicyFactory = [rowsPerGroup] {
      return std::make_unique<velox::parquet::DefaultFlushPolicy>(
          rowsPerGroup, 1LL << 30);
    };
    auto type = velox::ROW({"id", "score"}, velox::BIGINT());
    velox::parquet::Writer writer(
        std::make_unique<velox::dwio::common::LocalFileSink>(
            path, velox::dwio::common::FileSink::Options{.pool = pool.get()}),
        options,
        type);
    for (int64_t group = 0; group < numGroups; ++group) {
      auto ids = velox::BaseVector::create<velox::FlatVector<int64_t>>(
          velox::BIGINT(), rowsPerGroup, pool.get());
      auto scores = velox::BaseVector::create<velox::FlatVector<int64_t>>(
          velox::BIGINT(), rowsPerGroup, pool.get());
      for (int64_t i = 0; i < rowsPerGroup; ++i) {
        ids->set(i, group * rowsPerGroup + i);
        scores->set(i, (group * rowsPerGroup + i) % 7);
      }
      writer.write(std::make_shared<velox::RowVector>(
          pool.get(),
          type,
          nullptr,
          rowsPerGroup,
          std::vector<velox::VectorPtr>{ids, scores}));
    }
    writer.close();

    velox::parquet::ParquetReader reader(
        std::make_unique<velox::dwio::common::BufferedInput>(
            velox::filesystems::getFileSystem(path, nullptr)
                ->openFileForRead(path),
            *pool),
        velox::dwio::common::ReaderOptions(pool.get()));
    EXPECT_EQ(reader.fileMetaData().numRowGroups(), numGroups);
    return path;
  }

  static Dataframe wordCount(const std::string& path) {
    return ember_->read()
        .text(path)
        .select(explode(split(lower(col("value")), R"(\W+)")).as("word"))
        .filter(col("word") != "")
        .groupBy("word")
        .count()
        .orderBy(col("count").desc(), col("word"));
  }

  Dataframe people() {
    return ember_->createDataframe(
        velox::ROW(
            {"name", "dept", "salary"},
            {velox::VARCHAR(), velox::VARCHAR(), velox::BIGINT()}),
        {{"alice", "eng", 100L},
         {"bob", "eng", 80L},
         {"carol", "sales", 70L},
         {"dave", "sales", 50L},
         {"erin", "hr", 60L}});
  }

  // Rows as strings, in result order.
  static std::vector<std::string> toStrings(
      const std::vector<velox::RowVectorPtr>& batches) {
    std::vector<std::string> rows;
    for (const auto& batch : batches) {
      for (velox::vector_size_t i = 0; i < batch->size(); ++i) {
        rows.push_back(batch->toString(i));
      }
    }
    return rows;
  }

  static std::vector<std::string> sorted(std::vector<std::string> rows) {
    std::sort(rows.begin(), rows.end());
    return rows;
  }

  // Runs 'df' with Axiom's LocalRunner and its in-memory exchanges.
  std::vector<velox::RowVectorPtr> runWithLocalRunner(const Dataframe& df) {
    auto executor = std::make_unique<folly::CPUThreadPoolExecutor>(8);
    auto queryId = fmt::format("local_runner_{}", ++localRunnerQueries_);
    auto queryCtx = core::QueryCtx::create(
        executor.get(),
        core::QueryConfig({}),
        {},
        velox::cache::AsyncDataCache::getInstance(),
        nullptr,
        nullptr,
        queryId);
    auto connectorContext = std::make_shared<axiom_connector::ConnectorContext>(
        queryId,
        "test",
        axiom_connector::ConnectorProperties{},
        axiom_connector::ConnectorContext::noopStatWriterProvider());
    auto localRunner = std::make_shared<runner::LocalRunner>(
        std::make_shared<runner::RunnerSession>(
            connectorContext,
            std::make_shared<velox::NoopRuntimeStatWriter>(),
            runner::Properties{}),
        df.physicalPlan(),
        facebook::axiom::optimizer::FinishWrite{},
        queryCtx,
        std::make_shared<runner::ConnectorSplitSourceFactory>(),
        queryCtx->pool()->addLeafChild("runner"),
        "");
    std::vector<velox::RowVectorPtr> results;
    localRunner->drain([&](velox::RowVectorPtr batch) {
      results.push_back(std::dynamic_pointer_cast<velox::RowVector>(
          velox::BaseVector::copy(*batch, pool_.get())));
    });
    localRunner.reset();
    executor->join();
    return results;
  }

  static inline std::optional<Session> ember_;
  static inline kiln::local::LocalCluster* kiln_{nullptr};
  fs::path dir_;
  std::shared_ptr<velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<velox::memory::MemoryPool> pool_;
  int32_t localRunnerQueries_{0};
};

TEST_F(SchedulerTest, wordCountStages) {
  auto path = writeCorpus(1, 10);
  auto [dag, backend] = scheduler();
  auto stages = dag->stagePlan(*wordCount(path).physicalPlan());
  ASSERT_EQ(stages.stages.size(), 3) << stages.toString();

  // Map stage: scan to partial count, written to a shuffle on 'word'.
  const auto& map = stages.stages[0];
  EXPECT_FALSE(map.numTasks.has_value());
  EXPECT_EQ(map.scans.size(), 1);
  auto write = std::dynamic_pointer_cast<const shuffle::ShuffleWriteNode>(map.plan);
  ASSERT_NE(write, nullptr) << map.plan->toString(true, true);
  EXPECT_EQ(write->shuffleId(), *map.shuffleId);
  EXPECT_EQ(write->numPartitions(), 4);

  // Reduce stage: final count and sort, one task per partition.
  const auto& reduce = stages.stages[1];
  EXPECT_EQ(reduce.numTasks, 4);
  EXPECT_EQ(reduce.parents, std::vector<int32_t>{map.stageId});
  ASSERT_EQ(reduce.shuffleInputs.size(), 1);
  EXPECT_EQ(reduce.shuffleInputs[0].shuffleId, *map.shuffleId);

  // Result stage: merges the sorted reduce outputs.
  const auto& result = stages.resultStage();
  EXPECT_EQ(result.numTasks, 1);
  auto read = std::dynamic_pointer_cast<const shuffle::ShuffleReadNode>(result.plan);
  ASSERT_NE(read, nullptr) << result.plan->toString(true, true);
  EXPECT_TRUE(read->isMerge());

  // The task binary deserializes to the same plan, again.
  for (const auto& stage : stages.stages) {
    auto copy = velox::ISerializable::deserialize<core::PlanNode>(
        folly::parseJson(folly::toJson(stage.fragment)), pool_.get());
    EXPECT_EQ(copy->toString(true, true), stage.plan->toString(true, true));
  }
}

TEST_F(SchedulerTest, taskMessagesRoundTrip) {
  TaskDescription task;
  task.jobId = "job_1";
  task.stageId = 3;
  task.stageAttempt = 1;
  task.partition = 2;
  task.attempt = 1;
  task.fragment = folly::dynamic::object("name", "ValuesNode");
  task.splits["0"] = {folly::dynamic::object("name", "FileRangeSplit")};
  task.splits["6"] = {};
  task.resultTask = true;
  task.config = {{"ember.x", "y"}};
  auto copy = TaskDescription::deserialize(task.serialize());
  EXPECT_EQ(copy.taskId(), "ember://job_1/3.1/2.1");
  EXPECT_EQ(copy.fragment, task.fragment);
  EXPECT_EQ(copy.splits, task.splits);
  EXPECT_TRUE(copy.resultTask);
  EXPECT_EQ(copy.config, task.config);

  TaskResult result;
  result.taskId = copy.taskId();
  result.state = TaskState::kFailed;
  result.error = "boom";
  result.userError = true;
  result.mapStatus = shuffle::MapStatus{"executor-1", {10, 0, 7}};
  result.resultPages = {"page one", std::string("\0binary\0", 8)};
  result.outputRows = 42;
  auto resultCopy = TaskResult::deserialize(result.serialize());
  EXPECT_EQ(resultCopy.taskId, result.taskId);
  EXPECT_EQ(resultCopy.state, TaskState::kFailed);
  EXPECT_EQ(resultCopy.error, "boom");
  EXPECT_TRUE(resultCopy.userError);
  EXPECT_EQ(resultCopy.mapStatus->location, "executor-1");
  EXPECT_EQ(
      resultCopy.mapStatus->partitionLengths, (std::vector<int64_t>{10, 0, 7}));
  EXPECT_EQ(resultCopy.resultPages, result.resultPages);
  EXPECT_EQ(resultCopy.outputRows, 42);
}

TEST_F(SchedulerTest, matchesLocalRunner) {
  auto path = writeCorpus(3, 500);
  auto depts = ember_->createDataframe(
      velox::ROW({"dept", "floor"}, {velox::VARCHAR(), velox::BIGINT()}),
      {{"eng", 3L}, {"sales", 1L}, {"ops", 9L}});
  std::vector<std::pair<std::string, Dataframe>> queries = {
      {"wordCount", wordCount(path)},
      {"lines", ember_->read().text(path).filter(length(col("value")) > 20)},
      {"join", people().join(depts, {"dept"}, "full").select("name", "floor")},
      {"unionDistinct", people().unionAll(people()).select("dept").distinct()},
      {"aggregate",
       people()
           .groupBy("dept")
           .agg(sum(col("salary")).as("total"), count(col("name")).as("n"))
           .orderBy(col("total").desc())
           .limit(2)},
  };
  for (const auto& [name, df] : queries) {
    SCOPED_TRACE(name);
    auto expected = toStrings(runWithLocalRunner(df));
    auto actual = toStrings(df.collectBatches());
    ASSERT_FALSE(expected.empty());
    EXPECT_EQ(sorted(actual), sorted(expected));
  }
  // Sorted outputs agree in order too.
  EXPECT_EQ(
      toStrings(wordCount(path).collectBatches()),
      toStrings(runWithLocalRunner(wordCount(path))));
}

TEST_F(SchedulerTest, byteRangeSplitsAndSpills) {
  auto path = writeCorpus(2, 3000);
  auto plan = wordCount(path).physicalPlan();
  auto expected = toStrings(wordCount(path).collectBatches());

  // Text files cut into 4KB ranges; every shuffle page spills.
  auto [dag, backend] = scheduler(
      {{DagScheduler::kMaxSplitBytes, "4096"},
       {std::string(shuffle::kSpillThreshold), "1"}});
  auto actual = toStrings(dag->runJob(*plan, pool_.get()));
  EXPECT_EQ(actual, expected);

  // One map task per range.
  int64_t numRanges = 0;
  for (const auto& entry : fs::directory_iterator(dir_ / "input")) {
    numRanges += (entry.file_size() + 4095) / 4096;
  }
  const auto stages = dag->stagePlan(*plan);
  EXPECT_EQ(backend->launches(stages.stages[0].stageId), numRanges);
  EXPECT_GT(numRanges, 10);
}

TEST_F(SchedulerTest, everyLineReadOnceWhateverTheRangeSize) {
  auto path = dir_ / "input" / "lines.txt";
  std::ofstream(path, std::ios::binary)
      << "first line\r\n\n\nab\nlonger line that spans many ranges\r\n"
      << "x\n\r\nlast line without newline";
  auto lines = ember_->read().text(path.string());
  auto plan = lines.physicalPlan();
  auto expected = sorted(toStrings(lines.collectBatches()));
  ASSERT_EQ(expected.size(), 8);
  for (auto rangeBytes : {1, 2, 3, 5, 7, 11, 13, 64}) {
    SCOPED_TRACE(rangeBytes);
    auto [dag, backend] =
        scheduler({{DagScheduler::kMaxSplitBytes, std::to_string(rangeBytes)}});
    EXPECT_EQ(sorted(toStrings(dag->runJob(*plan, pool_.get()))), expected);
  }
}

TEST_F(SchedulerTest, everyParquetRowReadOnceWhateverTheRangeSize) {
  auto path = writeParquet(20, 100);
  auto df = ember_->read()
                .parquet(path)
                .groupBy("score")
                .agg(count(col("id")).as("n"), sum(col("id")).as("total"))
                .orderBy(col("score"));
  auto plan = df.physicalPlan();
  auto expected = toStrings(df.collectBatches());
  ASSERT_EQ(expected.size(), 7);
  const auto fileBytes = static_cast<int64_t>(fs::file_size(path));
  for (auto rangeBytes : {fileBytes / 13, fileBytes / 4, fileBytes}) {
    SCOPED_TRACE(rangeBytes);
    auto [dag, backend] =
        scheduler({{DagScheduler::kMaxSplitBytes, std::to_string(rangeBytes)}});
    EXPECT_EQ(toStrings(dag->runJob(*plan, pool_.get())), expected);
    // One map task per range.
    const auto stages = dag->stagePlan(*plan);
    EXPECT_EQ(
        backend->launches(stages.stages[0].stageId),
        (fileBytes + rangeBytes - 1) / rangeBytes);
  }
}

TEST_F(SchedulerTest, smallFilesShareTasks) {
  auto path = writeCorpus(40, 10);
  auto plan = wordCount(path).physicalPlan();
  auto expected = toStrings(wordCount(path).collectBatches());

  // Each file counts 4MB to open, so 15 fit in 64MB.
  auto [dag, backend] =
      scheduler({{DagScheduler::kMaxSplitBytes, std::to_string(64 << 20)}});
  EXPECT_EQ(toStrings(dag->runJob(*plan, pool_.get())), expected);
  const auto stages = dag->stagePlan(*plan);
  EXPECT_EQ(backend->launches(stages.stages[0].stageId), 3);
}

TEST_F(SchedulerTest, retriesFailedTasks) {
  auto path = writeCorpus(2, 200);
  auto df = wordCount(path);
  auto expected = toStrings(df.collectBatches());

  // Every first attempt fails.
  auto [dag, backend] = scheduler(
      {}, [](const TaskDescription& task) { return task.attempt == 0; });
  EXPECT_EQ(toStrings(dag->runJob(*df.physicalPlan(), pool_.get())), expected);
  // Two map tasks (one per file), four reduce tasks, one result task, each
  // run twice.
  EXPECT_EQ(backend->totalLaunches(), 2 * (2 + 4 + 1));
}

TEST_F(SchedulerTest, failsAfterMaxAttempts) {
  auto path = writeCorpus(1, 10);
  auto df = wordCount(path);
  auto [dag, backend] = scheduler({}, [](const TaskDescription& task) {
    return task.resultTask;
  });
  VELOX_ASSERT_THROW(
      dag->runJob(*df.physicalPlan(), pool_.get()),
      "failed 4 times. Last error: injected failure");
  const auto stages = dag->stagePlan(*df.physicalPlan());
  EXPECT_EQ(backend->launches(stages.resultStage().stageId), 4);

  // The scheduler runs the next job normally.
  auto [healthy, healthyBackend] = scheduler();
  EXPECT_FALSE(healthy->runJob(*df.physicalPlan(), pool_.get()).empty());
}

// Deleting a map stage's files once it is done: the reduce tasks cannot fetch
// them, so the map stage runs again and then the reduce tasks that failed.
TEST_F(SchedulerTest, recomputesShuffleOutputsThatCannotBeFetched) {
  auto path = writeCorpus(2, 200);
  auto df = wordCount(path);
  auto expected = toStrings(df.collectBatches());

  std::vector<StageAttemptInfo> attempts;
  std::optional<int32_t> mapStage;
  auto [dag, backend] = scheduler(
      {}, nullptr, [&](const StageAttemptInfo& info, FlakyBackend& backend) {
        attempts.push_back(info);
        if (!mapStage.has_value()) {
          mapStage = info.stageId;
          backend.removeShuffle(*info.shuffleId);
        }
      });
  EXPECT_EQ(toStrings(dag->runJob(*df.physicalPlan(), pool_.get())), expected);

  const auto plan = dag->stagePlan(*df.physicalPlan());
  const auto reduceStage = plan.stages[1].stageId;
  ASSERT_EQ(plan.stages[0].stageId, *mapStage);
  // Map, reduce (fetch failures), map again, reduce again, result.
  ASSERT_EQ(attempts.size(), 5);
  // Every reduce task that reads a block failed to fetch it.
  EXPECT_EQ(attempts[1].stageId, reduceStage);
  const auto failed = attempts[1].fetchFailures.size();
  EXPECT_GT(failed, 0);
  // Both map tasks ran again, then the failed reduce tasks.
  EXPECT_EQ(attempts[2].stageId, *mapStage);
  EXPECT_EQ(attempts[2].attempt, 1);
  EXPECT_EQ(attempts[2].partitions, (std::vector<int32_t>{0, 1}));
  EXPECT_EQ(attempts[3].stageId, reduceStage);
  EXPECT_EQ(attempts[3].attempt, 1);
  EXPECT_EQ(attempts[3].partitions.size(), failed);
  EXPECT_EQ(backend->launches(*mapStage), 2 * 2);
  EXPECT_EQ(backend->launches(reduceStage), 4 + failed);
  EXPECT_EQ(backend->launches(plan.resultStage().stageId), 1);
  EXPECT_TRUE(attempts[3].fetchFailures.empty());
}

// An executor lost after a map stage: its outputs are recomputed before any
// reduce task tries to read them.
TEST_F(SchedulerTest, recomputesOutputsOfALostExecutor) {
  auto path = writeCorpus(2, 200);
  auto df = wordCount(path);
  auto expected = toStrings(df.collectBatches());

  std::vector<StageAttemptInfo> attempts;
  auto [dag, backend] = scheduler(
      {}, nullptr, [&](const StageAttemptInfo& info, FlakyBackend& backend) {
        attempts.push_back(info);
        if (attempts.size() == 1) {
          ASSERT_EQ(info.outputLocations.size(), 2);
          backend.restartExecutor(info.outputLocations.at(0));
        }
      });
  EXPECT_EQ(toStrings(dag->runJob(*df.physicalPlan(), pool_.get())), expected);

  const auto plan = dag->stagePlan(*df.physicalPlan());
  EXPECT_EQ(backend->launches(plan.stages[0].stageId), 2 * 2);
  EXPECT_EQ(backend->launches(plan.stages[1].stageId), 4);
  for (const auto& attempt : attempts) {
    EXPECT_TRUE(attempt.fetchFailures.empty());
  }
}

TEST_F(SchedulerTest, failsAfterMaxStageAttempts) {
  auto path = writeCorpus(1, 50);
  auto df = wordCount(path);
  std::optional<int32_t> mapStage;
  auto [dag, backend] = scheduler(
      {}, nullptr, [&](const StageAttemptInfo& info, FlakyBackend& backend) {
        if (!mapStage.has_value()) {
          mapStage = info.stageId;
        }
        if (info.stageId == *mapStage) {
          backend.removeShuffle(*info.shuffleId);
        }
      });
  VELOX_ASSERT_THROW(
      dag->runJob(*df.physicalPlan(), pool_.get()),
      "4 attempts lost their inputs or outputs");
  EXPECT_EQ(backend->launches(*mapStage), 4);

  // The scheduler runs the next job normally.
  auto [healthy, healthyBackend] = scheduler();
  EXPECT_FALSE(healthy->runJob(*df.physicalPlan(), pool_.get()).empty());
}

TEST_F(SchedulerTest, userErrorsAreNotRetried) {
  auto df = people().selectExpr({"salary / (salary - salary) AS n"});
  auto [dag, backend] = scheduler();
  EXPECT_THROW(
      dag->runJob(*df.physicalPlan(), pool_.get()), velox::VeloxUserError);
  EXPECT_EQ(backend->totalLaunches(), 1);
  EXPECT_THROW(df.collect(), velox::VeloxUserError);
}

TEST_F(SchedulerTest, shuffleFilesAreRemovedAfterTheJob) {
  auto path = writeCorpus(2, 100);
  auto [dag, backend] = scheduler({{DagScheduler::kReuseOutputs, "false"}});
  dag->runJob(*wordCount(path).physicalPlan(), pool_.get());
  for (const auto& entry : fs::recursive_directory_iterator(dir_ / "local")) {
    EXPECT_FALSE(entry.is_regular_file()) << entry.path();
  }
}

TEST_F(SchedulerTest, storedOutputsAreKeptAfterTheJob) {
  auto path = writeCorpus(2, 100);
  auto [dag, backend] = scheduler();
  dag->runJob(*wordCount(path).physicalPlan(), pool_.get());
  int32_t files = 0;
  for (const auto& entry : fs::recursive_directory_iterator(dir_ / "local")) {
    files += entry.is_regular_file();
  }
  EXPECT_GT(files, 0);
}

// The second run of a job reads the stored output of its last map stage; the
// stages before it do not run.
TEST_F(SchedulerTest, reusesStoredOutputs) {
  auto path = writeCorpus(2, 200);
  auto df = wordCount(path);
  auto expected = toStrings(df.collectBatches());

  std::vector<StageAttemptInfo> attempts;
  auto [dag, backend] = scheduler(
      {}, nullptr, [&](const StageAttemptInfo& info, FlakyBackend&) {
        attempts.push_back(info);
      });
  const auto plan = df.physicalPlan();
  EXPECT_EQ(toStrings(dag->runJob(*plan, pool_.get())), expected);
  const auto launches = backend->totalLaunches();
  const auto firstAttempts = attempts.size();

  const auto stages = dag->stagePlan(*plan);
  ASSERT_EQ(stages.stages.size(), 3);
  auto explain = dag->explain(*plan);
  EXPECT_NE(explain.find(", not needed]"), std::string::npos) << explain;
  EXPECT_NE(explain.find(", reused]"), std::string::npos) << explain;

  EXPECT_EQ(toStrings(dag->runJob(*plan, pool_.get())), expected);
  EXPECT_EQ(backend->totalLaunches(), launches + 1);
  // Both map stages are reported reused, then the result stage runs.
  ASSERT_EQ(attempts.size(), firstAttempts + 3);
  EXPECT_TRUE(attempts[firstAttempts].reused);
  EXPECT_TRUE(attempts[firstAttempts + 1].reused);
  EXPECT_EQ(attempts[firstAttempts + 1].stageKey, stages.stages[1].key);
  EXPECT_FALSE(attempts[firstAttempts + 2].reused);

  // Without reuse, everything runs again.
  auto [fresh, freshBackend] =
      scheduler({{DagScheduler::kReuseOutputs, "false"}});
  fresh->runJob(*plan, pool_.get());
  fresh->runJob(*plan, pool_.get());
  EXPECT_EQ(freshBackend->totalLaunches(), 2 * launches);
}

// Stored outputs lost with their executor are recomputed.
TEST_F(SchedulerTest, recomputesLostStoredOutputs) {
  auto path = writeCorpus(2, 200);
  auto df = wordCount(path);
  auto expected = toStrings(df.collectBatches());

  std::string location;
  auto [dag, backend] = scheduler(
      {}, nullptr, [&](const StageAttemptInfo& info, FlakyBackend&) {
        if (!info.outputLocations.empty()) {
          location = info.outputLocations.begin()->second;
        }
      });
  const auto plan = df.physicalPlan();
  EXPECT_EQ(toStrings(dag->runJob(*plan, pool_.get())), expected);
  const auto launches = backend->totalLaunches();
  ASSERT_FALSE(location.empty());

  backend->restartExecutor(location);
  EXPECT_EQ(toStrings(dag->runJob(*plan, pool_.get())), expected);
  EXPECT_EQ(backend->totalLaunches(), 2 * launches);

  // Stored again: the next run reuses them.
  EXPECT_EQ(toStrings(dag->runJob(*plan, pool_.get())), expected);
  EXPECT_EQ(backend->totalLaunches(), 2 * launches + 1);
}

// The read of a cached table in 'plan'.
std::optional<Stage::ShuffleInput> cachedInput(
    DagScheduler& dag,
    const facebook::axiom::optimizer::MultiFragmentPlan& plan) {
  for (const auto& stage : dag.stagePlan(plan).stages) {
    for (const auto& input : stage.shuffleInputs) {
      if (input.cacheId.has_value()) {
        return input;
      }
    }
  }
  return std::nullopt;
}

// The key of the cached output 'plan' reads.
std::optional<std::string> cachedInputKey(
    DagScheduler& dag,
    const facebook::axiom::optimizer::MultiFragmentPlan& plan) {
  auto input = cachedInput(dag, plan);
  return input.has_value() ? input->cacheKey : std::nullopt;
}

// A threshold sweep over a cached projection computes the projection once;
// the filter is not pushed into it.
TEST_F(SchedulerTest, cachedSweepComputesPrefixOnce) {
  auto path = writeCorpus(3, 300);
  auto lines = ember_->read().text(path).select(
      length(col("value")).as("len"), upper(col("value")).as("upper"));
  auto cached = lines.cache();
  auto sweep = [](const Dataframe& df, int32_t threshold) {
    return df.filter(col("len") > lit(int64_t{threshold}))
        .agg({countAll().as("n"), sum(length(col("upper"))).as("chars")});
  };

  std::map<std::string, int32_t> ranTasks;
  std::map<std::string, int32_t> reusedStages;
  auto [dag, backend] = scheduler(
      {}, nullptr, [&](const StageAttemptInfo& info, FlakyBackend&) {
        if (info.stageKey.has_value()) {
          ranTasks[*info.stageKey] += info.partitions.size();
          reusedStages[*info.stageKey] += info.reused;
        }
      });
  const auto cutKey = cachedInputKey(*dag, *sweep(cached, 0).physicalPlan());
  ASSERT_TRUE(cutKey.has_value());
  EXPECT_NE(
      dag->explain(*sweep(cached, 0).physicalPlan()).find("not stored yet"),
      std::string::npos);

  const std::vector<int32_t> thresholds = {0, 20, 40, 60};
  for (auto threshold : thresholds) {
    EXPECT_EQ(
        toStrings(dag->runJob(*sweep(cached, threshold).physicalPlan(), pool_.get())),
        toStrings(sweep(lines, threshold).collectBatches()))
        << threshold;
  }
  // One task per file, once; every later job reads the stored output.
  EXPECT_EQ(ranTasks[*cutKey], 3);
  EXPECT_EQ(reusedStages[*cutKey], thresholds.size() - 1);
  EXPECT_NE(
      dag->explain(*sweep(cached, 99).physicalPlan()).find("(stored)"),
      std::string::npos);

  // A different cached prefix has a different key, and computes.
  auto other = lines.select(col("len"), lower(col("upper")).as("upper")).cache();
  const auto otherKey = cachedInputKey(*dag, *sweep(other, 0).physicalPlan());
  ASSERT_TRUE(otherKey.has_value());
  EXPECT_NE(otherKey, cutKey);
  EXPECT_EQ(
      toStrings(dag->runJob(*sweep(other, 20).physicalPlan(), pool_.get())),
      toStrings(sweep(lines, 20).collectBatches()));
  EXPECT_EQ(ranTasks[*otherKey], 3);

  // Dropped, it computes again.
  const auto id = *cachedInput(*dag, *sweep(cached, 0).physicalPlan())->cacheId;
  EXPECT_TRUE(dag->uncache(id));
  EXPECT_FALSE(dag->uncache(id));
  dag->runJob(*sweep(cached, 0).physicalPlan(), pool_.get());
  EXPECT_EQ(ranTasks[*cutKey], 6);
}

// A cached output lost with its executor is recomputed, only the lost part.
TEST_F(SchedulerTest, recomputesLostCachedOutput) {
  auto path = writeCorpus(3, 100);
  auto cached = ember_->read()
                    .text(path)
                    .select(upper(col("value")).as("upper"))
                    .cache();
  auto df = cached.groupBy("upper").count();
  auto expected = sorted(toStrings(df.collectBatches()));

  std::map<int32_t, std::string> locations;
  std::map<std::string, int32_t> ranTasks;
  auto [dag, backend] = scheduler(
      {}, nullptr, [&](const StageAttemptInfo& info, FlakyBackend&) {
        if (info.stageKey.has_value()) {
          ranTasks[*info.stageKey] += info.partitions.size();
        }
        for (const auto& [map, location] : info.outputLocations) {
          locations[map] = location;
        }
      });
  const auto plan = df.physicalPlan();
  const auto cutKey = cachedInputKey(*dag, *plan);
  ASSERT_TRUE(cutKey.has_value());
  EXPECT_EQ(sorted(toStrings(dag->runJob(*plan, pool_.get()))), expected);
  EXPECT_EQ(ranTasks[*cutKey], 3);

  // The local backend has one executor: all outputs are lost.
  backend->restartExecutor(locations.begin()->second);
  EXPECT_EQ(sorted(toStrings(dag->runJob(*plan, pool_.get()))), expected);
  EXPECT_EQ(ranTasks[*cutKey], 6);
  EXPECT_EQ(sorted(toStrings(dag->runJob(*plan, pool_.get()))), expected);
  EXPECT_EQ(ranTasks[*cutKey], 6);
}

// Query config that can change results is part of the key; Ember's own config
// is not.
TEST_F(SchedulerTest, keysCoverQueryConfig) {
  auto path = writeCorpus(1, 10);
  const auto plan = wordCount(path).physicalPlan();
  auto keyWith = [&](std::map<std::string, std::string> config) {
    auto [dag, backend] = scheduler(std::move(config));
    return dag->stagePlan(*plan).resultStage().key;
  };
  const auto base = keyWith({});
  ASSERT_TRUE(base.has_value());
  EXPECT_NE(keyWith({{"session_timezone", "Asia/Tokyo"}}), base);
  EXPECT_EQ(keyWith({{DagScheduler::kReuseOutputs, "true"}}), base);
}

TEST_F(SchedulerTest, kilnStoreMatchesFileStore) {
  auto path = writeCorpus(3, 500);
  auto depts = ember_->createDataframe(
      velox::ROW({"dept", "floor"}, {velox::VARCHAR(), velox::BIGINT()}),
      {{"eng", 3L}, {"sales", 1L}, {"ops", 9L}});
  std::vector<std::pair<std::string, Dataframe>> queries = {
      {"wordCount", wordCount(path)},
      {"join", people().join(depts, {"dept"}, "full").select("name", "floor")},
      {"unionDistinct", people().unionAll(people()).select("dept").distinct()},
  };
  for (const auto& [name, df] : queries) {
    SCOPED_TRACE(name);
    std::vector<StageAttemptInfo> attempts;
    auto [dag, backend] = scheduler(
        kilnConfig(), nullptr, [&](const StageAttemptInfo& info, auto&) {
          attempts.push_back(info);
        });
    auto actual = toStrings(dag->runJob(*df.physicalPlan(), pool_.get()));
    EXPECT_EQ(sorted(actual), sorted(toStrings(df.collectBatches())));
    for (const auto& attempt : attempts) {
      for (const auto& [mapId, location] : attempt.outputLocations) {
        EXPECT_TRUE(shuffle::isKilnLocation(location)) << location;
      }
    }
  }
}

TEST_F(SchedulerTest, kilnStoreSpills) {
  auto path = writeCorpus(2, 3000);
  auto df = wordCount(path);
  auto config = kilnConfig();
  config[std::string(shuffle::kSpillThreshold)] = "1";
  auto [dag, backend] = scheduler(std::move(config));
  EXPECT_EQ(
      toStrings(dag->runJob(*df.physicalPlan(), pool_.get())),
      toStrings(df.collectBatches()));
}

// Kiln evicted a map output's chunk before the reduce stage read it: the
// reads fail to fetch, and the map tasks run again.
TEST_F(SchedulerTest, recomputesEvictedKilnOutputs) {
  auto path = writeCorpus(2, 200);
  auto df = wordCount(path);
  auto expected = toStrings(df.collectBatches());

  std::vector<StageAttemptInfo> attempts;
  std::optional<int32_t> mapStage;
  auto [dag, backend] = scheduler(
      kilnConfig(), nullptr, [&](const StageAttemptInfo& info, auto&) {
        attempts.push_back(info);
        if (mapStage.has_value()) {
          return;
        }
        mapStage = info.stageId;
        auto client = shuffle::kilnClient(kilnMetadata());
        for (const auto& [mapId, location] : info.outputLocations) {
          const auto chunk = shuffle::parseKilnLocation(location).second;
          folly::coro::blockingWait(
              client->co_remove(kiln::ChunkId::parse(chunk)));
        }
      });
  EXPECT_EQ(toStrings(dag->runJob(*df.physicalPlan(), pool_.get())), expected);

  const auto plan = dag->stagePlan(*df.physicalPlan());
  ASSERT_GE(attempts.size(), 4);
  EXPECT_EQ(attempts[1].stageId, plan.stages[1].stageId);
  EXPECT_GT(attempts[1].fetchFailures.size(), 0);
  EXPECT_EQ(attempts[2].stageId, *mapStage);
  EXPECT_EQ(attempts[2].partitions, (std::vector<int32_t>{0, 1}));
  EXPECT_EQ(backend->launches(*mapStage), 2 * 2);
}

TEST(MapOutputTrackerTest, kilnOutputsOutliveTheirExecutor) {
  MapOutputTracker tracker;
  tracker.registerShuffle(0, 2);
  const auto kiln = shuffle::kilnLocation("meta:7000", "abc");
  ASSERT_TRUE(tracker.registerMapOutput(0, 0, {"executor-1", {10, 20}}));
  ASSERT_TRUE(tracker.registerMapOutput(0, 1, {kiln, {10, 20}}));
  EXPECT_EQ(tracker.markLocationLost("executor-1"), 1);
  EXPECT_EQ(tracker.missingMaps(0), (std::vector<int32_t>{0}));
  // Reported by a task on the lost executor, but held in Kiln.
  tracker.registerShuffle(1, 1);
  EXPECT_TRUE(tracker.registerMapOutput(1, 0, {kiln, {5}}));
  // Block URLs carry each partition's byte range of the chunk.
  EXPECT_EQ(
      tracker.mapBlockUrls(0, 1),
      (std::vector<std::string>{
          "kiln://meta:7000/abc/0/10/0/1/0",
          "kiln://meta:7000/abc/10/20/0/1/1"}));
  EXPECT_EQ(
      tracker.blockUrls(1, 0),
      (std::vector<std::string>{"kiln://meta:7000/abc/0/5/1/0/0"}));
  // A failed fetch drops that output only.
  EXPECT_EQ(tracker.removeOutputsAt(0, kiln), 1);
}

} // namespace
} // namespace ember::driver
