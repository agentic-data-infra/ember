#include "ember/driver/DagScheduler.h"

#include <set>

#include <folly/ScopeGuard.h>
#include <folly/coro/BlockingWait.h>
#include <glog/logging.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/runner/LocalRunner.h"
#include "ember/CachedTable.h"
#include "ember/FileSerde.h"
#include "ember/RowPages.h"
#include "ember/shuffle/KilnShuffle.h"
#include "ember/shuffle/ShuffleConfig.h"
#include "ember/shuffle/ShuffleWrite.h"
#include "kiln/client/Client.h"
#include "velox/common/file/FileSystems.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Exchange.h"
#include "velox/expression/Expr.h"

namespace ember::driver {

namespace axiom_connector = facebook::axiom::connector;
namespace core = facebook::velox::core;
namespace file = facebook::axiom::connector::file;
namespace optimizer = facebook::axiom::optimizer;
namespace runner = facebook::axiom::runner;
namespace velox = facebook::velox;

namespace {

constexpr int64_t kSplitBytesFloor = 4 << 20;
constexpr int64_t kSplitBytesCeiling = 128 << 20;
// What opening a file costs, in bytes read: small files share a task.
constexpr int64_t kOpenCostBytes = 4 << 20;

std::atomic<int32_t> schedulerCounter{0};

} // namespace

struct DagScheduler::StageState {
  const Stage* stage;
  bool isResult{false};
  // Splits of each task before shuffle blocks are added: file ranges for
  // scans, empty lists for the other split nodes.
  std::vector<std::map<std::string, std::vector<folly::dynamic>>> splits;
  // Per task, the node it must run on, or "" for any.
  std::vector<std::string> nodes;
  // Attempts so far, and those that left partitions missing.
  int32_t attempts{0};
  int32_t incompleteAttempts{0};
  // Partitions done, for a stage without shuffle output; a map stage's are
  // in the tracker.
  std::vector<bool> finished;
  // A result stage's pages and rows, per partition.
  std::vector<std::vector<std::string>> resultPages;
  std::vector<int64_t> resultRows;

  int32_t numTasks() const {
    return static_cast<int32_t>(splits.size());
  }
};

struct DagScheduler::JobContext {
  std::string jobId;
  std::shared_ptr<core::QueryCtx> queryCtx;
  std::shared_ptr<velox::memory::MemoryPool> pool;
  std::unique_ptr<velox::exec::SimpleExpressionEvaluator> evaluator;
  runner::RunnerSessionPtr session;
  // Where the result tasks write their rows, if not back to the driver.
  const OutputFiles* outputFiles{nullptr};
  // Stages by the shuffle they write.
  std::map<int32_t, StageState*> producers;
};

DagScheduler::DagScheduler(
    std::shared_ptr<SchedulerBackend> backend,
    DagSchedulerOptions options)
    : options_{std::move(options)},
      reuseOutputs_{
          !options_.config.contains(kReuseOutputs) ||
          folly::to<bool>(options_.config.at(kReuseOutputs))},
      rangeSortPartitions_{
          options_.config.contains(kRangeSortPartitions)
              ? folly::to<int32_t>(options_.config.at(kRangeSortPartitions))
              : 0},
      nodeLocalFiles_{
          options_.config.contains(kNodeLocalFiles) &&
          folly::to<bool>(options_.config.at(kNodeLocalFiles))},
      taskScheduler_{std::move(backend), options_.maxTaskAttempts},
      kilnMemo_{KilnMemo::fromConfig(options_.config)} {
  if (auto store = options_.config.find(std::string(shuffle::kShuffleStore));
      store != options_.config.end() && store->second == "kiln") {
    auto metadata =
        options_.config.find(std::string(shuffle::kKilnMetadata));
    VELOX_USER_CHECK(
        metadata != options_.config.end(),
        "{}=kiln needs config {}",
        shuffle::kShuffleStore,
        shuffle::kKilnMetadata);
    kiln_ = shuffle::kilnClient(metadata->second);
  }
  for (const auto& [key, value] : options_.config) {
    if (!key.starts_with("ember.") || key == kMaxSplitBytes ||
        key == kNodeLocalFiles) {
      keyConfig_[key] = value;
    }
  }
  rootPool_ = velox::memory::memoryManager()->addRootPool(
      fmt::format("ember_driver_{}_{}", options_.appName, ++schedulerCounter));
  planPool_ = rootPool_->addLeafChild("plans");
  taskScheduler_.backend().setExecutorListener(SchedulerBackend::ExecutorListener{
      .onExecutorAdded =
          [this](const std::string& location) {
            mapOutputTracker_.markLocationAvailable(location);
          },
      .onExecutorLost =
          [this](const std::string& location) {
            const auto removed = mapOutputTracker_.markLocationLost(location);
            if (removed > 0) {
              LOG(WARNING) << "Lost " << removed << " map outputs at "
                           << location;
            }
          }});
}

DagScheduler::~DagScheduler() {
  backend().setExecutorListener({});
  // Kept outputs go with the session.
  for (const auto& [shuffleId, bucket] : kilnBuckets_) {
    deleteKilnBucket(bucket);
  }
}

void DagScheduler::removeShuffle(int32_t shuffleId) {
  backend().removeShuffle(shuffleId);
  mapOutputTracker_.unregisterShuffle(shuffleId);
  std::optional<uint64_t> bucket;
  {
    std::lock_guard<std::mutex> l(bucketsMutex_);
    if (auto it = kilnBuckets_.find(shuffleId); it != kilnBuckets_.end()) {
      bucket = it->second;
      kilnBuckets_.erase(it);
    }
  }
  if (bucket.has_value()) {
    deleteKilnBucket(*bucket);
  }
}

void DagScheduler::deleteKilnBucket(uint64_t bucket) {
  try {
    folly::coro::blockingWait(kiln_->co_deleteBucket(bucket));
  } catch (const std::exception& e) {
    // Kiln evicts the chunks in time.
    LOG(WARNING) << "Cannot delete Kiln bucket " << bucket << ": " << e.what();
  }
}

StagePlan DagScheduler::stagePlan(const optimizer::MultiFragmentPlan& plan) {
  return planStages(
      plan,
      planPool_.get(),
      keyConfig_,
      [this](const auto& id) { return cachedOutput(id); },
      /*materialize=*/false,
      rangeSortPartitions_);
}

StagePlan DagScheduler::cachedStagePlan(const std::string& id) {
  auto table = findCachedTable(id);
  VELOX_USER_CHECK_NOT_NULL(table, "Cached Dataframe {} is gone", id);
  return planStages(
      *table->plan(),
      planPool_.get(),
      keyConfig_,
      [this](const auto& id) { return cachedOutput(id); },
      /*materialize=*/true,
      rangeSortPartitions_);
}

CachedOutput DagScheduler::cachedOutput(const std::string& id) {
  const auto stages = cachedStagePlan(id);
  const auto& last = stages.stages.back();
  const auto* write = last.plan->as<shuffle::ShuffleWriteNode>();
  VELOX_CHECK_NOT_NULL(write);
  return CachedOutput{
      findCachedTable(id)->schema,
      write->outputType(),
      write->serdeKind(),
      last.key};
}

std::optional<int32_t> DagScheduler::storedShuffle(
    const std::optional<std::string>& key,
    int32_t numTasks) {
  if (!key.has_value()) {
    return std::nullopt;
  }
  std::lock_guard<std::mutex> l(memoMutex_);
  auto it = memo_.find(*key);
  if (it == memo_.end() && kilnMemo_ != nullptr) {
    std::optional<std::vector<shuffle::MapStatus>> maps;
    try {
      maps = kilnMemo_->get(*key);
    } catch (const std::exception& e) {
      LOG(WARNING) << "Cannot look up stage " << *key << " in Kiln: "
                   << e.what();
    }
    if (maps.has_value() && maps->size() == numTasks) {
      const auto shuffleId = ++shuffleCounter_;
      mapOutputTracker_.registerShuffle(shuffleId, numTasks);
      for (int32_t mapId = 0; mapId < numTasks; ++mapId) {
        mapOutputTracker_.registerMapOutput(
            shuffleId, mapId, std::move((*maps)[mapId]));
      }
      it = memo_.emplace(*key, shuffleId).first;
    }
  }
  if (it == memo_.end() ||
      mapOutputTracker_.numMaps(it->second) != numTasks) {
    return std::nullopt;
  }
  return it->second;
}

void DagScheduler::storeInKiln(const std::string& key, int32_t shuffleId) {
  // A cached table without a stage key is stored under its session's id.
  if (kilnMemo_ == nullptr || key.starts_with("cached ")) {
    return;
  }
  auto maps = mapOutputTracker_.mapStatuses(shuffleId);
  if (!maps.has_value()) {
    return;
  }
  try {
    if (kilnMemo_->put(key, *maps)) {
      // Later programs read the chunks: they stay until Kiln evicts them.
      std::lock_guard<std::mutex> l(bucketsMutex_);
      kilnBuckets_.erase(shuffleId);
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "Cannot store stage " << key << " in Kiln: " << e.what();
  }
}

std::string DagScheduler::explain(const optimizer::MultiFragmentPlan& plan) {
  const auto stages = stagePlan(plan);
  // Stages that run: the result stage, and what running stages read.
  std::set<int32_t> runs{stages.resultStage().stageId};
  std::map<int32_t, std::string> notes;
  for (auto it = stages.stages.rbegin(); it != stages.stages.rend(); ++it) {
    const auto& stage = *it;
    if (!runs.contains(stage.stageId)) {
      notes[stage.stageId] = ", not needed";
      continue;
    }
    if (stage.shuffleId.has_value() && stage.key.has_value()) {
      std::lock_guard<std::mutex> l(memoMutex_);
      if (memo_.contains(*stage.key)) {
        notes[stage.stageId] = ", reused";
        continue;
      }
    }
    for (const auto& input : stage.shuffleInputs) {
      if (input.cacheId.has_value()) {
        std::lock_guard<std::mutex> l(memoMutex_);
        notes[stage.stageId] += fmt::format(
            ", reads {} ({})",
            *input.cacheId,
            memo_.contains(cacheMemoKey(*input.cacheId, input.cacheKey))
                ? "stored"
                : "not stored yet");
      }
    }
    runs.insert(stage.parents.begin(), stage.parents.end());
  }
  return stages.toString([&](const Stage& stage) {
    auto it = notes.find(stage.stageId);
    return it == notes.end() ? std::string() : it->second;
  });
}

std::vector<std::map<std::string, std::vector<folly::dynamic>>>
DagScheduler::scanSplits(
    const Stage& stage,
    JobContext& job,
    std::vector<std::string>& nodes) {
  // A scan's files on one node; node "" for files any executor reads.
  struct NodeFiles {
    std::string node;
    std::vector<rpc::FileInfo> files;
  };
  // Ranges per scan and node, in order.
  struct NodeRanges {
    std::string node;
    std::vector<std::shared_ptr<FileRangeSplit>> ranges;
  };
  std::vector<std::vector<NodeRanges>> rangesPerScan;
  std::vector<int64_t> maxSplitBytesPerScan;
  for (const auto& scan : stage.scans) {
    const auto* handle =
        scan->tableHandle()->asChecked<file::FileTableHandle>();
    std::vector<NodeFiles> filesPerNode;
    if (nodeLocalFiles_) {
      for (auto& node : backend().nodes()) {
        auto files = backend().listFiles(
            node, handle->schemaTableName().schema, handle->filePath());
        filesPerNode.push_back({std::move(node), std::move(files)});
      }
    } else {
      filesPerNode.push_back({"", listFiles(*scan, job)});
    }

    // Files split into byte ranges. A text range owns the lines that start in
    // it, a gensort range the records, a Parquet range the row groups. A
    // metadata table (e.g. a Parquet file's row groups) reads whole files.
    const bool wholeFiles = !handle->suffix().empty();
    int64_t totalBytes = 0;
    for (const auto& [node, files] : filesPerNode) {
      for (const auto& file : files) {
        totalBytes += file.size + kOpenCostBytes;
      }
    }
    int64_t maxSplitBytes = std::min(
        kSplitBytesCeiling,
        std::max(kSplitBytesFloor, totalBytes / options_.parallelism));
    if (auto it = options_.config.find(kMaxSplitBytes);
        it != options_.config.end()) {
      maxSplitBytes = folly::to<int64_t>(it->second);
    }
    VELOX_USER_CHECK_GT(maxSplitBytes, 0);
    auto& scanRanges = rangesPerScan.emplace_back();
    for (const auto& [node, files] : filesPerNode) {
      auto& ranges = scanRanges.emplace_back(NodeRanges{node, {}}).ranges;
      for (const auto& file : files) {
        const auto step =
            wholeFiles ? std::max<int64_t>(1, file.size) : maxSplitBytes;
        for (int64_t start = 0; start < file.size; start += step) {
          ranges.push_back(std::make_shared<FileRangeSplit>(
              handle->connectorId(),
              file.path,
              start,
              std::min(step, file.size - start)));
        }
      }
    }
    maxSplitBytesPerScan.push_back(maxSplitBytes);
  }

  // Several scans: 'parallelism' tasks share their ranges.
  if (rangesPerScan.size() != 1) {
    VELOX_USER_CHECK(
        !nodeLocalFiles_,
        "{} supports one scan per stage, not {}",
        kNodeLocalFiles,
        rangesPerScan.size());
    std::vector<std::map<std::string, std::vector<folly::dynamic>>> result(
        options_.parallelism);
    for (size_t i = 0; i < stage.scans.size(); ++i) {
      const auto& nodeId = stage.scans[i]->id();
      const auto& ranges = rangesPerScan[i][0].ranges;
      for (size_t j = 0; j < ranges.size(); ++j) {
        result[j % result.size()][nodeId].push_back(ranges[j]->serialize());
      }
    }
    nodes.assign(result.size(), "");
    return result;
  }

  // One scan: each node's ranges packed in order into tasks of about
  // 'maxSplitBytes', each range counting kOpenCostBytes extra, so small files
  // share a task.
  const auto& nodeId = stage.scans[0]->id();
  std::vector<std::map<std::string, std::vector<folly::dynamic>>> result;
  nodes.clear();
  for (const auto& [node, ranges] : rangesPerScan[0]) {
    int64_t taskBytes = 0;
    for (const auto& range : ranges) {
      const auto bytes = range->length + kOpenCostBytes;
      if (nodes.empty() || nodes.back() != node ||
          (taskBytes > 0 && taskBytes + bytes > maxSplitBytesPerScan[0])) {
        result.emplace_back();
        nodes.push_back(node);
        taskBytes = 0;
      }
      result.back()[nodeId].push_back(range->serialize());
      taskBytes += bytes;
    }
  }
  if (result.empty()) {
    // No files: one task that reads nothing.
    result.emplace_back();
    nodes.push_back("");
  }
  return result;
}

std::vector<rpc::FileInfo> DagScheduler::listFiles(
    const core::TableScanNode& scan,
    JobContext& job) {
  auto source = runner::ConnectorSplitSourceFactory().splitSourceForScan(
      job.session, scan, *job.evaluator, nullptr, std::nullopt);
  std::vector<rpc::FileInfo> files;
  std::exception_ptr error;
  try {
    for (;;) {
      auto batch = folly::coro::blockingWait(source->co_getSplits(1024));
      for (auto& split : batch.splits) {
        auto fileSplit =
            std::dynamic_pointer_cast<file::FileSplit>(split.connectorSplit);
        VELOX_CHECK_NOT_NULL(fileSplit, "Expected a file split");
        const auto size =
            velox::filesystems::getFileSystem(fileSplit->filePath, nullptr)
                ->openFileForRead(fileSplit->filePath)
                ->size();
        files.push_back(rpc::FileInfo{fileSplit->filePath, size});
      }
      if (batch.noMoreSplits) {
        break;
      }
    }
  } catch (...) {
    error = std::current_exception();
  }
  folly::coro::blockingWait(source->co_close());
  if (error) {
    std::rethrow_exception(error);
  }
  return files;
}

std::vector<velox::RowVectorPtr> DagScheduler::runJob(
    const optimizer::MultiFragmentPlan& plan,
    velox::memory::MemoryPool* pool) {
  auto stages = stagePlan(plan);
  return runStages(stages, std::nullopt, pool);
}

int64_t DagScheduler::write(
    const optimizer::MultiFragmentPlan& plan,
    const OutputFiles& files) {
  auto stages = stagePlan(plan);
  int64_t rows = 0;
  runStages(stages, std::nullopt, nullptr, nullptr, &files, &rows);
  return rows;
}

int32_t DagScheduler::materialize(const std::string& id) {
  auto stages = cachedStagePlan(id);
  int32_t shuffleId = 0;
  runStages(
      stages,
      cacheMemoKey(id, stages.stages.back().key),
      nullptr,
      &shuffleId);
  return shuffleId;
}

bool DagScheduler::uncache(const std::string& id) {
  if (findCachedTable(id) == nullptr) {
    return false;
  }
  const auto stages = cachedStagePlan(id);
  const auto key = cacheMemoKey(id, stages.stages.back().key);
  std::optional<int32_t> shuffleId;
  {
    std::lock_guard<std::mutex> l(memoMutex_);
    auto it = memo_.find(key);
    if (it == memo_.end()) {
      return false;
    }
    shuffleId = it->second;
    memo_.erase(it);
  }
  removeShuffle(*shuffleId);
  return true;
}

std::vector<velox::RowVectorPtr> DagScheduler::runStages(
    StagePlan& stages,
    const std::optional<std::string>& outputKey,
    velox::memory::MemoryPool* pool,
    int32_t* outputShuffle,
    const OutputFiles* outputFiles,
    int64_t* writtenRows) {
  JobContext job;
  job.jobId = fmt::format("{}_{}", options_.appName, ++jobCounter_);
  job.outputFiles = outputFiles;
  job.queryCtx = core::QueryCtx::create(
      /*executor=*/nullptr,
      core::QueryConfig(
          std::unordered_map<std::string, std::string>(
              options_.config.begin(), options_.config.end())),
      /*connectorConfigs=*/{},
      velox::cache::AsyncDataCache::getInstance(),
      /*pool=*/nullptr,
      /*spillExecutor=*/nullptr,
      job.jobId);
  job.pool = job.queryCtx->pool()->addLeafChild("splits");
  job.evaluator = std::make_unique<velox::exec::SimpleExpressionEvaluator>(
      job.queryCtx.get(), job.pool.get());
  job.session = std::make_shared<runner::RunnerSession>(
      std::make_shared<axiom_connector::ConnectorContext>(
          job.jobId,
          options_.appName,
          axiom_connector::ConnectorProperties{},
          axiom_connector::ConnectorContext::noopStatWriterProvider()),
      std::make_shared<velox::NoopRuntimeStatWriter>(),
      runner::Properties{});

  // Cached tables this job reads are stored first; their placeholder
  // shuffle ids become the ids of the shuffles that hold them.
  std::map<int32_t, int32_t> newIds;
  for (const auto& stage : stages.stages) {
    for (const auto& input : stage.shuffleInputs) {
      if (input.cacheId.has_value()) {
        newIds[input.shuffleId] = materialize(*input.cacheId);
      }
    }
  }

  std::vector<StageState> states(stages.stages.size());
  for (size_t i = 0; i < stages.stages.size(); ++i) {
    const auto& stage = stages.stages[i];
    auto& state = states[i];
    state.stage = &stage;
    state.isResult = !outputKey.has_value() && i + 1 == stages.stages.size();
    // A stage reading cached tables runs a task per stored map output.
    int32_t cachedMaps = 0;
    for (const auto& input : stage.shuffleInputs) {
      if (input.cacheId.has_value()) {
        cachedMaps = std::max(
            cachedMaps,
            mapOutputTracker_.numMaps(newIds.at(input.shuffleId)).value());
      }
    }
    if (stage.numTasks.has_value()) {
      state.splits.resize(*stage.numTasks);
      for (size_t j = 0; j < stage.scans.size(); ++j) {
        VELOX_NYI("Scans in a stage with a fixed task count");
      }
    } else if (cachedMaps > 0 && stage.scans.empty()) {
      state.splits.resize(cachedMaps);
    } else {
      state.splits = scanSplits(stage, job, state.nodes);
      if (state.numTasks() < cachedMaps) {
        state.splits.resize(cachedMaps);
      }
    }
    state.nodes.resize(state.numTasks());
  }

  // The memo key of a stage's output.
  auto memoKey = [&](const StageState& state) {
    return outputKey.has_value() && &state == &states.back()
        ? outputKey
        : state.stage->key;
  };

  // Shuffles get ids unique in the session; a stage with stored output takes
  // the id of the shuffle that holds it.
  std::set<int32_t> reused;
  for (auto& state : states) {
    const auto& stage = *state.stage;
    if (!stage.shuffleId.has_value()) {
      continue;
    }
    auto stored = storedShuffle(memoKey(state), state.numTasks());
    if (stored.has_value()) {
      reused.insert(*stored);
    }
    newIds[*stage.shuffleId] = stored.value_or(++shuffleCounter_);
  }
  stages.renumberShuffles(newIds, planPool_.get());

  // The job's own shuffles: kept for reuse if keyed, otherwise removed. A
  // cached table's output is always kept.
  std::map<int32_t, const StageState*> written;
  SCOPE_EXIT {
    for (const auto& [shuffleId, state] : written) {
      const auto key = memoKey(*state);
      const bool isOutput = outputKey.has_value() && state == &states.back();
      if ((reuseOutputs_ || isOutput) && key.has_value()) {
        std::lock_guard<std::mutex> l(memoMutex_);
        auto [it, inserted] = memo_.emplace(*key, shuffleId);
        if (inserted) {
          storeInKiln(*key, shuffleId);
          continue;
        }
        if (!reused.contains(it->second) && !written.contains(it->second)) {
          // The stored output did not fit this job; keep the new one.
          removeShuffle(it->second);
          it->second = shuffleId;
          storeInKiln(*key, shuffleId);
          continue;
        }
      }
      removeShuffle(shuffleId);
    }
  };

  // Write partitions per shuffle, to check against reader task counts.
  std::map<int32_t, int32_t> shufflePartitions;

  for (auto& state : states) {
    const auto& stage = *state.stage;
    const auto numTasks = state.numTasks();
    for (auto& taskSplits : state.splits) {
      for (const auto& input : stage.shuffleInputs) {
        taskSplits[input.nodeId];
      }
      for (const auto& scan : stage.scans) {
        taskSplits[scan->id()];
      }
    }

    for (const auto& input : stage.shuffleInputs) {
      if (!input.broadcast && !input.cacheId.has_value()) {
        VELOX_CHECK_EQ(
            shufflePartitions.at(input.shuffleId),
            numTasks,
            "Stage {} reads shuffle {} with a different task count",
            stage.stageId,
            input.shuffleId);
      }
    }

    if (stage.shuffleId.has_value()) {
      if (reused.contains(*stage.shuffleId)) {
        if (options_.onStageCompleted) {
          options_.onStageCompleted(StageAttemptInfo{
              .jobId = job.jobId,
              .stageId = stage.stageId,
              .stageKey = stage.key,
              .shuffleId = stage.shuffleId,
              .reused = true});
        }
      } else {
        written[*stage.shuffleId] = &state;
        mapOutputTracker_.registerShuffle(*stage.shuffleId, numTasks);
        if (kiln_ != nullptr) {
          const auto bucket =
              folly::coro::blockingWait(kiln_->co_createBucket());
          std::lock_guard<std::mutex> l(bucketsMutex_);
          kilnBuckets_[*stage.shuffleId] = bucket;
        }
      }
      shufflePartitions[*stage.shuffleId] =
          stage.plan->as<shuffle::ShuffleWriteNode>()->numPartitions();
      job.producers[*stage.shuffleId] = &state;
    } else {
      state.finished.assign(numTasks, false);
    }
    if (state.isResult) {
      state.resultPages.resize(numTasks);
      state.resultRows.resize(numTasks);
    }
  }

  // The result stage runs the stages it reads first, and so on upstream;
  // stored outputs are complete already, unless some were lost.
  runStage(states.back(), job);

  if (outputKey.has_value()) {
    *outputShuffle = *states.back().stage->shuffleId;
    return {};
  }
  const auto& result = states.back();
  if (outputFiles != nullptr) {
    *writtenRows = 0;
    for (auto rows : result.resultRows) {
      *writtenRows += rows;
    }
    return {};
  }
  std::vector<velox::RowVectorPtr> rows;
  const auto type = result.stage->plan->outputType();
  for (const auto& pages : result.resultPages) {
    for (const auto& page : pages) {
      rows.push_back(deserializeRows(page, type, pool));
    }
  }
  return rows;
}

void DagScheduler::runStage(StageState& state, JobContext& job) {
  const auto& stage = *state.stage;
  auto missingPartitions = [&]() {
    if (stage.shuffleId.has_value()) {
      return mapOutputTracker_.missingMaps(*stage.shuffleId);
    }
    std::vector<int32_t> missing;
    for (int32_t p = 0; p < state.numTasks(); ++p) {
      if (!state.finished[p]) {
        missing.push_back(p);
      }
    }
    return missing;
  };

  for (auto partitions = missingPartitions(); !partitions.empty();
       partitions = missingPartitions()) {
    if (state.incompleteAttempts >= options_.maxStageAttempts) {
      VELOX_FAIL(
          "Stage {} of job {} failed: {} attempts lost their inputs or outputs",
          stage.stageId,
          job.jobId,
          state.incompleteAttempts);
    }

    // Recompute lost inputs; that may lose others, so check them all again.
    bool inputsReady = false;
    while (!inputsReady) {
      inputsReady = true;
      for (const auto& input : stage.shuffleInputs) {
        if (mapOutputTracker_.isComplete(input.shuffleId)) {
          continue;
        }
        if (input.cacheId.has_value()) {
          VELOX_CHECK_EQ(materialize(*input.cacheId), input.shuffleId);
        } else {
          runStage(*job.producers.at(input.shuffleId), job);
        }
        inputsReady = false;
      }
    }

    const auto attempt = state.attempts++;
    if (attempt > 0) {
      LOG(WARNING) << "Resubmitting stage " << stage.stageId << " of job "
                   << job.jobId << " (attempt " << attempt << ") for "
                   << partitions.size() << " of " << state.numTasks()
                   << " partitions";
    }

    std::vector<TaskDescription> tasks(partitions.size());
    bool inputsLost = false;
    for (size_t i = 0; i < partitions.size() && !inputsLost; ++i) {
      const auto p = partitions[i];
      auto& task = tasks[i];
      task.jobId = job.jobId;
      task.stageId = stage.stageId;
      task.stageAttempt = attempt;
      task.partition = p;
      task.fragment = stage.fragment;
      task.splits = state.splits[p];
      task.node = state.nodes[p];
      for (const auto& input : stage.shuffleInputs) {
        // An executor may have been lost since the inputs were complete.
        if (!mapOutputTracker_.isComplete(input.shuffleId)) {
          inputsLost = true;
          break;
        }
        auto& nodeSplits = task.splits[input.nodeId];
        std::vector<std::string> urls;
        if (input.cacheId.has_value()) {
          // Map outputs p, p + numTasks, ...
          const auto numMaps = *mapOutputTracker_.numMaps(input.shuffleId);
          for (int32_t m = p; m < numMaps; m += state.numTasks()) {
            auto mapUrls = mapOutputTracker_.mapBlockUrls(input.shuffleId, m);
            urls.insert(urls.end(), mapUrls.begin(), mapUrls.end());
          }
        } else {
          urls = mapOutputTracker_.blockUrls(
              input.shuffleId, input.broadcast ? 0 : p);
        }
        for (const auto& url : urls) {
          nodeSplits.push_back(
              velox::exec::RemoteConnectorSplit(url).serialize());
        }
      }
      task.resultTask = state.isResult;
      if (state.isResult && job.outputFiles != nullptr) {
        task.outputFiles = *job.outputFiles;
      }
      task.config = options_.config;
      if (stage.shuffleId.has_value()) {
        std::lock_guard<std::mutex> l(bucketsMutex_);
        if (auto it = kilnBuckets_.find(*stage.shuffleId);
            it != kilnBuckets_.end()) {
          task.config[std::string(shuffle::kKilnBucket)] =
              std::to_string(it->second);
        }
      }
    }

    StageAttemptInfo info{
        .jobId = job.jobId,
        .stageId = stage.stageId,
        .attempt = attempt,
        .stageKey = stage.key,
        .shuffleId = stage.shuffleId,
        .partitions = partitions};
    if (!inputsLost) {
      auto taskSet = taskScheduler_.runTaskSet(std::move(tasks));
      for (size_t i = 0; i < partitions.size(); ++i) {
        auto& result = taskSet.results[i];
        if (!result.has_value()) {
          continue;
        }
        const auto p = partitions[i];
        if (stage.shuffleId.has_value()) {
          VELOX_CHECK(result->mapStatus.has_value());
          auto location = result->mapStatus->location;
          if (mapOutputTracker_.registerMapOutput(
                  *stage.shuffleId, p, std::move(*result->mapStatus))) {
            info.outputLocations[p] = std::move(location);
          }
        } else {
          state.finished[p] = true;
        }
        if (state.isResult) {
          state.resultPages[p] = std::move(result->resultPages);
          state.resultRows[p] = result->outputRows;
        }
      }
      for (const auto& failure : taskSet.fetchFailures) {
        // The executor may be alive, but its outputs of this shuffle are not
        // to be trusted. If it is dead, the backend reports it lost too.
        const auto removed = mapOutputTracker_.removeOutputsAt(
            failure.block.shuffleId, failure.location);
        LOG(WARNING) << "Stage " << stage.stageId << " could not fetch "
                     << failure.block.toString() << " from "
                     << failure.location << "; " << removed
                     << " map outputs there will be recomputed";
      }
      info.fetchFailures = std::move(taskSet.fetchFailures);
    }
    if (!missingPartitions().empty()) {
      ++state.incompleteAttempts;
    }
    if (options_.onStageCompleted) {
      options_.onStageCompleted(info);
    }
  }
}

} // namespace ember::driver
