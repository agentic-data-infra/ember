#include "ember/driver/DagScheduler.h"

#include <algorithm>
#include <numeric>
#include <random>
#include <set>

#include <folly/ScopeGuard.h>
#include <folly/coro/BlockingWait.h>
#include <glog/logging.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/runner/LocalRunner.h"
#include "ember/CachedConnector.h"
#include "ember/CachedTable.h"
#include "ember/FileSerde.h"
#include "ember/ParquetConnector.h"
#include "ember/RowPages.h"
#include "ember/driver/RangeBounds.h"
#include "ember/shuffle/KilnShuffle.h"
#include "ember/shuffle/ShuffleConfig.h"
#include "ember/shuffle/ShuffleWrite.h"
#include "kiln/client/Client.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Exchange.h"
#include "velox/expression/Expr.h"

namespace ember::driver {

namespace axiom_connector = facebook::axiom::connector;
namespace core = facebook::velox::core;
namespace file = facebook::axiom::connector::file;
namespace hive = facebook::velox::connector::hive;
namespace optimizer = facebook::axiom::optimizer;
namespace runner = facebook::axiom::runner;
namespace velox = facebook::velox;

namespace {

constexpr int64_t kSplitBytesFloor = 4 << 20;
constexpr int64_t kSplitBytesCeiling = 128 << 20;
// What opening a file costs, in bytes read: small files share a task.
constexpr int64_t kOpenCostBytes = 4 << 20;

std::atomic<int32_t> schedulerCounter{0};

// Shuffles items [begin, end) with Fisher-Yates: the same order from the same
// seed on any build, which std::shuffle does not promise.
template <typename T>
void shuffleRange(
    std::vector<T>& items,
    size_t begin,
    size_t end,
    std::mt19937_64& rng) {
  for (size_t n = end - begin; n > 1; --n) {
    std::swap(items[begin + n - 1], items[begin + rng() % n]);
  }
}

// The file format and path of what a scan reads.
std::pair<std::string, std::string> scanSource(
    const velox::connector::ConnectorTableHandle& handle) {
  if (handle.connectorId() == kParquetConnectorId) {
    return {kParquetSchema, handle.name()};
  }
  const auto* fileHandle = handle.asChecked<file::FileTableHandle>();
  return {fileHandle->schemaTableName().schema, fileHandle->filePath()};
}

// A split of bytes [start, start + length) of 'path'.
folly::dynamic rangeSplit(
    const velox::connector::ConnectorTableHandle& handle,
    const std::string& path,
    int64_t start,
    int64_t length) {
  if (handle.connectorId() == kParquetConnectorId) {
    return hive::HiveConnectorSplitBuilder(path)
        .connectorId(handle.connectorId())
        .fileFormat(velox::dwio::common::FileFormat::PARQUET)
        .start(start)
        .length(length)
        .build()
        ->serialize();
  }
  return FileRangeSplit(handle.connectorId(), path, start, length).serialize();
}

const std::string& splitPath(const velox::connector::ConnectorSplit& split) {
  if (const auto* parquet = dynamic_cast<const hive::HiveConnectorSplit*>(&split)) {
    return parquet->filePath;
  }
  return split.asChecked<file::FileSplit>()->filePath;
}

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
  // Stages by the shuffle they write.
  std::map<int32_t, StageState*> producers;
};

DagScheduler::DagScheduler(
    std::shared_ptr<SchedulerBackend> backend,
    DagSchedulerOptions options)
    : options_{std::move(options)},
      shuffleTarget_{shuffle::ShuffleTarget::fromConfig(options_.config)},
      reuseOutputs_{
          !options_.config.contains(kReuseOutputs) ||
          folly::to<bool>(options_.config.at(kReuseOutputs))},
      sampleRanges_{
          !options_.config.contains(kRangeSortSample) ||
          folly::to<bool>(options_.config.at(kRangeSortSample))},
      nodeLocalFiles_{
          options_.config.contains(kNodeLocalFiles) &&
          folly::to<bool>(options_.config.at(kNodeLocalFiles))},
      shuffleSplits_{
          options_.config.contains(kShuffleSplits)
              ? options_.config.at(kShuffleSplits)
              : "none"},
      shuffleSeed_{
          options_.config.contains(kShuffleSeed)
              ? folly::to<uint64_t>(options_.config.at(kShuffleSeed))
              : 0},
      taskScheduler_{std::move(backend), options_.maxTaskAttempts},
      kilnMemo_{KilnMemo::fromConfig(options_.config)} {
  VELOX_USER_CHECK(
      shuffleSplits_ == "none" || shuffleSplits_ == "file" ||
          shuffleSplits_ == "global",
      "{} must be none, file or global, not '{}'",
      kShuffleSplits,
      shuffleSplits_);
  if (shuffleTarget_.store == "kiln") {
    kiln_ = shuffle::kilnClient(shuffleTarget_.kilnMetadata);
  }
  for (const auto& [key, value] : options_.config) {
    // Spilling does not change results.
    if (key == core::QueryConfig::kSpillEnabled) {
      continue;
    }
    if (!key.starts_with("ember.") || key == kMaxSplitBytes ||
        key == kNodeLocalFiles || key == kShuffleSplits ||
        key == kShuffleSeed) {
      keyConfig_[key] = value;
    }
  }
  if (!sampleRanges_) {
    keyConfig_[kRangeSortSample] = "false";
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
  {
    std::lock_guard<std::mutex> l(rangeBoundsMutex_);
    rangeBounds_.erase(shuffleId);
  }
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
      [this](const auto& id) { return cachedStagePlan(id).stages.back().key; },
      /*materialize=*/false,
      shuffleTarget_);
}

StagePlan DagScheduler::cachedStagePlan(const std::string& id) {
  auto table = findCachedTable(id);
  VELOX_USER_CHECK_NOT_NULL(table, "Cached Dataframe {} is gone", id);
  return planStages(
      *table->plan(),
      planPool_.get(),
      keyConfig_,
      [this](const auto& id) { return cachedStagePlan(id).stages.back().key; },
      /*materialize=*/true,
      shuffleTarget_);
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
      // Later programs read the chunks: they stay, but Kiln may evict them
      // now, like cached ones.
      std::optional<uint64_t> bucket;
      {
        std::lock_guard<std::mutex> l(bucketsMutex_);
        if (auto it = kilnBuckets_.find(shuffleId); it != kilnBuckets_.end()) {
          bucket = it->second;
          kilnBuckets_.erase(it);
        }
      }
      if (bucket.has_value()) {
        folly::coro::blockingWait(kiln_->co_releaseBucket(*bucket));
      }
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
  struct Range {
    folly::dynamic split;
    int64_t length;
  };
  // Ranges per scan and node, in order.
  struct NodeRanges {
    std::string node;
    std::vector<Range> ranges;
  };
  std::vector<std::vector<NodeRanges>> rangesPerScan;
  std::vector<int64_t> maxSplitBytesPerScan;
  for (const auto& scan : stage.scans) {
    const auto& handle = *scan->tableHandle();
    const auto [schema, path] = scanSource(handle);
    std::vector<NodeFiles> filesPerNode;
    if (nodeLocalFiles_) {
      for (auto& node : backend().nodes()) {
        auto files = backend().listFiles(node, schema, path);
        filesPerNode.push_back({std::move(node), std::move(files)});
      }
    } else {
      filesPerNode.push_back({"", listFiles(*scan, job)});
    }

    // Files split into byte ranges. A text range owns the lines that start in
    // it, a gensort range the records, a Parquet range the row groups.
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
    // Ranges in file order, or shuffled (kShuffleSplits) from files sorted
    // by path, so that the seed alone sets the order.
    std::mt19937_64 rng(shuffleSeed_);
    auto& scanRanges = rangesPerScan.emplace_back();
    for (auto& [node, files] : filesPerNode) {
      if (shuffleSplits_ != "none") {
        std::sort(files.begin(), files.end(), [](const auto& a, const auto& b) {
          return a.path < b.path;
        });
      }
      auto& ranges = scanRanges.emplace_back(NodeRanges{node, {}}).ranges;
      for (const auto& file : files) {
        const auto first = ranges.size();
        for (int64_t start = 0; start < file.size; start += maxSplitBytes) {
          const auto length = std::min(maxSplitBytes, file.size - start);
          ranges.push_back(
              {rangeSplit(handle, file.path, start, length), length});
        }
        if (shuffleSplits_ == "file") {
          shuffleRange(ranges, first, ranges.size(), rng);
        }
      }
      if (shuffleSplits_ == "global") {
        shuffleRange(ranges, 0, ranges.size(), rng);
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
        result[j % result.size()][nodeId].push_back(ranges[j].split);
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
      const auto bytes = range.length + kOpenCostBytes;
      if (nodes.empty() || nodes.back() != node ||
          (taskBytes > 0 && taskBytes + bytes > maxSplitBytesPerScan[0])) {
        result.emplace_back();
        nodes.push_back(node);
        taskBytes = 0;
      }
      result.back()[nodeId].push_back(range.split);
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
        const auto path = splitPath(*split.connectorSplit);
        const auto size = velox::filesystems::getFileSystem(path, nullptr)
                              ->openFileForRead(path)
                              ->size();
        files.push_back(rpc::FileInfo{path, size});
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
  return runStages(stages, std::nullopt, pool).rows;
}

int32_t DagScheduler::materialize(const std::string& id) {
  auto stages = cachedStagePlan(id);
  const auto key = cacheMemoKey(id, stages.stages.back().key);
  return runStages(stages, key, nullptr).outputShuffle;
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

DagScheduler::JobResult DagScheduler::runStages(
    StagePlan& stages,
    const std::optional<std::string>& outputKey,
    velox::memory::MemoryPool* pool) {
  JobContext job;
  job.jobId = fmt::format("{}_{}", options_.appName, ++jobCounter_);
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
  stages.renumberShuffles(newIds);

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

  JobResult jobResult;
  if (outputKey.has_value()) {
    jobResult.outputShuffle = *states.back().stage->shuffleId;
    return jobResult;
  }
  const auto& result = states.back();
  const auto type = result.stage->plan->outputType();
  for (const auto& pages : result.resultPages) {
    for (const auto& page : pages) {
      jobResult.rows.push_back(deserializeRows(page, type, pool));
    }
  }
  return jobResult;
}

std::optional<std::vector<std::string>> DagScheduler::storedRangeBounds(
    int32_t shuffleId) {
  std::lock_guard<std::mutex> l(rangeBoundsMutex_);
  auto it = rangeBounds_.find(shuffleId);
  if (it == rangeBounds_.end()) {
    return std::nullopt;
  }
  return it->second;
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

  // The tasks of 'partitions', or nothing if an input was lost.
  auto makeTasks = [&](const folly::dynamic& fragment,
                       const std::string& jobId,
                       int32_t attempt,
                       bool resultTask,
                       const std::vector<int32_t>& partitions)
      -> std::optional<std::vector<TaskDescription>> {
    const auto compression =
        velox::core::QueryConfig(
            {options_.config.begin(), options_.config.end()})
            .shuffleCompressionKind();
    std::vector<TaskDescription> tasks(partitions.size());
    for (size_t i = 0; i < partitions.size(); ++i) {
      const auto p = partitions[i];
      auto& task = tasks[i];
      task.jobId = jobId;
      task.stageId = stage.stageId;
      task.stageAttempt = attempt;
      task.partition = p;
      task.fragment = fragment;
      task.splits = state.splits[p];
      task.node = state.nodes[p];
      for (const auto& input : stage.shuffleInputs) {
        // An executor may have been lost since the inputs were complete.
        if (!mapOutputTracker_.isComplete(input.shuffleId)) {
          return std::nullopt;
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
              input.cacheId.has_value()
                  ? CachedBlockSplit(url, compression).serialize()
                  : velox::exec::RemoteConnectorSplit(url).serialize());
        }
      }
      task.resultTask = resultTask;
      task.config = options_.config;
    }
    return tasks;
  };

  auto removeFailedOutputs = [&](const auto& failures) {
    for (const auto& failure : failures) {
      // The executor may be alive, but its outputs of this shuffle are not
      // to be trusted. If it is dead, the backend reports it lost too.
      const auto removed = mapOutputTracker_.removeOutputsAt(
          failure.block.shuffleId, failure.location);
      LOG(WARNING) << "Stage " << stage.stageId << " could not fetch "
                   << failure.block.toString() << " from "
                   << failure.location << "; " << removed
                   << " map outputs there will be recomputed";
    }
  };

  // Split points from a sample of the stage's input, or nothing if inputs
  // were lost. The tasks use their own job id, so that they do not clash with
  // the stage's.
  auto sampleRangeBounds = [&]() -> std::optional<std::vector<std::string>> {
    std::vector<int32_t> all(state.numTasks());
    std::iota(all.begin(), all.end(), 0);
    auto tasks = makeTasks(
        sampleFragment(stage, state.numTasks()),
        job.jobId + "_sample",
        state.incompleteAttempts,
        /*resultTask=*/true,
        all);
    if (!tasks.has_value()) {
      return std::nullopt;
    }
    auto taskSet = taskScheduler_.runTaskSet(std::move(*tasks));
    if (!taskSet.fetchFailures.empty()) {
      removeFailedOutputs(taskSet.fetchFailures);
      return std::nullopt;
    }
    const auto type = sampleRowType();
    std::vector<velox::RowVectorPtr> samples;
    for (const auto& result : taskSet.results) {
      if (result.has_value()) {
        for (const auto& page : result->resultPages) {
          samples.push_back(deserializeRows(page, type, job.pool.get()));
        }
      }
    }
    return rangeBounds(samples, numRanges(stage));
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

    // A range shuffle keeps its split points when tasks run again, so that
    // the partitions of old and new map outputs match.
    auto write = std::dynamic_pointer_cast<const shuffle::ShuffleWriteNode>(
        stage.plan);
    if (sampleRanges_ && writesRanges(stage)) {
      auto bounds = storedRangeBounds(*stage.shuffleId);
      if (!bounds.has_value()) {
        // Only a shuffle restored from Kiln's catalog has outputs and no
        // split points, and they cannot be told from its outputs.
        VELOX_CHECK_EQ(
            partitions.size(),
            state.numTasks(),
            "Stage {} cannot recompute part of a range shuffle read from Kiln",
            stage.stageId);
        bounds = sampleRangeBounds();
        if (!bounds.has_value()) {
          ++state.incompleteAttempts;
          continue;
        }
        std::lock_guard<std::mutex> l(rangeBoundsMutex_);
        rangeBounds_[*stage.shuffleId] = *bounds;
      }
      write = withBounds(stage, *bounds);
    }

    const auto attempt = state.attempts++;
    if (attempt > 0) {
      LOG(WARNING) << "Resubmitting stage " << stage.stageId << " of job "
                   << job.jobId << " (attempt " << attempt << ") for "
                   << partitions.size() << " of " << state.numTasks()
                   << " partitions";
    }

    // Each shuffle has its own Kiln bucket.
    if (write != nullptr) {
      std::lock_guard<std::mutex> l(bucketsMutex_);
      if (auto it = kilnBuckets_.find(*stage.shuffleId);
          it != kilnBuckets_.end()) {
        write = write->withKilnBucket(it->second);
      }
    }

    auto tasks = makeTasks(
        write != nullptr ? write->serialize() : stage.fragment,
        job.jobId,
        attempt,
        state.isResult,
        partitions);

    StageAttemptInfo info{
        .jobId = job.jobId,
        .stageId = stage.stageId,
        .attempt = attempt,
        .stageKey = stage.key,
        .shuffleId = stage.shuffleId,
        .partitions = partitions};
    if (tasks.has_value()) {
      auto taskSet = taskScheduler_.runTaskSet(std::move(*tasks));
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
      removeFailedOutputs(taskSet.fetchFailures);
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
