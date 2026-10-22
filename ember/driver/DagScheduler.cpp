#include "ember/driver/DagScheduler.h"

#include <folly/ScopeGuard.h>
#include <folly/coro/BlockingWait.h>
#include <glog/logging.h>

#include "axiom/connectors/ConnectorMetadata.h"
#include "axiom/runner/LocalRunner.h"
#include "ember/FileSerde.h"
#include "ember/RowPages.h"
#include "ember/TextFileHandler.h"
#include "ember/shuffle/ShuffleWrite.h"
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

std::atomic<int32_t> schedulerCounter{0};

} // namespace

struct DagScheduler::StageState {
  const Stage* stage;
  bool isResult{false};
  // Splits of each task before shuffle blocks are added: file ranges for
  // scans, empty lists for the other split nodes.
  std::vector<std::map<std::string, std::vector<folly::dynamic>>> splits;
  // Attempts so far, and those that left partitions missing.
  int32_t attempts{0};
  int32_t incompleteAttempts{0};
  // Partitions done, for a stage without shuffle output; a map stage's are
  // in the tracker.
  std::vector<bool> finished;
  // A result stage's pages, per partition.
  std::vector<std::vector<std::string>> resultPages;

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
      taskScheduler_{std::move(backend), options_.maxTaskAttempts} {
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
}

StagePlan DagScheduler::stagePlan(const optimizer::MultiFragmentPlan& plan) {
  return planStages(plan, planPool_.get());
}

std::vector<std::map<std::string, std::vector<folly::dynamic>>>
DagScheduler::scanSplits(const Stage& stage, JobContext& job) {
  // Splits per scan, in order.
  std::vector<std::vector<std::shared_ptr<FileRangeSplit>>> splitsPerScan;
  for (const auto& scan : stage.scans) {
    auto source = runner::ConnectorSplitSourceFactory().splitSourceForScan(
        job.session, *scan, *job.evaluator, nullptr, std::nullopt);
    std::vector<std::shared_ptr<file::FileSplit>> fileSplits;
    std::exception_ptr error;
    try {
      for (;;) {
        auto batch = folly::coro::blockingWait(source->co_getSplits(1024));
        for (auto& split : batch.splits) {
          auto fileSplit =
              std::dynamic_pointer_cast<file::FileSplit>(split.connectorSplit);
          VELOX_CHECK_NOT_NULL(fileSplit, "Expected a file split");
          fileSplits.push_back(std::move(fileSplit));
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

    const auto* handle =
        scan->tableHandle()->asChecked<file::FileTableHandle>();
    std::vector<std::shared_ptr<FileRangeSplit>> ranges;
    if (handle->schemaTableName().schema != kTextSchema) {
      for (const auto& split : fileSplits) {
        ranges.push_back(std::make_shared<FileRangeSplit>(
            split->connectorId, split->filePath));
      }
    } else {
      // Text files split into byte ranges; a range owns the lines starting in it.
      std::vector<int64_t> sizes;
      int64_t totalBytes = 0;
      for (const auto& split : fileSplits) {
        sizes.push_back(
            velox::filesystems::getFileSystem(split->filePath, nullptr)
                ->openFileForRead(split->filePath)
                ->size());
        totalBytes += sizes.back();
      }
      int64_t maxSplitBytes = std::min(
          kSplitBytesCeiling,
          std::max(kSplitBytesFloor, totalBytes / options_.parallelism));
      if (auto it = options_.config.find(kMaxSplitBytes);
          it != options_.config.end()) {
        maxSplitBytes = folly::to<int64_t>(it->second);
      }
      VELOX_USER_CHECK_GT(maxSplitBytes, 0);
      for (size_t i = 0; i < fileSplits.size(); ++i) {
        for (int64_t start = 0; start < sizes[i]; start += maxSplitBytes) {
          ranges.push_back(std::make_shared<FileRangeSplit>(
              fileSplits[i]->connectorId,
              fileSplits[i]->filePath,
              start,
              std::min(maxSplitBytes, sizes[i] - start)));
        }
      }
    }
    splitsPerScan.push_back(std::move(ranges));
  }

  // One scan: a task per split. Several: 'parallelism' tasks share them.
  size_t numTasks = options_.parallelism;
  if (splitsPerScan.size() == 1) {
    numTasks = std::max<size_t>(1, splitsPerScan[0].size());
  }
  std::vector<std::map<std::string, std::vector<folly::dynamic>>> result(
      numTasks);
  for (size_t i = 0; i < stage.scans.size(); ++i) {
    const auto& nodeId = stage.scans[i]->id();
    for (size_t j = 0; j < splitsPerScan[i].size(); ++j) {
      result[j % numTasks][nodeId].push_back(splitsPerScan[i][j]->serialize());
    }
  }
  return result;
}

std::vector<velox::RowVectorPtr> DagScheduler::runJob(
    const optimizer::MultiFragmentPlan& plan,
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

  auto stages = stagePlan(plan);

  std::vector<int32_t> shuffles;
  SCOPE_EXIT {
    for (auto shuffleId : shuffles) {
      backend().removeShuffle(shuffleId);
      mapOutputTracker_.unregisterShuffle(shuffleId);
    }
  };

  // Write partitions per shuffle, to check against reader task counts.
  std::map<int32_t, int32_t> shufflePartitions;

  std::vector<StageState> states(stages.stages.size());
  for (size_t i = 0; i < stages.stages.size(); ++i) {
    const auto& stage = stages.stages[i];
    auto& state = states[i];
    state.stage = &stage;
    state.isResult = i + 1 == stages.stages.size();
    if (stage.numTasks.has_value()) {
      state.splits.resize(*stage.numTasks);
      for (size_t j = 0; j < stage.scans.size(); ++j) {
        VELOX_NYI("Scans in a stage with a fixed task count");
      }
    } else {
      state.splits = scanSplits(stage, job);
    }
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
      if (!input.broadcast) {
        VELOX_CHECK_EQ(
            shufflePartitions.at(input.shuffleId),
            numTasks,
            "Stage {} reads shuffle {} with a different task count",
            stage.stageId,
            input.shuffleId);
      }
    }

    if (stage.shuffleId.has_value()) {
      shuffles.push_back(*stage.shuffleId);
      mapOutputTracker_.registerShuffle(*stage.shuffleId, numTasks);
      shufflePartitions[*stage.shuffleId] =
          stage.plan->as<shuffle::ShuffleWriteNode>()->numPartitions();
      job.producers[*stage.shuffleId] = &state;
    } else {
      state.finished.assign(numTasks, false);
    }
    if (state.isResult) {
      state.resultPages.resize(numTasks);
    }
  }

  // Stages run in dependency order; a stage whose inputs were lost reruns
  // their producers first.
  for (auto& state : states) {
    runStage(state, job);
  }

  std::vector<velox::RowVectorPtr> rows;
  const auto& result = states.back();
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
        if (!mapOutputTracker_.isComplete(input.shuffleId)) {
          runStage(*job.producers.at(input.shuffleId), job);
          inputsReady = false;
        }
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
      for (const auto& input : stage.shuffleInputs) {
        // An executor may have been lost since the inputs were complete.
        if (!mapOutputTracker_.isComplete(input.shuffleId)) {
          inputsLost = true;
          break;
        }
        auto& nodeSplits = task.splits[input.nodeId];
        for (const auto& url : mapOutputTracker_.blockUrls(
                 input.shuffleId, input.broadcast ? 0 : p)) {
          nodeSplits.push_back(
              velox::exec::RemoteConnectorSplit(url).serialize());
        }
      }
      task.resultTask = state.isResult;
      task.config = options_.config;
    }

    StageAttemptInfo info{
        .jobId = job.jobId,
        .stageId = stage.stageId,
        .attempt = attempt,
        .stageKey = stage.key,
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
