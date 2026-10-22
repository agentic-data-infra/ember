#include "ember/worker/Executor.h"

#include <filesystem>

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "ember/RowPages.h"
#include "ember/Runtime.h"
#include "ember/shuffle/ShuffleConfig.h"
#include "ember/shuffle/ShuffleWrite.h"
#include "ember/worker/TaskDescription.h"
#include "velox/common/time/Timer.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Task.h"

namespace ember {

namespace core = facebook::velox::core;
namespace exec = facebook::velox::exec;
namespace velox = facebook::velox;

namespace {

void collectSplitNodes(
    const core::PlanNodePtr& node,
    std::vector<core::PlanNodeId>& ids) {
  if (node->requiresSplits()) {
    ids.push_back(node->id());
  }
  for (const auto& source : node->sources()) {
    collectSplitNodes(source, ids);
  }
}

} // namespace

Executor::Executor(ExecutorOptions options) : options_{std::move(options)} {
  initializeRuntime();
  blockStore_ = std::make_shared<shuffle::ShuffleBlockStore>(
      options_.location, options_.shuffleDirectory);
  shuffle::ShuffleBlockStore::registerStore(blockStore_);
  driverExecutor_ =
      std::make_unique<folly::CPUThreadPoolExecutor>(options_.numThreads);
  rootPool_ = velox::memory::memoryManager()->addRootPool(
      fmt::format("ember_executor_{}", options_.location));
  resultPool_ = rootPool_->addLeafChild("results");
  planPool_ = rootPool_->addLeafChild("plans");
}

Executor::~Executor() {
  driverExecutor_->join();
  shuffle::ShuffleBlockStore::unregisterStore(options_.location);
  std::error_code ec;
  std::filesystem::remove_all(options_.shuffleDirectory, ec);
}

void Executor::removeShuffle(int32_t shuffleId) {
  blockStore_->removeShuffle(shuffleId);
}

std::string Executor::runTask(const std::string& bytes) {
  TaskResult result;
  uint64_t wallMicros = 0;
  try {
    velox::MicrosecondTimer timer(&wallMicros);
    auto description = TaskDescription::deserialize(bytes);
    result.taskId = description.taskId();

    auto plan = velox::ISerializable::deserialize<core::PlanNode>(
        description.fragment, planPool_.get());

    auto config = std::unordered_map<std::string, std::string>(
        description.config.begin(), description.config.end());
    config[std::string(shuffle::kExecutorLocation)] = options_.location;
    auto queryCtx = core::QueryCtx::create(
        driverExecutor_.get(),
        core::QueryConfig(std::move(config)),
        /*connectorConfigs=*/{},
        velox::cache::AsyncDataCache::getInstance(),
        /*pool=*/nullptr,
        /*spillExecutor=*/nullptr,
        result.taskId);

    std::mutex resultMutex;
    exec::ConsumerSupplier consumer;
    if (description.resultTask) {
      consumer = [&]() -> exec::Consumer {
        return [&](velox::RowVectorPtr rows,
                   bool /*drained*/,
                   velox::ContinueFuture* /*future*/) {
          if (rows != nullptr && rows->size() > 0) {
            auto page = serializeRows(rows, resultPool_.get());
            std::lock_guard<std::mutex> l(resultMutex);
            result.outputRows += rows->size();
            result.resultPages.push_back(std::move(page));
          }
          return exec::BlockingReason::kNotBlocked;
        };
      };
    }

    auto task = exec::Task::create(
        result.taskId,
        core::PlanFragment{plan},
        description.partition,
        std::move(queryCtx),
        exec::Task::ExecutionMode::kParallel,
        std::move(consumer));
    task->start(/*maxDrivers=*/1);

    std::vector<core::PlanNodeId> splitNodes;
    collectSplitNodes(plan, splitNodes);
    for (const auto& nodeId : splitNodes) {
      auto it = description.splits.find(nodeId);
      if (it != description.splits.end()) {
        for (const auto& split : it->second) {
          task->addSplit(
              nodeId,
              exec::Split(
                  std::const_pointer_cast<velox::connector::ConnectorSplit>(
                      velox::ISerializable::deserialize<
                          velox::connector::ConnectorSplit>(split))));
        }
      }
      task->noMoreSplits(nodeId);
    }

    task->taskCompletionFuture().wait();
    if (task->state() != exec::TaskState::kFinished) {
      result.state = TaskState::kFailed;
      result.error = task->errorMessage();
      if (auto error = task->error()) {
        try {
          std::rethrow_exception(error);
        } catch (const velox::VeloxUserError&) {
          result.userError = true;
        } catch (...) {
        }
      }
      if (result.error.empty()) {
        result.error = fmt::format(
            "Task ended as {}", exec::taskStateString(task->state()));
      }
      result.fetchFailure = shuffle::FetchFailure::find(result.error);
      if (result.fetchFailure.has_value()) {
        result.userError = false;
      }
    } else if (
        auto write =
            std::dynamic_pointer_cast<const shuffle::ShuffleWriteNode>(plan)) {
      result.mapStatus =
          blockStore_->mapStatus(write->shuffleId(), description.partition);
    }
  } catch (const velox::VeloxUserError& e) {
    result.state = TaskState::kFailed;
    result.error = e.what();
    result.userError = true;
  } catch (const std::exception& e) {
    result.state = TaskState::kFailed;
    result.error = e.what();
  }
  result.wallMicros = wallMicros;
  return result.serialize();
}

} // namespace ember
