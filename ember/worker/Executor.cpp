#include "ember/worker/Executor.h"

#include <filesystem>

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "axiom/connectors/file/FileHandler.h"
#include "ember/RowPages.h"
#include "ember/Runtime.h"
#include "ember/shuffle/ShuffleContext.h"
#include "ember/shuffle/ShuffleWrite.h"
#include "ember/worker/TaskDescription.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/PlanNodeStats.h"
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
  transport_ = options_.fetchOwnBlocksRemotely
      ? options_.remoteTransport
      : std::make_shared<shuffle::LocalBlockTransport>(
            blockStore_, options_.remoteTransport);
  driverExecutor_ =
      std::make_unique<folly::CPUThreadPoolExecutor>(options_.numThreads);
  spillExecutor_ =
      std::make_unique<folly::CPUThreadPoolExecutor>(options_.numThreads);
  rootPool_ = velox::memory::memoryManager()->addRootPool(
      fmt::format("ember_executor_{}", options_.location));
  resultPool_ = rootPool_->addLeafChild("results");
  planPool_ = rootPool_->addLeafChild("plans");
}

Executor::~Executor() {
  driverExecutor_->join();
  spillExecutor_->join();
  std::error_code ec;
  std::filesystem::remove_all(options_.shuffleDirectory, ec);
}

void Executor::removeShuffle(int32_t shuffleId) {
  blockStore_->removeShuffle(shuffleId);
}

std::vector<rpc::FileInfo> Executor::listFiles(
    const std::string& schema,
    const std::string& path) {
  initializeRuntime();
  std::vector<rpc::FileInfo> files;
  for (auto& file :
       facebook::axiom::connector::file::handler(schema).listFiles(path)) {
    const auto size = velox::filesystems::getFileSystem(file, nullptr)
                          ->openFileForRead(file)
                          ->size();
    files.push_back(rpc::FileInfo{.path = std::move(file), .size = size});
  }
  return files;
}

namespace {

// Default bytes a merge exchange buffers across its sources.
constexpr int64_t kMergeExchangeBufferBytes = 32 << 20;

} // namespace

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
    // Velox's 128 MB per merge would let the merges of a few reduce tasks
    // hold more Kiln read buffers than the pool has, and the rest of their
    // reads would be copied.
    config.try_emplace(
        std::string(core::QueryConfig::kMaxMergeExchangeBufferSize),
        std::to_string(kMergeExchangeBufferBytes));
    // With a memory limit, the task's pool cannot grow past it: the
    // arbitrator makes its operators spill, if spilling is enabled, or fails
    // the task.
    std::shared_ptr<velox::memory::MemoryPool> taskPool;
    if (auto it = config.find(core::QueryConfig::kQueryMaxMemoryPerNode);
        it != config.end()) {
      const auto capacity = velox::config::toCapacity(
          it->second, velox::config::CapacityUnit::BYTE);
      if (capacity > 0) {
        taskPool = velox::memory::memoryManager()->addRootPool(
            core::QueryCtx::generatePoolName(result.taskId), capacity);
      }
    }
    auto queryCtx = core::QueryCtx::create(
        driverExecutor_.get(),
        core::QueryConfig(std::move(config)),
        /*connectorConfigs=*/{},
        velox::cache::AsyncDataCache::getInstance(),
        std::move(taskPool),
        spillExecutor_.get(),
        result.taskId);
    const shuffle::ShuffleContextScope shuffle(
        *queryCtx->pool(), blockStore_, transport_);
    // Created on the first spill, removed with the task.
    const auto spillDirectory = fmt::format(
        "{}/spill/{}_{}.{}_{}.{}",
        options_.shuffleDirectory,
        description.jobId,
        description.stageId,
        description.stageAttempt,
        description.partition,
        description.attempt);
    velox::common::SpillDiskOptions spillOptions{
        .spillDirPath = spillDirectory,
        .spillDirCreated = false,
        .spillDirCreateCb = [spillDirectory]() {
          std::filesystem::create_directories(spillDirectory);
          return spillDirectory;
        }};

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
        std::move(consumer),
        /*memoryArbitrationPriority=*/0,
        std::move(spillOptions));
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
      result.fetchFailure = shuffle.context().fetchFailures.failure();
      if (result.fetchFailure.has_value()) {
        result.userError = false;
      }
    } else {
      if (auto write =
              std::dynamic_pointer_cast<const shuffle::ShuffleWriteNode>(
                  plan)) {
        result.mapStatus =
            blockStore_->mapStatus(write->shuffleId(), description.partition);
      }
      int64_t spilledBytes = 0;
      for (const auto& [id, stats] : exec::toPlanStats(task->taskStats())) {
        spilledBytes += stats.spilledBytes;
      }
      if (spilledBytes > 0) {
        LOG(INFO) << "Task " << result.taskId << " spilled " << spilledBytes
                  << " bytes";
      }
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
