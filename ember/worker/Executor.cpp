#include "ember/worker/Executor.h"

#include <filesystem>

#include <folly/executors/CPUThreadPoolExecutor.h>

#include "axiom/connectors/file/FileHandler.h"
#include "ember/CachedConnector.h"
#include "ember/OutputConnector.h"
#include "ember/RowPages.h"
#include "ember/Runtime.h"
#include "ember/shuffle/KilnShuffle.h"
#include "ember/shuffle/ShuffleWrite.h"
#include "ember/worker/TaskDescription.h"
#include "velox/common/config/Config.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/time/Timer.h"
#include "velox/core/QueryCtx.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Task.h"

namespace ember {

namespace core = facebook::velox::core;
namespace exec = facebook::velox::exec;
namespace velox = facebook::velox;

namespace {

// The shuffle block 'split' reads, if any.
std::optional<std::string> splitBlockUrl(
    const std::shared_ptr<velox::connector::ConnectorSplit>& split) {
  if (auto remote =
          std::dynamic_pointer_cast<exec::RemoteConnectorSplit>(split)) {
    return remote->taskId;
  }
  if (auto cached = std::dynamic_pointer_cast<CachedBlockSplit>(split)) {
    return cached->url;
  }
  return std::nullopt;
}

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
  transport_ = std::make_shared<shuffle::LocalBlockTransport>(
      blockStore_, options_.remoteTransport);
  driverExecutor_ =
      std::make_unique<folly::CPUThreadPoolExecutor>(options_.numThreads);
  spillExecutor_ =
      std::make_unique<folly::CPUThreadPoolExecutor>(options_.numThreads);
  ioExecutor_ =
      std::make_shared<folly::CPUThreadPoolExecutor>(options_.numThreads);
  rootPool_ = velox::memory::memoryManager()->addRootPool(
      fmt::format("ember_executor_{}", options_.location));
  resultPool_ = rootPool_->addLeafChild("results");
  planPool_ = rootPool_->addLeafChild("plans");
  runtime_.emplace(blockStore_, transport_, ioExecutor_);
}

Executor::~Executor() {
  stop();
  driverExecutor_->join();
  spillExecutor_->join();
  ioExecutor_->join();
  std::error_code ec;
  std::filesystem::remove_all(options_.shuffleDirectory, ec);
}

void Executor::removeShuffle(int32_t shuffleId) {
  blockStore_->removeShuffle(shuffleId);
}

void Executor::stop() {
  runtime_.reset();
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
    // A merge buffers its slot's share of the Kiln read pool, so that the
    // reads of all running merges fit and none of them are copied.
    config.try_emplace(
        std::string(core::QueryConfig::kMaxMergeExchangeBufferSize),
        std::to_string(shuffle::kilnReadPoolBytes() / options_.slots));
    // The output's writer sizes what it buffers by the slot's share of the
    // memory arbitrator.
    const auto taskMemoryBytes =
        velox::memory::memoryManager()->arbitrator()->capacity() /
        options_.slots;
    std::unordered_map<std::string, std::shared_ptr<velox::config::ConfigBase>>
        connectorConfigs{
            {std::string(kOutputConnectorId),
             std::make_shared<velox::config::ConfigBase>(
                 std::unordered_map<std::string, std::string>{
                     {std::string(kTaskMemoryBytes),
                      std::to_string(taskMemoryBytes)}})}};
    auto queryCtx = core::QueryCtx::create(
        driverExecutor_.get(),
        core::QueryConfig(std::move(config)),
        std::move(connectorConfigs),
        velox::cache::AsyncDataCache::getInstance(),
        /*pool=*/nullptr,
        spillExecutor_.get(),
        result.taskId);
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
    std::vector<std::string> blocks;
    for (const auto& nodeId : splitNodes) {
      auto it = description.splits.find(nodeId);
      if (it != description.splits.end()) {
        for (const auto& split : it->second) {
          auto connectorSplit =
              std::const_pointer_cast<velox::connector::ConnectorSplit>(
                  velox::ISerializable::deserialize<
                      velox::connector::ConnectorSplit>(split));
          if (auto url = splitBlockUrl(connectorSplit)) {
            blocks.push_back(std::move(*url));
          }
          task->addSplit(nodeId, exec::Split(std::move(connectorSplit)));
        }
      }
      task->noMoreSplits(nodeId);
    }

    task->taskCompletionFuture().wait();
    auto fetchFailure = shuffle::ShuffleRuntime::current()->takeFailure(blocks);
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
      result.fetchFailure = std::move(fetchFailure);
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
