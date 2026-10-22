#include "ember/driver/ClusterBackend.h"

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/Future.h>
#include <glog/logging.h>

#include "ember/rpc/Messages.h"
#include "ember/worker/TaskDescription.h"
#include "velox/common/base/Exceptions.h"

namespace ember::driver {

namespace {
constexpr auto kShutdownTimeout = std::chrono::seconds(5);
constexpr auto kRemoveShuffleTimeout = std::chrono::seconds(30);
constexpr auto kMonitorInterval = std::chrono::milliseconds(500);
} // namespace

ClusterBackend::ClusterBackend(ClusterBackendOptions options)
    : options_{std::move(options)},
      callbacks_{std::make_unique<folly::CPUThreadPoolExecutor>(4)},
      noWorkersSince_{std::chrono::steady_clock::now()} {
  server_.setHandler(rpc::Method::kRegisterWorker, [this](std::string payload) {
    return rpc::respond(registerWorker(payload));
  });
  server_.setHandler(rpc::Method::kHeartbeat, [this](std::string payload) {
    return rpc::respond(heartbeat(payload));
  });
  server_.start(options_.bindHost, options_.port);
  monitor_ = std::thread([this] { monitor(); });
  LOG(INFO) << "Driver of " << options_.appName << " listening on port "
            << server_.port();
}

ClusterBackend::~ClusterBackend() {
  stop();
}

std::string ClusterBackend::registerWorker(const std::string& payload) {
  auto request = rpc::RegisterWorker::deserialize(payload);
  VELOX_CHECK_GT(request.slots, 0, "Worker {} offers no slots", request.address);
  // Tasks go to the worker over a connection of their own.
  auto client = rpc::RpcClient::connect(rpc::HostAndPort::parse(request.address));
  auto worker = std::make_shared<WorkerInfo>();
  worker->id = request.workerId;
  worker->address = request.address;
  worker->slots = request.slots;
  worker->client = std::move(client);
  worker->lastHeartbeat = std::chrono::steady_clock::now();
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (stopped_) {
      worker->client->close();
      VELOX_FAIL("Driver of {} is stopping", options_.appName);
    }
    if (auto it = workers_.find(worker->id); it != workers_.end()) {
      removeWorkerLocked(it->second, "registered again");
    }
    workers_[worker->id] = worker;
    if (listener_.onExecutorAdded) {
      listener_.onExecutorAdded(worker->address);
    }
  }
  LOG(INFO) << "Worker " << worker->address << " registered with "
            << worker->slots << " slots";
  changed_.notify_all();
  dispatch();
  return rpc::RegisterWorkerReply{
      options_.appName, options_.heartbeatInterval.count()}
      .serialize();
}

std::string ClusterBackend::heartbeat(const std::string& payload) {
  auto request = rpc::Heartbeat::deserialize(payload);
  std::lock_guard<std::mutex> l(mutex_);
  auto it = workers_.find(request.workerId);
  if (it == workers_.end()) {
    return rpc::HeartbeatReply{false}.serialize();
  }
  it->second->lastHeartbeat = std::chrono::steady_clock::now();
  return rpc::HeartbeatReply{true}.serialize();
}

void ClusterBackend::waitForWorkers(
    int32_t count,
    std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> l(mutex_);
  const bool ready = changed_.wait_for(l, timeout, [&] {
    return stopped_ || static_cast<int32_t>(workers_.size()) >= count;
  });
  VELOX_CHECK(!stopped_, "Driver of {} is stopped", options_.appName);
  VELOX_USER_CHECK(
      ready,
      "{} of {} workers registered at port {} within {} ms",
      workers_.size(),
      count,
      server_.port(),
      timeout.count());
}

std::vector<std::string> ClusterBackend::workers() const {
  std::lock_guard<std::mutex> l(mutex_);
  std::vector<std::string> addresses;
  for (const auto& [id, worker] : workers_) {
    addresses.push_back(worker->address);
  }
  return addresses;
}

int32_t ClusterBackend::totalSlots() const {
  std::lock_guard<std::mutex> l(mutex_);
  int32_t slots = 0;
  for (const auto& [id, worker] : workers_) {
    slots += worker->slots;
  }
  return slots;
}

void ClusterBackend::launchTask(std::string task, ResultCallback onResult) {
  PendingTask pending{std::move(task), std::move(onResult)};
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (!stopped_) {
      pending_.push_back(std::move(pending));
      pending.onResult = nullptr;
    }
  }
  if (pending.onResult != nullptr) {
    fail(std::move(pending), "Backend is stopped");
    return;
  }
  dispatch();
}

void ClusterBackend::dispatch() {
  std::vector<std::pair<std::shared_ptr<WorkerInfo>, PendingTask>> launches;
  {
    std::lock_guard<std::mutex> l(mutex_);
    while (!pending_.empty()) {
      // The worker with the most free slots, to spread tasks out.
      std::shared_ptr<WorkerInfo> best;
      for (const auto& [id, worker] : workers_) {
        const auto free = worker->slots - worker->running;
        if (free > 0 && (best == nullptr || free > best->slots - best->running)) {
          best = worker;
        }
      }
      if (best == nullptr) {
        break;
      }
      ++best->running;
      launches.emplace_back(best, std::move(pending_.front()));
      pending_.pop_front();
    }
  }
  for (auto& [worker, task] : launches) {
    auto bytes = task.task;
    worker->client->call(rpc::Method::kLaunchTask, std::move(bytes))
        .via(callbacks_.get())
        .thenTry([this, worker, task = std::move(task)](
                     folly::Try<std::string> result) mutable {
          onTaskDone(worker, std::move(task), std::move(result));
        });
  }
}

void ClusterBackend::onTaskDone(
    const std::shared_ptr<WorkerInfo>& worker,
    PendingTask task,
    folly::Try<std::string> result) {
  std::string error;
  {
    std::lock_guard<std::mutex> l(mutex_);
    --worker->running;
    if (result.hasException()) {
      error = fmt::format(
          "Lost worker {}: {}",
          worker->address,
          result.exception().what().toStdString());
      auto it = workers_.find(worker->id);
      if (it != workers_.end() && it->second == worker) {
        removeWorkerLocked(worker, error);
      }
    }
  }
  if (result.hasException()) {
    fail(std::move(task), error);
  } else {
    task.onResult(std::move(*result));
  }
  dispatch();
}

void ClusterBackend::removeWorkerLocked(
    const std::shared_ptr<WorkerInfo>& worker,
    const std::string& reason) {
  LOG(WARNING) << "Removing worker " << worker->address << ": " << reason;
  workers_.erase(worker->id);
  // Fails the worker's running tasks.
  worker->client->close();
  if (listener_.onExecutorLost) {
    listener_.onExecutorLost(worker->address);
  }
  if (workers_.empty()) {
    noWorkersSince_ = std::chrono::steady_clock::now();
  }
}

void ClusterBackend::fail(PendingTask task, const std::string& error) {
  TaskResult result;
  try {
    result.taskId = TaskDescription::deserialize(task.task).taskId();
  } catch (const std::exception&) {
  }
  result.state = TaskState::kFailed;
  result.error = error;
  task.onResult(result.serialize());
}

void ClusterBackend::monitor() {
  std::unique_lock<std::mutex> l(mutex_);
  while (!changed_.wait_for(l, kMonitorInterval, [&] { return stopped_; })) {
    const auto now = std::chrono::steady_clock::now();
    // A worker that died between tasks shows as a closed connection.
    std::vector<std::pair<std::shared_ptr<WorkerInfo>, std::string>> lost;
    for (const auto& [id, worker] : workers_) {
      if (!worker->client->isOpen()) {
        lost.emplace_back(worker, "connection closed");
      } else if (now - worker->lastHeartbeat > options_.heartbeatTimeout) {
        lost.emplace_back(worker, "no heartbeat");
      }
    }
    for (const auto& [worker, reason] : lost) {
      removeWorkerLocked(worker, reason);
    }
    if (workers_.empty() && !pending_.empty() &&
        now - noWorkersSince_ > options_.noWorkersTimeout) {
      std::deque<PendingTask> pending;
      pending.swap(pending_);
      l.unlock();
      for (auto& task : pending) {
        fail(
            std::move(task),
            fmt::format(
                "No workers registered for {} ms",
                options_.noWorkersTimeout.count()));
      }
      l.lock();
    }
  }
}

void ClusterBackend::setExecutorListener(ExecutorListener listener) {
  std::lock_guard<std::mutex> l(mutex_);
  listener_ = std::move(listener);
}

void ClusterBackend::removeShuffle(int32_t shuffleId) {
  std::vector<std::shared_ptr<rpc::RpcClient>> clients;
  {
    std::lock_guard<std::mutex> l(mutex_);
    for (const auto& [id, worker] : workers_) {
      clients.push_back(worker->client);
    }
  }
  std::vector<folly::SemiFuture<std::string>> calls;
  const auto request = rpc::RemoveShuffle{shuffleId}.serialize();
  for (const auto& client : clients) {
    calls.push_back(client->call(rpc::Method::kRemoveShuffle, request));
  }
  for (auto& call : calls) {
    auto result = std::move(call).getTry(kRemoveShuffleTimeout);
    if (result.hasException()) {
      LOG(WARNING) << "Removing shuffle " << shuffleId
                   << " failed: " << result.exception().what();
    }
  }
}

void ClusterBackend::stop() {
  std::map<std::string, std::shared_ptr<WorkerInfo>> workers;
  std::deque<PendingTask> pending;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (stopped_) {
      return;
    }
    stopped_ = true;
    workers.swap(workers_);
    pending.swap(pending_);
  }
  changed_.notify_all();
  monitor_.join();

  std::vector<folly::SemiFuture<std::string>> calls;
  for (const auto& [id, worker] : workers) {
    calls.push_back(worker->client->call(rpc::Method::kShutdown, ""));
  }
  for (auto& call : calls) {
    std::move(call).getTry(kShutdownTimeout);
  }
  for (const auto& [id, worker] : workers) {
    worker->client->close();
  }
  for (auto& task : pending) {
    fail(std::move(task), "Backend is stopped");
  }
  callbacks_->join();
  server_.stop();
}

} // namespace ember::driver
