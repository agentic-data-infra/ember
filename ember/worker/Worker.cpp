#include "ember/worker/Worker.h"

#include <unistd.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <glog/logging.h>

#include "ember/rpc/Messages.h"
#include "ember/worker/Executor.h"
#include "ember/worker/NetworkBlockTransport.h"
#include "velox/common/base/Exceptions.h"

namespace ember {

namespace {
constexpr auto kRetryInterval = std::chrono::seconds(1);
constexpr auto kCallTimeout = std::chrono::seconds(30);
} // namespace

Worker::Worker(WorkerOptions options)
    : options_{std::move(options)},
      numThreads_{
          options_.numThreads > 0 ? options_.numThreads
                                  : std::max(4, 2 * options_.slots)} {
  VELOX_CHECK_GT(options_.slots, 0);
  VELOX_CHECK(!options_.localDirectory.empty());
}

Worker::~Worker() {
  stop();
  server_.stop();
  stopExecutor();
  if (blockReaders_ != nullptr) {
    blockReaders_->join();
  }
  if (shuffle::BlockTransport::instance() == transport_) {
    shuffle::BlockTransport::setInstance(
        std::make_shared<shuffle::LocalBlockTransport>());
  }
}

std::string Worker::start() {
  transport_ = std::make_shared<NetworkBlockTransport>(
      /*fetchLocalBlocks=*/!options_.fetchLocalBlocksOverNetwork);
  shuffle::BlockTransport::setInstance(transport_);
  blockReaders_ = std::make_unique<folly::CPUThreadPoolExecutor>(4);

  server_.setHandler(rpc::Method::kLaunchTask, [this](std::string task) {
    return launchTask(std::move(task));
  });
  server_.setHandler(rpc::Method::kRemoveShuffle, [this](std::string payload) {
    auto request = rpc::RemoveShuffle::deserialize(payload);
    std::shared_ptr<Executor> executor;
    {
      std::lock_guard<std::mutex> l(mutex_);
      executor = executor_;
    }
    if (executor != nullptr) {
      executor->removeShuffle(request.shuffleId);
    }
    return rpc::respond("");
  });
  server_.setHandler(rpc::Method::kShutdown, [this](std::string /*payload*/) {
    {
      std::lock_guard<std::mutex> l(mutex_);
      driverDone_ = true;
    }
    wakeup_.notify_all();
    return rpc::respond("");
  });
  serveBlocks(
      server_,
      [this] { return shuffle::ShuffleBlockStore::find(address_); },
      blockReaders_.get());

  server_.start(options_.bindHost, options_.port);
  const auto host = options_.advertisedHost.empty() ? rpc::localHostName()
                                                    : options_.advertisedHost;
  address_ = rpc::HostAndPort{host, server_.port()}.toString();
  LOG(INFO) << "Worker serving at " << address_;
  return address_;
}

void Worker::stop() {
  {
    std::lock_guard<std::mutex> l(mutex_);
    stopping_ = true;
  }
  wakeup_.notify_all();
}

void Worker::run() {
  VELOX_CHECK(!address_.empty(), "Call start() first");
  auto connectTimeout = options_.connectTimeout;
  while (true) {
    const bool served = serveDriver(connectTimeout);
    {
      std::lock_guard<std::mutex> l(mutex_);
      if (stopping_ || !options_.persistent || !served) {
        return;
      }
    }
    // A persistent worker waits for the next driver as long as it takes.
    connectTimeout = std::chrono::milliseconds(0);
  }
}

bool Worker::serveDriver(std::chrono::milliseconds connectTimeout) {
  const auto deadline = std::chrono::steady_clock::now() + connectTimeout;
  std::shared_ptr<rpc::RpcClient> driver;
  while (driver == nullptr) {
    try {
      driver = rpc::RpcClient::connect(options_.driver, std::chrono::seconds(5));
    } catch (const std::exception& e) {
      if (connectTimeout.count() > 0 &&
          std::chrono::steady_clock::now() >= deadline) {
        LOG(ERROR) << "Giving up on driver " << options_.driver.toString()
                   << ": " << e.what();
        return false;
      }
      VLOG(1) << "Driver not reachable yet: " << e.what();
      std::unique_lock<std::mutex> l(mutex_);
      if (wakeup_.wait_for(l, kRetryInterval, [&] { return stopping_; })) {
        return false;
      }
    }
  }

  startExecutor();
  rpc::RegisterWorkerReply reply;
  try {
    reply = rpc::RegisterWorkerReply::deserialize(
        driver
            ->call(
                rpc::Method::kRegisterWorker,
                rpc::RegisterWorker{address_, address_, options_.slots}
                    .serialize())
            .get(kCallTimeout));
    LOG(INFO) << "Registered with driver " << options_.driver.toString()
              << " of " << reply.appName << ", " << options_.slots
              << " slots";
  } catch (const std::exception& e) {
    LOG(ERROR) << "Registration with " << options_.driver.toString()
               << " failed: " << e.what();
    driver->close();
    stopExecutor();
    return true;
  }

  const auto interval =
      std::chrono::milliseconds(std::max<int64_t>(reply.heartbeatIntervalMs, 10));
  while (true) {
    {
      std::unique_lock<std::mutex> l(mutex_);
      if (wakeup_.wait_for(
              l, interval, [&] { return stopping_ || driverDone_; })) {
        LOG(INFO) << "Driver " << options_.driver.toString() << " is done";
        break;
      }
    }
    try {
      auto heartbeat = rpc::HeartbeatReply::deserialize(
          driver
              ->call(
                  rpc::Method::kHeartbeat, rpc::Heartbeat{address_}.serialize())
              .get(kCallTimeout));
      if (!heartbeat.registered) {
        LOG(WARNING) << "Driver no longer knows this worker";
        break;
      }
    } catch (const std::exception& e) {
      LOG(WARNING) << "Lost driver " << options_.driver.toString() << ": "
                   << e.what();
      break;
    }
  }
  driver->close();
  stopExecutor();
  return true;
}

void Worker::startExecutor() {
  auto directory = fmt::format(
      "{}/worker-{}-{}-{}",
      options_.localDirectory,
      server_.port(),
      ::getpid(),
      ++registrations_);
  auto executor = std::make_shared<Executor>(ExecutorOptions{
      .location = address_,
      .shuffleDirectory = std::move(directory),
      .numThreads = numThreads_});
  auto slots = std::make_shared<folly::CPUThreadPoolExecutor>(options_.slots);
  std::lock_guard<std::mutex> l(mutex_);
  executor_ = std::move(executor);
  slots_ = std::move(slots);
  driverDone_ = false;
}

void Worker::stopExecutor() {
  std::shared_ptr<Executor> executor;
  std::shared_ptr<folly::CPUThreadPoolExecutor> slots;
  {
    std::lock_guard<std::mutex> l(mutex_);
    executor.swap(executor_);
    slots.swap(slots_);
  }
  if (slots != nullptr) {
    // Running tasks finish; their results go nowhere if the driver is gone.
    slots->join();
  }
}

folly::SemiFuture<std::unique_ptr<folly::IOBuf>> Worker::launchTask(
    std::string task) {
  std::shared_ptr<Executor> executor;
  std::shared_ptr<folly::CPUThreadPoolExecutor> slots;
  {
    std::lock_guard<std::mutex> l(mutex_);
    executor = executor_;
    slots = slots_;
  }
  VELOX_CHECK_NOT_NULL(executor, "Worker {} has no driver", address_);
  return folly::via(
             slots.get(),
             [executor, task = std::move(task)] {
               return folly::IOBuf::fromString(executor->runTask(task));
             })
      .semi();
}

} // namespace ember
