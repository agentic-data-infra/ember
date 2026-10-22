// The RPC layer on its own: concurrent calls on one connection, handler
// errors, and calls failing when a connection breaks.

#include <gtest/gtest.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/SharedPromise.h>

#include "ember/rpc/Clients.h"
#include "ember/rpc/RpcServer.h"

namespace ember::rpc {
namespace {

// launchTask echoes its task after the number of ms it starts with, if any;
// "throw" throws and "fail" fails its future. removeShuffle does not return
// until the test lets it. fetchBlock returns a block of 'reduceId' bytes.
class TestHandler : public WorkerHandler {
 public:
  TestHandler() : pool_{std::make_unique<folly::CPUThreadPoolExecutor>(8)} {}

  ~TestHandler() override {
    release();
    pool_->join();
  }

  void release() {
    if (!hang_.isFulfilled()) {
      hang_.setValue();
    }
  }

  folly::SemiFuture<std::string> launchTask(std::string task) override {
    if (task == "throw") {
      throw std::runtime_error("handler threw");
    }
    if (task == "fail") {
      return folly::makeSemiFuture<std::string>(
          std::runtime_error("future failed"));
    }
    return folly::via(pool_.get(), [task = std::move(task)]() mutable {
             auto delay = folly::tryTo<int32_t>(task.substr(0, task.find(' ')));
             if (delay.hasValue()) {
               std::this_thread::sleep_for(std::chrono::milliseconds(*delay));
             }
             return std::move(task);
           })
        .semi();
  }

  void removeShuffle(int32_t /*shuffleId*/) override {
    hang_.getSemiFuture().wait();
  }

  void shutdown() override {}

  folly::SemiFuture<std::unique_ptr<folly::IOBuf>> fetchBlock(
      const BlockId& block) override {
    std::string bytes(block.reduceId, '\0');
    for (size_t i = 0; i < bytes.size(); ++i) {
      bytes[i] = static_cast<char>(i * 7);
    }
    return folly::makeSemiFuture(folly::IOBuf::fromString(std::move(bytes)));
  }

 private:
  std::unique_ptr<folly::CPUThreadPoolExecutor> pool_;
  folly::SharedPromise<folly::Unit> hang_;
};

class RpcTest : public testing::Test {
 protected:
  void SetUp() override {
    handler_ = std::make_shared<TestHandler>();
    server_.start(handler_, "", 0);
  }

  void TearDown() override {
    handler_->release();
    server_.stop();
  }

  HostAndPort address() const {
    return HostAndPort{"localhost", server_.port()};
  }

  static folly::SemiFuture<std::string> echo(
      WorkerClient& client,
      const std::string& task) {
    return client.launchTask(task);
  }

  std::shared_ptr<TestHandler> handler_;
  RpcServer server_;
};

TEST_F(RpcTest, hostAndPort) {
  EXPECT_EQ(HostAndPort::parse("node-1:7077"), (HostAndPort{"node-1", 7077}));
  EXPECT_EQ(HostAndPort::parse("[::1]:80"), (HostAndPort{"::1", 80}));
  EXPECT_EQ((HostAndPort{"::1", 80}).toString(), "[::1]:80");
  EXPECT_EQ((HostAndPort{"node-1", 7077}).toString(), "node-1:7077");
  EXPECT_THROW(HostAndPort::parse("node-1"), RpcError);
  EXPECT_THROW(HostAndPort::parse("node-1:"), RpcError);
  EXPECT_THROW(HostAndPort::parse("node-1:99999"), RpcError);
}

TEST_F(RpcTest, roundTrip) {
  auto client = WorkerClient::connect(address());
  EXPECT_EQ(echo(*client, "hello").get(), "hello");
  EXPECT_EQ(echo(*client, "").get(), "");

  // A payload larger than socket buffers, with every byte value.
  std::string big(16 << 20, '\0');
  for (size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<char>(i * 7);
  }
  EXPECT_EQ(echo(*client, big).get(), big);

  auto blocks = ShuffleBlockClient::connect(address());
  auto bytes = blocks->fetchBlock(BlockId{.reduceId = 16 << 20}).get();
  EXPECT_EQ(bytes->computeChainDataLength(), big.size());
  EXPECT_EQ(bytes->to<std::string>(), big);
}

TEST_F(RpcTest, concurrentCallsCompleteOutOfOrder) {
  auto client = WorkerClient::connect(address());
  // The first call answers last.
  auto slow = echo(*client, "300 slow");
  std::vector<folly::SemiFuture<std::string>> fast;
  for (int i = 0; i < 100; ++i) {
    fast.push_back(echo(*client, fmt::format("0 {}", i)));
  }
  for (int i = 0; i < 100; ++i) {
    EXPECT_EQ(std::move(fast[i]).get(), fmt::format("0 {}", i));
  }
  EXPECT_FALSE(slow.isReady());
  EXPECT_EQ(std::move(slow).get(), "300 slow");
}

TEST_F(RpcTest, manyClients) {
  std::vector<std::thread> threads;
  std::atomic<int> ok{0};
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&, t] {
      auto client = WorkerClient::connect(address());
      for (int i = 0; i < 50; ++i) {
        auto payload = fmt::format("x{}-{}", t, i);
        if (echo(*client, payload).get() == payload) {
          ++ok;
        }
      }
    });
  }
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(ok, 8 * 50);
}

TEST_F(RpcTest, errors) {
  auto client = WorkerClient::connect(address());
  EXPECT_THROW(
      {
        try {
          echo(*client, "throw").get();
        } catch (const RpcError& e) {
          EXPECT_NE(
              std::string(e.what()).find("handler threw"), std::string::npos);
          throw;
        }
      },
      RpcError);
  EXPECT_THROW(echo(*client, "fail").get(), RpcError);
  // Errors leave the connection usable.
  EXPECT_EQ(echo(*client, "still").get(), "still");
}

TEST_F(RpcTest, pendingCallsFailWhenConnectionBreaks) {
  auto client = WorkerClient::connect(address());
  auto pending = client->removeShuffle(1);
  server_.stop();
  EXPECT_THROW(std::move(pending).get(std::chrono::seconds(10)), RpcError);
  EXPECT_FALSE(client->isOpen());
  EXPECT_THROW(echo(*client, "x").get(), RpcError);
}

TEST_F(RpcTest, closeFailsPendingCalls) {
  auto client = WorkerClient::connect(address());
  auto pending = client->removeShuffle(1);
  client->close();
  EXPECT_THROW(std::move(pending).get(std::chrono::seconds(10)), RpcError);
  EXPECT_FALSE(client->isOpen());
}

TEST_F(RpcTest, connectFails) {
  RpcServer other;
  other.start(std::make_shared<TestHandler>(), "", 0);
  const auto port = other.port();
  other.stop();
  EXPECT_THROW(
      WorkerClient::connect(
          HostAndPort{"localhost", port}, std::chrono::milliseconds(500)),
      RpcError);
}

} // namespace
} // namespace ember::rpc
