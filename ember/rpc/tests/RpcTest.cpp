// The RPC layer on its own: framing, concurrent calls on one connection,
// handler errors, and calls failing when a connection breaks.

#include <gtest/gtest.h>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/futures/SharedPromise.h>

#include "ember/rpc/Messages.h"
#include "ember/rpc/RpcClient.h"
#include "ember/rpc/RpcServer.h"

namespace ember::rpc {
namespace {

class RpcTest : public testing::Test {
 protected:
  void SetUp() override {
    pool_ = std::make_unique<folly::CPUThreadPoolExecutor>(8);
    // Echoes the payload after the number of ms it starts with, if any.
    server_.setHandler(Method::kFetchBlock, [this](std::string payload) {
      return folly::via(
                 pool_.get(),
                 [payload = std::move(payload)] {
                   auto delay = folly::tryTo<int32_t>(
                       payload.substr(0, payload.find(' ')));
                   if (delay.hasValue()) {
                     std::this_thread::sleep_for(
                         std::chrono::milliseconds(*delay));
                   }
                   return folly::IOBuf::copyBuffer(payload);
                 })
          .semi();
    });
    server_.setHandler(Method::kRemoveShuffle, [](std::string payload) {
      if (payload == "throw") {
        throw std::runtime_error("handler threw");
      }
      return folly::makeSemiFuture<std::unique_ptr<folly::IOBuf>>(
          folly::exception_wrapper(std::runtime_error("future failed")));
    });
    server_.setHandler(Method::kHeartbeat, [this](std::string /*payload*/) {
      // Never answers until the test lets it.
      return hang_.getSemiFuture().deferValue(
          [](folly::Unit) { return folly::IOBuf::copyBuffer("late"); });
    });
    server_.start("", 0);
  }

  void TearDown() override {
    hang_.setValue();
    server_.stop();
    pool_->join();
  }

  HostAndPort address() const {
    return HostAndPort{"localhost", server_.port()};
  }

  std::unique_ptr<folly::CPUThreadPoolExecutor> pool_;
  folly::SharedPromise<folly::Unit> hang_;
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

TEST_F(RpcTest, messages) {
  auto worker = RegisterWorker::deserialize(
      RegisterWorker{"w", "h:1", 3}.serialize());
  EXPECT_EQ(worker.workerId, "w");
  EXPECT_EQ(worker.address, "h:1");
  EXPECT_EQ(worker.slots, 3);
  auto block = FetchBlock::deserialize(FetchBlock{1, 2, 3}.serialize());
  EXPECT_EQ(block.shuffleId, 1);
  EXPECT_EQ(block.mapId, 2);
  EXPECT_EQ(block.reduceId, 3);
  EXPECT_FALSE(
      HeartbeatReply::deserialize(HeartbeatReply{false}.serialize()).registered);
  EXPECT_THROW(RegisterWorker::deserialize("not json"), RpcError);
}

TEST_F(RpcTest, roundTrip) {
  auto client = RpcClient::connect(address());
  EXPECT_EQ(client->call(Method::kFetchBlock, "hello").get(), "hello");
  EXPECT_EQ(client->call(Method::kFetchBlock, "").get(), "");

  // A payload larger than socket buffers, with every byte value.
  std::string big(16 << 20, '\0');
  for (size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<char>(i * 7);
  }
  EXPECT_EQ(client->call(Method::kFetchBlock, big).get(), big);
}

TEST_F(RpcTest, concurrentCallsCompleteOutOfOrder) {
  auto client = RpcClient::connect(address());
  // The first call answers last.
  auto slow = client->call(Method::kFetchBlock, "300 slow");
  std::vector<folly::SemiFuture<std::string>> fast;
  for (int i = 0; i < 100; ++i) {
    fast.push_back(client->call(Method::kFetchBlock, fmt::format("0 {}", i)));
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
      auto client = RpcClient::connect(address());
      for (int i = 0; i < 50; ++i) {
        auto payload = fmt::format("x{}-{}", t, i);
        if (client->call(Method::kFetchBlock, payload).get() == payload) {
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
  auto client = RpcClient::connect(address());
  EXPECT_THROW(
      {
        try {
          client->call(Method::kRemoveShuffle, "throw").get();
        } catch (const RpcError& e) {
          EXPECT_NE(std::string(e.what()).find("handler threw"), std::string::npos);
          throw;
        }
      },
      RpcError);
  EXPECT_THROW(client->call(Method::kRemoveShuffle, "").get(), RpcError);
  EXPECT_THROW(client->call(Method::kLaunchTask, "").get(), RpcError);
  // Errors leave the connection usable.
  EXPECT_EQ(client->call(Method::kFetchBlock, "still").get(), "still");
}

TEST_F(RpcTest, pendingCallsFailWhenConnectionBreaks) {
  auto client = RpcClient::connect(address());
  auto pending = client->call(Method::kHeartbeat, "");
  server_.stop();
  EXPECT_THROW(std::move(pending).get(std::chrono::seconds(10)), RpcError);
  EXPECT_FALSE(client->isOpen());
  EXPECT_THROW(client->call(Method::kFetchBlock, "x").get(), RpcError);
}

TEST_F(RpcTest, closeFailsPendingCalls) {
  auto client = RpcClient::connect(address());
  auto pending = client->call(Method::kHeartbeat, "");
  client->close();
  EXPECT_THROW(std::move(pending).get(std::chrono::seconds(10)), RpcError);
}

TEST_F(RpcTest, connectFails) {
  RpcServer other;
  other.start("", 0);
  const auto port = other.port();
  other.stop();
  EXPECT_THROW(
      RpcClient::connect(
          HostAndPort{"localhost", port}, std::chrono::milliseconds(500)),
      RpcError);
}

} // namespace
} // namespace ember::rpc
