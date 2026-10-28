// The RPC layer on its own: concurrent calls on one connection, handler
// errors, calls failing when a connection breaks, and blocked handlers
// delaying but not failing other calls.

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <thread>

#include <folly/coro/BlockingWait.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/executors/InlineExecutor.h>
#include <folly/futures/SharedPromise.h>

#include "ember/rpc/Clients.h"
#include "ember/rpc/RpcServer.h"

namespace ember::rpc {
namespace {

// launchTask echoes its task after the number of ms it starts with, if any;
// "throw" throws right away and "fail" throws on the handler's pool.
// removeShuffle does not return until the test lets it. fetchBlock returns a
// block of 'reduceId' bytes. listFiles returns one file named after its
// arguments.
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

  folly::coro::Task<std::string> co_launchTask(std::string task) override {
    if (task == "throw") {
      throw std::runtime_error("handler threw");
    }
    auto run = [](std::string task) -> folly::coro::Task<std::string> {
      if (task == "fail") {
        throw std::runtime_error("task failed");
      }
      auto delay = folly::tryTo<int32_t>(task.substr(0, task.find(' ')));
      if (delay.hasValue()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(*delay));
      }
      co_return task;
    };
    co_return co_await folly::coro::co_withExecutor(
        folly::getKeepAliveToken(pool_.get()), run(std::move(task)));
  }

  void removeShuffle(int32_t /*shuffleId*/) override {
    hang_.getSemiFuture().wait();
  }

  void shutdown() override {}

  std::vector<FileInfo> listFiles(
      const std::string& schema,
      const std::string& path) override {
    return {FileInfo{.path = path + "/" + schema, .size = 42}};
  }

  folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchBlock(
      BlockId block) override {
    std::string bytes(block.reduceId, '\0');
    for (size_t i = 0; i < bytes.size(); ++i) {
      bytes[i] = static_cast<char>(i * 7);
    }
    co_return folly::IOBuf::fromString(std::move(bytes));
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

  static std::string echo(WorkerClient& client, const std::string& task) {
    return folly::coro::blockingWait(client.co_launchTask(task));
  }

  // Starts a call, which is sent before start() returns.
  template <typename T>
  static auto start(folly::coro::Task<T> call) {
    return folly::coro::co_withExecutor(
               &folly::InlineExecutor::instance(), std::move(call))
        .start();
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
  EXPECT_EQ(echo(*client, "hello"), "hello");
  EXPECT_EQ(echo(*client, ""), "");

  // A payload larger than socket buffers, with every byte value.
  std::string big(16 << 20, '\0');
  for (size_t i = 0; i < big.size(); ++i) {
    big[i] = static_cast<char>(i * 7);
  }
  EXPECT_EQ(echo(*client, big), big);

  auto blocks = ShuffleBlockClient::connect(address());
  auto bytes = folly::coro::blockingWait(
      blocks->co_fetchBlock(BlockId{.reduceId = 16 << 20}));
  EXPECT_EQ(bytes->computeChainDataLength(), big.size());
  EXPECT_EQ(bytes->to<std::string>(), big);

  auto files = folly::coro::blockingWait(client->co_listFiles("gensort", "/in"));
  ASSERT_EQ(files.size(), 1);
  EXPECT_EQ(files[0].path, "/in/gensort");
  EXPECT_EQ(files[0].size, 42);
}

TEST_F(RpcTest, concurrentCallsCompleteOutOfOrder) {
  auto client = WorkerClient::connect(address());
  // The first call answers last.
  auto slow = start(client->co_launchTask("300 slow"));
  std::vector<folly::SemiFuture<std::string>> fast;
  for (int i = 0; i < 100; ++i) {
    fast.push_back(start(client->co_launchTask(fmt::format("0 {}", i))));
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
        if (echo(*client, payload) == payload) {
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
          echo(*client, "throw");
        } catch (const RpcError& e) {
          EXPECT_NE(
              std::string(e.what()).find("handler threw"), std::string::npos);
          throw;
        }
      },
      RpcError);
  EXPECT_THROW(echo(*client, "fail"), RpcError);
  // Errors leave the connection usable.
  EXPECT_EQ(echo(*client, "still"), "still");
}

// A peer that accepts one connection, reads until it has seen 'marker' (a
// call's method name), then closes the connection: a server that dies with
// the call in flight.
class DroppingPeer {
 public:
  explicit DroppingPeer(std::string marker) : marker_{std::move(marker)} {
    listener_ = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    socklen_t length = sizeof(address);
    EXPECT_EQ(::bind(listener_, (sockaddr*)&address, length), 0);
    EXPECT_EQ(::listen(listener_, 1), 0);
    ::getsockname(listener_, (sockaddr*)&address, &length);
    port_ = ntohs(address.sin_port);
    thread_ = std::thread([this] { run(); });
  }

  ~DroppingPeer() {
    thread_.join();
    ::close(listener_);
  }

  HostAndPort address() const {
    return HostAndPort{"127.0.0.1", port_};
  }

 private:
  void run() {
    const int connection = ::accept(listener_, nullptr, nullptr);
    std::string received;
    char buffer[4096];
    while (received.find(marker_) == std::string::npos) {
      const auto n = ::read(connection, buffer, sizeof(buffer));
      if (n <= 0) {
        break;
      }
      received.append(buffer, n);
    }
    ::close(connection);
  }

  const std::string marker_;
  int listener_;
  uint16_t port_;
  std::thread thread_;
};

TEST_F(RpcTest, pendingCallsFailWhenConnectionBreaks) {
  DroppingPeer peer("removeShuffle");
  auto client = WorkerClient::connect(peer.address());
  auto pending = start(client->co_removeShuffle(1));
  EXPECT_THROW(std::move(pending).get(std::chrono::seconds(10)), RpcError);
  EXPECT_FALSE(client->isOpen());
  EXPECT_THROW(echo(*client, "x"), RpcError);
}

TEST_F(RpcTest, blockedHandlersDelayButDoNotFailOtherCalls) {
  auto client = WorkerClient::connect(address());
  // A blocked handler leaves the other handler threads to other calls.
  std::vector<folly::SemiFuture<folly::Unit>> blocked;
  blocked.push_back(start(client->co_removeShuffle(0)));
  start(client->co_shutdown()).get(std::chrono::seconds(10));
  EXPECT_EQ(echo(*client, "other"), "other");

  // With every handler thread blocked, calls wait for one, however long.
  for (int i = 1; i < 4; ++i) {
    blocked.push_back(start(client->co_removeShuffle(i)));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto queued = start(client->co_launchTask("queued"));
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  EXPECT_FALSE(queued.isReady());
  handler_->release();
  EXPECT_EQ(std::move(queued).get(std::chrono::seconds(10)), "queued");
  for (auto& call : blocked) {
    std::move(call).get(std::chrono::seconds(10));
  }
}

TEST_F(RpcTest, closeFailsPendingCalls) {
  auto client = WorkerClient::connect(address());
  auto pending = start(client->co_removeShuffle(1));
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

TEST_F(RpcTest, startFailsWhenPortIsTaken) {
  RpcServer other;
  EXPECT_THROW(
      other.start(std::make_shared<TestHandler>(), "", server_.port()),
      RpcError);
  // The first server still answers.
  auto client = WorkerClient::connect(address());
  EXPECT_EQ(echo(*client, "hello"), "hello");
}

} // namespace
} // namespace ember::rpc
