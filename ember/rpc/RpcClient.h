// A connection to one of Ember's Thrift services (ember.thrift), usable from
// any thread.
//
// fbthrift's channels belong to an event base thread; this wraps one on a
// thread of a process-wide IO pool. Any number of calls may be in flight on a
// connection and complete in any order. Calls fail with RpcError: when the
// handler threw, when the connection broke (which fails every pending call),
// or after close(). There is no default call timeout, since a task runs as
// long as it takes; callers bound their waits.

#pragma once

#include <atomic>
#include <chrono>
#include <memory>

#include <fmt/format.h>
#include <folly/futures/Future.h>
#include <thrift/lib/cpp2/async/RequestCallback.h>
#include <thrift/lib/cpp2/async/RequestChannel.h>

#include "ember/rpc/Rpc.h"

namespace folly {
class EventBase;
}

namespace apache::thrift {
template <typename Service>
class Client;
}

namespace ember::rpc {

/// The service-independent part of RpcClient: the channel and its state.
class Connection : private apache::thrift::CloseCallback {
 public:
  /// Connects to 'address'. Throws RpcError if it cannot within 'timeout'.
  explicit Connection(
      const HostAndPort& address,
      std::chrono::milliseconds timeout);

  ~Connection() override;

  Connection(const Connection&) = delete;
  Connection& operator=(const Connection&) = delete;

  const HostAndPort& address() const {
    return address_;
  }

  /// False once the connection broke or close() was called.
  bool isOpen() const {
    return open_;
  }

  /// Breaks the connection; pending calls fail.
  void close();

  folly::EventBase* eventBase() const {
    return eventBase_;
  }

  const std::shared_ptr<apache::thrift::RequestChannel>& channel() const {
    return channel_;
  }

 private:
  void channelClosed() override;

  const HostAndPort address_;
  folly::EventBase* const eventBase_;
  std::shared_ptr<apache::thrift::RequestChannel> channel_;
  std::atomic<bool> open_{true};
};

/// Turns a failed call's exception into RpcError.
[[noreturn]] void throwRpcError(
    const HostAndPort& address,
    const folly::exception_wrapper& error);

template <typename Service>
class RpcClient {
 public:
  /// Connects to 'address'. Throws RpcError if it cannot within 'timeout'.
  static std::shared_ptr<RpcClient> connect(
      const HostAndPort& address,
      std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    return std::shared_ptr<RpcClient>(new RpcClient(address, timeout));
  }

  ~RpcClient() {
    // The generated client releases the channel, which must happen on its
    // thread.
    connection_.eventBase()->runImmediatelyOrRunInEventBaseThreadAndWait(
        [this] { client_.reset(); });
  }

  RpcClient(const RpcClient&) = delete;
  RpcClient& operator=(const RpcClient&) = delete;

  /// Makes a call with the generated client, e.g.
  ///
  ///   client->call([&](auto& c) { return c.semifuture_heartbeat(request); })
  ///
  /// The arguments are serialized before call() returns.
  template <typename MakeCall>
  auto call(MakeCall&& makeCall) {
    using Result = typename decltype(makeCall(*client_))::value_type;
    if (!connection_.isOpen()) {
      return folly::makeSemiFuture<Result>(RpcError(fmt::format(
          "Connection to {} is closed", connection_.address().toString())));
    }
    return makeCall(*client_).deferError(
        [address = connection_.address()](
            folly::exception_wrapper error) -> Result {
          throwRpcError(address, error);
        });
  }

  const HostAndPort& address() const {
    return connection_.address();
  }

  /// False once the connection broke or close() was called.
  bool isOpen() const {
    return connection_.isOpen();
  }

  /// Breaks the connection; pending calls fail.
  void close() {
    connection_.close();
  }

 private:
  RpcClient(const HostAndPort& address, std::chrono::milliseconds timeout)
      : connection_{address, timeout},
        client_{std::make_unique<apache::thrift::Client<Service>>(
            connection_.channel())} {}

  Connection connection_;
  std::unique_ptr<apache::thrift::Client<Service>> client_;
};

} // namespace ember::rpc
