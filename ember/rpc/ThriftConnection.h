// Internal to ember/rpc: a connection to one of Ember's Thrift services
// (ember.thrift), usable from any thread. Clients.h is the public side.
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
#include <folly/coro/Task.h>
#include <folly/io/async/EventBase.h>
#include <thrift/lib/cpp2/async/RequestCallback.h>
#include <thrift/lib/cpp2/async/RequestChannel.h>

#include "ember/rpc/Rpc.h"


namespace apache::thrift {
template <typename Service>
class Client;
}

namespace ember::rpc::detail {

/// A Rocket channel and whether it is still open.
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

/// The generated client of 'Service' over 'connection'. Destroy it with
/// destroyClient().
template <typename Service>
std::unique_ptr<apache::thrift::Client<Service>> makeClient(
    const Connection& connection) {
  return std::make_unique<apache::thrift::Client<Service>>(
      connection.channel());
}

/// Destroys a generated client on its connection's thread, where its channel
/// must be released.
template <typename Service>
void destroyClient(
    const Connection& connection,
    std::unique_ptr<apache::thrift::Client<Service>>& client) {
  connection.eventBase()->runImmediatelyOrRunInEventBaseThreadAndWait(
      [&] { client.reset(); });
}

/// Awaits a call of a generated client, e.g.
///
///   co_await call(connection, client->co_heartbeat(request));
///
/// Fails with RpcError if the connection is closed or the call fails. The
/// call's arguments must stay alive until it completes.
template <typename T>
folly::coro::Task<T> call(
    const Connection& connection,
    folly::coro::Task<T> call) {
  if (!connection.isOpen()) {
    throw RpcError(fmt::format(
        "Connection to {} is closed", connection.address().toString()));
  }
  auto result = co_await folly::coro::co_awaitTry(std::move(call));
  if (result.hasException()) {
    throwRpcError(connection.address(), result.exception());
  }
  co_return std::move(result).value();
}

} // namespace ember::rpc::detail
