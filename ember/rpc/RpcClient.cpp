#include "ember/rpc/RpcClient.h"

#include <folly/executors/IOThreadPoolExecutor.h>
#include <folly/io/async/AsyncSocket.h>
#include <thrift/lib/cpp2/async/RocketClientChannel.h>

namespace ember::rpc {

namespace {

// The event base threads of every connection in the process.
folly::EventBase* nextEventBase() {
  static auto* pool = new folly::IOThreadPoolExecutor(
      4, std::make_shared<folly::NamedThreadFactory>("EmberRpcIO"));
  return pool->getEventBase();
}

class ConnectCallback : public folly::AsyncSocket::ConnectCallback {
 public:
  folly::SemiFuture<folly::Unit> future() {
    return promise_.getSemiFuture();
  }

  void connectSuccess() noexcept override {
    promise_.setValue();
  }

  void connectErr(const folly::AsyncSocketException& error) noexcept override {
    promise_.setException(RpcError(error.what()));
  }

 private:
  folly::Promise<folly::Unit> promise_;
};

} // namespace

Connection::Connection(
    const HostAndPort& address,
    std::chrono::milliseconds timeout)
    : address_{address}, eventBase_{nextEventBase()} {
  folly::SocketAddress socketAddress;
  try {
    socketAddress.setFromHostPort(address.host, address.port);
  } catch (const std::exception& e) {
    throw RpcError(
        fmt::format("Cannot resolve {}: {}", address.toString(), e.what()));
  }

  ConnectCallback connected;
  folly::AsyncSocket::UniquePtr socket;
  eventBase_->runInEventBaseThreadAndWait([&] {
    socket = folly::AsyncSocket::UniquePtr(new folly::AsyncSocket(eventBase_));
    socket->connect(&connected, socketAddress, timeout.count());
  });
  auto result = connected.future().getTry();
  if (result.hasException()) {
    eventBase_->runInEventBaseThreadAndWait([&] { socket.reset(); });
    throw RpcError(fmt::format(
        "Cannot connect to {}: {}",
        address.toString(),
        result.exception().get_exception()->what()));
  }

  eventBase_->runInEventBaseThreadAndWait([&] {
    auto channel = apache::thrift::RocketClientChannel::newChannel(
        std::move(socket));
    // Tasks run as long as they take; callers bound their own waits.
    channel->setTimeout(0);
    channel->setCloseCallback(this);
    channel_ = std::move(channel);
  });
}

Connection::~Connection() {
  eventBase_->runImmediatelyOrRunInEventBaseThreadAndWait([this] {
    auto* channel = static_cast<apache::thrift::ClientChannel*>(channel_.get());
    channel->setCloseCallback(nullptr);
    channel->closeNow();
    channel_.reset();
  });
}

void Connection::close() {
  open_ = false;
  // Without waiting: the caller may hold locks that failing calls' callbacks
  // take.
  eventBase_->runInEventBaseThread([channel = channel_] {
    static_cast<apache::thrift::ClientChannel*>(channel.get())->closeNow();
  });
}

void Connection::channelClosed() {
  open_ = false;
}

void throwRpcError(
    const HostAndPort& address,
    const folly::exception_wrapper& error) {
  if (error.is_compatible_with<RpcError>()) {
    error.throw_exception();
  }
  const auto* e = error.get_exception();
  throw RpcError(fmt::format(
      "Call to {} failed: {}",
      address.toString(),
      e != nullptr ? e->what() : error.what().toStdString()));
}

} // namespace ember::rpc
