#include "ember/rpc/RpcServer.h"

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <folly/String.h>
#include <folly/executors/InlineExecutor.h>
#include <glog/logging.h>

#include "ember/rpc/Socket.h"

namespace ember::rpc {

struct RpcServer::Connection {
  explicit Connection(int fd) : fd{fd} {}

  ~Connection() {
    ::close(fd);
  }

  void send(const FrameHeader& header, const folly::IOBuf* payload) {
    std::lock_guard<std::mutex> l(writeMutex);
    if (!closed && !writeFrame(fd, header, payload)) {
      shutdown();
    }
  }

  void shutdown() {
    if (!closed.exchange(true)) {
      ::shutdown(fd, SHUT_RDWR);
    }
  }

  const int fd;
  std::thread thread;
  std::mutex writeMutex;
  std::atomic<bool> closed{false};
  std::atomic<bool> finished{false};
};

RpcServer::~RpcServer() {
  stop();
}

void RpcServer::setHandler(Method method, Handler handler) {
  handlers_[method] = std::move(handler);
}

void RpcServer::start(const std::string& host, uint16_t port) {
  listenFd_ = listenTcp(host, port);
  port_ = boundPort(listenFd_);
  acceptor_ = std::thread([this] { acceptLoop(); });
}

void RpcServer::stop() {
  if (stopped_.exchange(true) || listenFd_ < 0) {
    return;
  }
  ::shutdown(listenFd_, SHUT_RDWR);
  if (acceptor_.joinable()) {
    acceptor_.join();
  }
  ::close(listenFd_);
  std::list<std::shared_ptr<Connection>> connections;
  {
    std::lock_guard<std::mutex> l(mutex_);
    connections.swap(connections_);
  }
  for (auto& connection : connections) {
    connection->shutdown();
  }
  for (auto& connection : connections) {
    if (connection->thread.joinable()) {
      connection->thread.join();
    }
  }
}

void RpcServer::acceptLoop() {
  while (!stopped_) {
    const int fd = ::accept4(listenFd_, nullptr, nullptr, SOCK_CLOEXEC);
    if (fd < 0) {
      if (errno == EINTR || errno == ECONNABORTED) {
        continue;
      }
      if (!stopped_) {
        LOG(ERROR) << "RPC server on port " << port_
                   << " stopped accepting: " << folly::errnoStr(errno);
      }
      return;
    }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    auto connection = std::make_shared<Connection>(fd);
    reapFinished();
    std::lock_guard<std::mutex> l(mutex_);
    if (stopped_) {
      return;
    }
    connections_.push_back(connection);
    connection->thread = std::thread([this, connection] { serve(connection); });
  }
}

void RpcServer::reapFinished() {
  std::list<std::shared_ptr<Connection>> finished;
  {
    std::lock_guard<std::mutex> l(mutex_);
    for (auto it = connections_.begin(); it != connections_.end();) {
      if ((*it)->finished) {
        finished.push_back(std::move(*it));
        it = connections_.erase(it);
      } else {
        ++it;
      }
    }
  }
  for (auto& connection : finished) {
    connection->thread.join();
  }
}

void RpcServer::serve(const std::shared_ptr<Connection>& connection) {
  FrameHeader header;
  std::string payload;
  try {
    while (readFrame(connection->fd, header, payload)) {
      if (header.kind != FrameKind::kRequest) {
        LOG(WARNING) << "RPC server got a non-request frame; closing";
        break;
      }
      const auto id = header.id;
      const auto method = header.method;
      auto replyError = [connection, id, method](const std::string& message) {
        auto buffer = folly::IOBuf::copyBuffer(message);
        connection->send(
            FrameHeader{
                static_cast<uint32_t>(message.size()),
                FrameKind::kError,
                method,
                id},
            buffer.get());
      };
      auto it = handlers_.find(method);
      if (it == handlers_.end()) {
        replyError(fmt::format("No handler for {}", methodName(method)));
        continue;
      }
      auto future = folly::SemiFuture<std::unique_ptr<folly::IOBuf>>::makeEmpty();
      try {
        future = it->second(std::move(payload));
      } catch (const std::exception& e) {
        replyError(e.what());
        payload = std::string();
        continue;
      }
      payload = std::string();
      std::move(future)
          .via(&folly::InlineExecutor::instance())
          .thenTry([connection, id, method, replyError](
                       folly::Try<std::unique_ptr<folly::IOBuf>> result) {
            if (result.hasException()) {
              replyError(result.exception().what().toStdString());
              return;
            }
            const auto& response = *result;
            const auto length =
                response == nullptr ? 0 : response->computeChainDataLength();
            if (length > kMaxPayloadBytes) {
              replyError(fmt::format("Response of {} bytes is too large", length));
              return;
            }
            connection->send(
                FrameHeader{
                    static_cast<uint32_t>(length),
                    FrameKind::kResponse,
                    method,
                    id},
                response.get());
          });
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "RPC connection closed: " << e.what();
  }
  connection->shutdown();
  connection->finished = true;
}

} // namespace ember::rpc
