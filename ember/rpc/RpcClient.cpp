#include "ember/rpc/RpcClient.h"

#include <sys/socket.h>
#include <unistd.h>

#include <glog/logging.h>

#include "ember/rpc/Socket.h"

namespace ember::rpc {

std::shared_ptr<RpcClient> RpcClient::connect(
    const HostAndPort& address,
    std::chrono::milliseconds timeout) {
  const int fd = connectTcp(address, timeout);
  std::shared_ptr<RpcClient> client(new RpcClient(address, fd));
  client->reader_ = std::thread([raw = client.get()] { raw->readLoop(); });
  return client;
}

RpcClient::RpcClient(HostAndPort address, int fd)
    : address_{std::move(address)}, fd_{fd} {}

RpcClient::~RpcClient() {
  close();
  if (reader_.joinable()) {
    if (reader_.get_id() == std::this_thread::get_id()) {
      reader_.detach();
    } else {
      reader_.join();
    }
  }
  ::close(fd_);
}

void RpcClient::close() {
  if (!closed_.exchange(true)) {
    ::shutdown(fd_, SHUT_RDWR);
  }
}

folly::SemiFuture<std::string> RpcClient::call(
    Method method,
    std::string payload) {
  return call(method, folly::IOBuf::fromString(std::move(payload)));
}

folly::SemiFuture<std::string> RpcClient::call(
    Method method,
    std::unique_ptr<folly::IOBuf> payload) {
  const auto length = payload->computeChainDataLength();
  if (length > kMaxPayloadBytes) {
    return folly::makeSemiFuture<std::string>(RpcError(fmt::format(
        "{} request of {} bytes is too large", methodName(method), length)));
  }
  folly::Promise<std::string> promise;
  auto future = promise.getSemiFuture();
  uint64_t id;
  {
    std::lock_guard<std::mutex> l(mutex_);
    if (closed_) {
      return folly::makeSemiFuture<std::string>(RpcError(
          fmt::format("Connection to {} is closed", address_.toString())));
    }
    id = nextId_++;
    pending_.emplace(id, std::move(promise));
  }
  FrameHeader header{
      static_cast<uint32_t>(length), FrameKind::kRequest, method, id};
  bool ok;
  {
    std::lock_guard<std::mutex> l(writeMutex_);
    ok = writeFrame(fd_, header, payload.get());
  }
  if (!ok) {
    // The reader sees the broken connection too and fails every call.
    close();
  }
  return future;
}

void RpcClient::readLoop() {
  FrameHeader header;
  std::string payload;
  std::string reason = "connection closed";
  try {
    while (readFrame(fd_, header, payload)) {
      folly::Promise<std::string> promise;
      {
        std::lock_guard<std::mutex> l(mutex_);
        auto it = pending_.find(header.id);
        if (it == pending_.end()) {
          LOG(WARNING) << "RPC response for unknown call " << header.id
                       << " from " << address_.toString();
          continue;
        }
        promise = std::move(it->second);
        pending_.erase(it);
      }
      if (header.kind == FrameKind::kError) {
        promise.setException(RpcError(payload));
      } else {
        promise.setValue(std::move(payload));
      }
      payload = std::string();
    }
  } catch (const std::exception& e) {
    reason = e.what();
  }
  close();
  failPending(reason);
}

void RpcClient::failPending(const std::string& reason) {
  std::map<uint64_t, folly::Promise<std::string>> pending;
  {
    std::lock_guard<std::mutex> l(mutex_);
    pending.swap(pending_);
  }
  for (auto& [id, promise] : pending) {
    promise.setException(RpcError(fmt::format(
        "Connection to {} lost: {}", address_.toString(), reason)));
  }
}

} // namespace ember::rpc
