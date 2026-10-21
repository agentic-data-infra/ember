#include "ember/rpc/Socket.h"

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <csignal>
#include <cstring>
#include <mutex>
#include <vector>

#include <folly/FileUtil.h>
#include <folly/String.h>
#include <folly/lang/Bits.h>

namespace ember::rpc {

namespace {

constexpr size_t kHeaderBytes = 16;

void encodeHeader(const FrameHeader& header, uint8_t* out) {
  const auto length = folly::Endian::little(header.length);
  const auto id = folly::Endian::little(header.id);
  std::memcpy(out, &length, 4);
  out[4] = static_cast<uint8_t>(header.kind);
  out[5] = static_cast<uint8_t>(header.method);
  out[6] = 0;
  out[7] = 0;
  std::memcpy(out + 8, &id, 8);
}

FrameHeader decodeHeader(const uint8_t* in) {
  FrameHeader header;
  uint32_t length;
  uint64_t id;
  std::memcpy(&length, in, 4);
  std::memcpy(&id, in + 8, 8);
  header.length = folly::Endian::little(length);
  header.kind = static_cast<FrameKind>(in[4]);
  header.method = static_cast<Method>(in[5]);
  header.id = folly::Endian::little(id);
  return header;
}

void setNoDelay(int fd) {
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

struct AddrInfo {
  addrinfo* list{nullptr};
  ~AddrInfo() {
    if (list != nullptr) {
      ::freeaddrinfo(list);
    }
  }
};

AddrInfo resolve(const std::string& host, uint16_t port, bool passive) {
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  if (passive) {
    hints.ai_flags = AI_PASSIVE;
  }
  AddrInfo result;
  const auto service = std::to_string(port);
  const bool anyHost = host.empty() || host == "0.0.0.0" || host == "*";
  if (passive && anyHost) {
    // Prefer IPv4 for the wildcard, so "localhost" clients reach it.
    hints.ai_family = AF_INET;
  }
  const int rc = ::getaddrinfo(
      anyHost && passive ? nullptr : host.c_str(),
      service.c_str(),
      &hints,
      &result.list);
  if (rc != 0) {
    throw RpcError(
        fmt::format("Cannot resolve {}:{}: {}", host, port, gai_strerror(rc)));
  }
  return result;
}

} // namespace

void ignoreSigpipe() {
  static std::once_flag once;
  std::call_once(once, [] { ::signal(SIGPIPE, SIG_IGN); });
}

int listenTcp(const std::string& host, uint16_t port) {
  ignoreSigpipe();
  auto addresses = resolve(host, port, /*passive=*/true);
  std::string lastError = "no address";
  for (auto* ai = addresses.list; ai != nullptr; ai = ai->ai_next) {
    int fd = ::socket(ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC, 0);
    if (fd < 0) {
      lastError = folly::errnoStr(errno);
      continue;
    }
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    if (::bind(fd, ai->ai_addr, ai->ai_addrlen) == 0 && ::listen(fd, 128) == 0) {
      return fd;
    }
    lastError = folly::errnoStr(errno);
    ::close(fd);
  }
  throw RpcError(
      fmt::format("Cannot listen on {}:{}: {}", host, port, lastError));
}

uint16_t boundPort(int fd) {
  sockaddr_storage address{};
  socklen_t length = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
    throw RpcError(fmt::format("getsockname: {}", folly::errnoStr(errno)));
  }
  if (address.ss_family == AF_INET6) {
    return ntohs(reinterpret_cast<sockaddr_in6*>(&address)->sin6_port);
  }
  return ntohs(reinterpret_cast<sockaddr_in*>(&address)->sin_port);
}

int connectTcp(const HostAndPort& address, std::chrono::milliseconds timeout) {
  ignoreSigpipe();
  auto addresses = resolve(address.host, address.port, /*passive=*/false);
  std::string lastError = "no address";
  for (auto* ai = addresses.list; ai != nullptr; ai = ai->ai_next) {
    int fd = ::socket(
        ai->ai_family, ai->ai_socktype | SOCK_CLOEXEC | SOCK_NONBLOCK, 0);
    if (fd < 0) {
      lastError = folly::errnoStr(errno);
      continue;
    }
    int rc = ::connect(fd, ai->ai_addr, ai->ai_addrlen);
    if (rc != 0 && errno == EINPROGRESS) {
      pollfd pfd{fd, POLLOUT, 0};
      rc = ::poll(&pfd, 1, static_cast<int>(timeout.count()));
      if (rc == 1) {
        int error = 0;
        socklen_t length = sizeof(error);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length);
        rc = error == 0 ? 0 : -1;
        errno = error;
      } else {
        if (rc == 0) {
          errno = ETIMEDOUT;
        }
        rc = -1;
      }
    }
    if (rc == 0) {
      ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) & ~O_NONBLOCK);
      setNoDelay(fd);
      return fd;
    }
    lastError = folly::errnoStr(errno);
    ::close(fd);
  }
  throw RpcError(
      fmt::format("Cannot connect to {}: {}", address.toString(), lastError));
}

bool writeFrame(int fd, const FrameHeader& header, const folly::IOBuf* payload) {
  uint8_t encoded[kHeaderBytes];
  encodeHeader(header, encoded);
  std::vector<iovec> iov;
  iov.push_back({encoded, kHeaderBytes});
  if (payload != nullptr) {
    for (auto range : *payload) {
      if (!range.empty()) {
        iov.push_back(
            {const_cast<uint8_t*>(range.data()), static_cast<size_t>(range.size())});
      }
    }
  }
  size_t total = 0;
  for (const auto& v : iov) {
    total += v.iov_len;
  }
  // writevFull handles partial writes and EINTR, in chunks of IOV_MAX.
  size_t written = 0;
  for (size_t i = 0; i < iov.size(); i += IOV_MAX) {
    const auto n = std::min<size_t>(IOV_MAX, iov.size() - i);
    auto rc = folly::writevFull(fd, iov.data() + i, n);
    if (rc < 0) {
      return false;
    }
    written += rc;
  }
  return written == total;
}

bool readFrame(int fd, FrameHeader& header, std::string& payload) {
  uint8_t encoded[kHeaderBytes];
  if (folly::readFull(fd, encoded, kHeaderBytes) !=
      static_cast<ssize_t>(kHeaderBytes)) {
    return false;
  }
  header = decodeHeader(encoded);
  if (header.length > kMaxPayloadBytes ||
      header.kind < FrameKind::kRequest || header.kind > FrameKind::kError) {
    throw RpcError("Malformed RPC frame");
  }
  payload.resize(header.length);
  return folly::readFull(fd, payload.data(), header.length) ==
      static_cast<ssize_t>(header.length);
}

} // namespace ember::rpc
