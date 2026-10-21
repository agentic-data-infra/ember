// Blocking TCP socket helpers for the RPC layer.

#pragma once

#include <chrono>
#include <string>

#include <folly/io/IOBuf.h>

#include "ember/rpc/Rpc.h"

namespace ember::rpc {

/// A frame header as on the wire (see Rpc.h).
struct FrameHeader {
  uint32_t length{0};
  FrameKind kind{FrameKind::kRequest};
  Method method{Method::kRegisterWorker};
  uint64_t id{0};
};

/// Listens on 'host' (empty or "0.0.0.0" for all interfaces) and 'port' (0
/// picks a free port). Returns the socket.
int listenTcp(const std::string& host, uint16_t port);

/// The port a socket is bound to.
uint16_t boundPort(int fd);

/// Connects to 'address' within 'timeout'. Throws RpcError on failure.
int connectTcp(const HostAndPort& address, std::chrono::milliseconds timeout);

/// Writes one frame. Returns false if the socket is broken.
bool writeFrame(int fd, const FrameHeader& header, const folly::IOBuf* payload);

/// Reads one frame into 'header' and 'payload'. Returns false at end of
/// stream or on error. Throws RpcError on a malformed frame.
bool readFrame(int fd, FrameHeader& header, std::string& payload);

/// Makes writes to closed sockets fail with EPIPE instead of a signal.
void ignoreSigpipe();

} // namespace ember::rpc
