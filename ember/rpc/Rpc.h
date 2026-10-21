// A small RPC over TCP between the driver and workers, and between workers
// for shuffle blocks.
//
// A connection carries frames in both directions. Each frame is a 16-byte
// header followed by a payload:
//
//   uint32 payload length | uint8 kind | uint8 method | uint16 0 | uint64 id
//
// little endian. A client sends requests, each with a fresh id; the server
// answers each with a response (or an error, whose payload is the message)
// carrying the same id. Requests on one connection may be answered out of
// order, so one connection serves any number of concurrent calls. Payloads are
// opaque bytes: JSON for control messages (Messages.h), task and result bytes,
// raw shuffle blocks.

#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>

namespace ember::rpc {

enum class Method : uint8_t {
  /// Worker -> driver: a worker joins (RegisterWorker).
  kRegisterWorker = 1,
  /// Worker -> driver: the worker is alive (Heartbeat).
  kHeartbeat = 2,
  /// Driver -> worker: run a TaskDescription; the reply is its TaskResult.
  kLaunchTask = 3,
  /// Driver -> worker: delete a shuffle's files (RemoveShuffle).
  kRemoveShuffle = 4,
  /// Driver -> worker: the application is over.
  kShutdown = 5,
  /// Worker -> worker: the bytes of one shuffle block (FetchBlock).
  kFetchBlock = 6,
};

const char* methodName(Method method);

enum class FrameKind : uint8_t {
  kRequest = 1,
  kResponse = 2,
  kError = 3,
};

/// Frames larger than this are refused.
constexpr uint32_t kMaxPayloadBytes = 0x7fffffff;

/// A failed call: the connection broke or the server's handler threw.
class RpcError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

/// A TCP endpoint, written "host:port" ("[v6addr]:port" for IPv6 literals).
struct HostAndPort {
  std::string host;
  uint16_t port{0};

  std::string toString() const;

  /// Parses "host:port". Throws on a malformed address.
  static HostAndPort parse(const std::string& address);

  bool operator==(const HostAndPort& other) const = default;
};

/// This machine's host name, as other hosts should reach it.
std::string localHostName();

} // namespace ember::rpc
