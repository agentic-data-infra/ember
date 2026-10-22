// What Ember's RPC has besides the generated services (ember.thrift):
// addresses and the error calls fail with.

#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>

namespace ember::rpc {

/// A failed call: the connection broke, the call timed out or the server's
/// handler threw.
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
