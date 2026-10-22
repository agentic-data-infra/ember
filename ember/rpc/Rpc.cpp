#include "ember/rpc/Rpc.h"

#include <unistd.h>

#include <climits>

#include <fmt/format.h>
#include <folly/Conv.h>

namespace ember::rpc {

std::string HostAndPort::toString() const {
  if (host.find(':') != std::string::npos) {
    return fmt::format("[{}]:{}", host, port);
  }
  return fmt::format("{}:{}", host, port);
}

HostAndPort HostAndPort::parse(const std::string& address) {
  const auto colon = address.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 == address.size()) {
    throw RpcError(fmt::format("Expected host:port, got '{}'", address));
  }
  auto host = address.substr(0, colon);
  if (host.size() >= 2 && host.front() == '[' && host.back() == ']') {
    host = host.substr(1, host.size() - 2);
  }
  auto port = folly::tryTo<uint16_t>(address.substr(colon + 1));
  if (!port.hasValue()) {
    throw RpcError(fmt::format("Bad port in '{}'", address));
  }
  return HostAndPort{std::move(host), *port};
}

std::string localHostName() {
  char name[HOST_NAME_MAX + 1] = {};
  if (::gethostname(name, sizeof(name)) != 0 || name[0] == '\0') {
    return "localhost";
  }
  return name;
}

} // namespace ember::rpc
