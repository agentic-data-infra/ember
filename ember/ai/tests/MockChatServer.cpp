#include "ember/ai/tests/MockChatServer.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <boost/algorithm/string.hpp>
#include <fmt/format.h>
#include <folly/json.h>

#include "velox/common/base/Exceptions.h"

namespace ember::ai::test {

MockChatServer::MockChatServer(Handler handler)
    : handler_{std::move(handler)} {
  listenFd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  VELOX_CHECK_GE(listenFd_, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  VELOX_CHECK_EQ(
      ::bind(
          listenFd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
      0);
  VELOX_CHECK_EQ(::listen(listenFd_, 64), 0);
  socklen_t length = sizeof(address);
  ::getsockname(listenFd_, reinterpret_cast<sockaddr*>(&address), &length);
  port_ = ntohs(address.sin_port);
  thread_ = std::thread([this] { serve(); });
}

MockChatServer::~MockChatServer() {
  stopping_ = true;
  thread_.join();
  ::close(listenFd_);
}

std::string MockChatServer::url() const {
  return fmt::format("http://127.0.0.1:{}/v1", port_);
}

int MockChatServer::requests(const std::string& prompt) const {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = requests_.find(prompt);
  return it == requests_.end() ? 0 : it->second;
}

std::string MockChatServer::modelOf(const std::string& prompt) const {
  std::lock_guard<std::mutex> l(mutex_);
  auto it = models_.find(prompt);
  return it == models_.end() ? "" : it->second;
}

std::string MockChatServer::lastResponseFormat() const {
  std::lock_guard<std::mutex> l(mutex_);
  return lastResponseFormat_;
}

std::string MockChatServer::lastModel() const {
  std::lock_guard<std::mutex> l(mutex_);
  return lastModel_;
}

std::string MockChatServer::lastAuthorization() const {
  std::lock_guard<std::mutex> l(mutex_);
  return lastAuthorization_;
}

void MockChatServer::serve() {
  while (!stopping_) {
    pollfd listening{listenFd_, POLLIN, 0};
    if (::poll(&listening, 1, 50) <= 0) {
      continue;
    }
    const int connection = ::accept(listenFd_, nullptr, nullptr);
    if (connection >= 0) {
      handle(connection);
      ::close(connection);
    }
  }
}

void MockChatServer::handle(int connection) {
  std::string request;
  char buffer[4096];
  size_t headerEnd = std::string::npos;
  size_t contentLength = 0;
  while (true) {
    if (headerEnd == std::string::npos) {
      headerEnd = request.find("\r\n\r\n");
      if (headerEnd != std::string::npos) {
        std::vector<std::string> lines;
        boost::split(lines, request.substr(0, headerEnd), boost::is_any_of("\n"));
        for (auto& line : lines) {
          boost::trim(line);
          if (boost::istarts_with(line, "content-length:")) {
            contentLength = std::stoul(line.substr(15));
          } else if (boost::istarts_with(line, "authorization:")) {
            std::lock_guard<std::mutex> l(mutex_);
            lastAuthorization_ = boost::trim_copy(line.substr(14));
          }
        }
      }
    }
    if (headerEnd != std::string::npos &&
        request.size() >= headerEnd + 4 + contentLength) {
      break;
    }
    const auto n = ::read(connection, buffer, sizeof(buffer));
    if (n <= 0) {
      return;
    }
    request.append(buffer, n);
  }

  const auto body = folly::parseJson(request.substr(headerEnd + 4));
  const auto prompt = body["messages"][0]["content"].asString();
  int previous = 0;
  {
    std::lock_guard<std::mutex> l(mutex_);
    previous = requests_[prompt]++;
    lastModel_ = body["model"].asString();
    models_[prompt] = lastModel_;
    lastResponseFormat_ = body.count("response_format")
        ? folly::toJson(body["response_format"])
        : "";
  }
  const auto reply = handler_(prompt, previous);
  std::string content = reply.content;
  if (reply.status == 200) {
    content = folly::toJson(folly::dynamic::object(
        "choices",
        folly::dynamic::array(folly::dynamic::object(
            "message",
            folly::dynamic::object("role", "assistant")(
                "content", reply.content)))));
  }
  const auto response = fmt::format(
      "HTTP/1.1 {} Mock\r\nContent-Type: application/json\r\n"
      "Content-Length: {}\r\nConnection: close\r\n\r\n{}",
      reply.status,
      content.size(),
      content);
  size_t written = 0;
  while (written < response.size()) {
    const auto n =
        ::write(connection, response.data() + written, response.size() - written);
    if (n <= 0) {
      return;
    }
    written += n;
  }
}

} // namespace ember::ai::test
