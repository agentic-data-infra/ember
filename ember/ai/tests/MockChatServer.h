// A minimal OpenAI-compatible chat completions server for tests. It serves
// one connection at a time on a thread of its own and answers each request
// with what the handler returns for the request's prompt.

#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <thread>

namespace ember::ai::test {

class MockChatServer {
 public:
  struct Reply {
    /// 200 wraps 'content' in a chat completion, whose usage counts a token
    /// per byte of the prompt and of 'content'; any other status sends
    /// 'content' as the body.
    int status{200};
    std::string content;
  };

  /// Called with the prompt and how many requests carried it before.
  using Handler = std::function<Reply(const std::string& prompt, int previous)>;

  explicit MockChatServer(Handler handler);
  ~MockChatServer();

  /// The base URL of the API, e.g. http://127.0.0.1:12345/v1.
  std::string url() const;

  /// How many requests carried 'prompt'.
  int requests(const std::string& prompt) const;

  /// The model of the last request that carried 'prompt'.
  std::string modelOf(const std::string& prompt) const;

  /// The model, Authorization header and response_format (as JSON, empty
  /// when there was none) of the last request.
  std::string lastModel() const;
  std::string lastAuthorization() const;
  std::string lastResponseFormat() const;

 private:
  void serve();
  void handle(int connection);

  const Handler handler_;
  int listenFd_{-1};
  int port_{0};
  std::atomic<bool> stopping_{false};
  mutable std::mutex mutex_;
  std::map<std::string, int> requests_;
  std::map<std::string, std::string> models_;
  std::string lastResponseFormat_;
  std::string lastModel_;
  std::string lastAuthorization_;
  std::thread thread_;
};

} // namespace ember::ai::test
