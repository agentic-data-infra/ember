#include "ember/ai/HttpBackend.h"

#include <curl/curl.h>
#include <simdjson.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <mutex>

#include <fmt/format.h>
#include <folly/container/F14Map.h>
#include <folly/futures/Future.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventHandler.h>

#include "ember/rpc/IoEventBases.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"

namespace ember::ai {

namespace velox = facebook::velox;

namespace {

// An attempt that takes longer fails and is retried.
constexpr std::chrono::milliseconds kAttemptTimeout{120'000};
constexpr int32_t kMaxAttempts = 4;
// Doubles with each retry.
constexpr std::chrono::milliseconds kFirstBackoff{500};
// How much of an unexpected response body an error quotes.
constexpr size_t kQuotedBytes = 300;

size_t escapedSize(std::string_view s) {
  size_t size = 0;
  for (unsigned char c : s) {
    if (c == '"' || c == '\\' || c == '\n' || c == '\r' || c == '\t') {
      size += 2;
    } else if (c < 0x20) {
      size += 6;
    } else {
      ++size;
    }
  }
  return size;
}

// Writes 's' as the contents of a JSON string; returns the end.
char* writeEscaped(std::string_view s, char* out) {
  for (unsigned char c : s) {
    switch (c) {
      case '"':
        *out++ = '\\';
        *out++ = '"';
        break;
      case '\\':
        *out++ = '\\';
        *out++ = '\\';
        break;
      case '\n':
        *out++ = '\\';
        *out++ = 'n';
        break;
      case '\r':
        *out++ = '\\';
        *out++ = 'r';
        break;
      case '\t':
        *out++ = '\\';
        *out++ = 't';
        break;
      default:
        if (c < 0x20) {
          out = fmt::format_to(out, "\\u{:04x}", c);
        } else {
          *out++ = static_cast<char>(c);
        }
    }
  }
  return out;
}

char* write(std::string_view s, char* out) {
  std::memcpy(out, s.data(), s.size());
  return out + s.size();
}

// {"model":"<model>","messages":[{"role":"user","content":"<prompt>"}]}
velox::BufferPtr requestBody(
    std::string_view model,
    std::string_view prompt,
    velox::memory::MemoryPool& pool) {
  constexpr std::string_view kModel = R"({"model":")";
  constexpr std::string_view kMessages =
      R"(","messages":[{"role":"user","content":")";
  constexpr std::string_view kEnd = R"("}]})";
  const auto size = kModel.size() + escapedSize(model) + kMessages.size() +
      escapedSize(prompt) + kEnd.size();
  auto body = velox::AlignedBuffer::allocate<char>(size, &pool);
  char* out = body->asMutable<char>();
  out = write(kModel, out);
  out = writeEscaped(model, out);
  out = write(kMessages, out);
  out = writeEscaped(prompt, out);
  out = write(kEnd, out);
  VELOX_CHECK_EQ(out - body->as<char>(), size);
  return body;
}

// One request, through its attempts.
struct Transfer {
  velox::BufferPtr request;
  // The response body so far: 'received' bytes, with room for simdjson's
  // padding behind them.
  velox::BufferPtr response;
  size_t received{0};
  int32_t attempts{0};
  folly::Promise<AiAnswer> promise;
  std::unique_ptr<CURL, decltype(&curl_easy_cleanup)> easy{
      nullptr,
      curl_easy_cleanup};
  char curlError[CURL_ERROR_SIZE]{};

  std::string_view body() const {
    return response == nullptr
        ? std::string_view()
        : std::string_view(response->as<char>(), received);
  }
};

bool retryable(CURLcode code) {
  switch (code) {
    case CURLE_OPERATION_TIMEDOUT:
    case CURLE_COULDNT_CONNECT:
    case CURLE_SEND_ERROR:
    case CURLE_RECV_ERROR:
    case CURLE_GOT_NOTHING:
    case CURLE_PARTIAL_FILE:
      return true;
    default:
      return false;
  }
}

std::string authorization(const AiEndpoint& endpoint) {
  if (endpoint.apiKeyEnv.empty()) {
    return "";
  }
  const char* key = std::getenv(endpoint.apiKeyEnv.c_str());
  VELOX_USER_CHECK(
      key != nullptr,
      "AI endpoint '{}': environment variable {} is not set",
      endpoint.name,
      endpoint.apiKeyEnv);
  return fmt::format("Authorization: Bearer {}", key);
}

// The libcurl multi handle and its requests. Everything but submit() runs on
// the event base thread. The AsyncTimeout is libcurl's timer.
class Client : public folly::AsyncTimeout {
 public:
  Client(const AiEndpoint& endpoint, folly::EventBase& eventBase)
      : folly::AsyncTimeout(&eventBase),
        eventBase_{eventBase},
        endpoint_{endpoint},
        url_{fmt::format(
            "{}/chat/completions",
            std::string_view(endpoint.url).substr(
                0, endpoint.url.find_last_not_of('/') + 1))},
        pool_{velox::memory::memoryManager()->addLeafPool(fmt::format(
            "ember_ai_{}_{}", endpoint.name, ++poolCounter))} {
    static std::once_flag once;
    std::call_once(once, [] { curl_global_init(CURL_GLOBAL_DEFAULT); });
    curl_slist* headers =
        curl_slist_append(nullptr, "Content-Type: application/json");
    if (auto header = authorization(endpoint); !header.empty()) {
      headers = curl_slist_append(headers, header.c_str());
    }
    headers_.reset(headers);
    multi_.reset(curl_multi_init());
    VELOX_CHECK_NOT_NULL(multi_.get());
    curl_multi_setopt(multi_.get(), CURLMOPT_SOCKETFUNCTION, &Client::onSocket);
    curl_multi_setopt(multi_.get(), CURLMOPT_SOCKETDATA, this);
    curl_multi_setopt(multi_.get(), CURLMOPT_TIMERFUNCTION, &Client::onTimer);
    curl_multi_setopt(multi_.get(), CURLMOPT_TIMERDATA, this);
  }

  const AiEndpoint& endpoint() const {
    return endpoint_;
  }

  velox::memory::MemoryPool& pool() const {
    return *pool_;
  }

  void submit(std::unique_ptr<Transfer> transfer) {
    eventBase_.runInEventBaseThread(
        [this, transfer = std::move(transfer)]() mutable {
          queue_.push_back(std::move(transfer));
          pump();
        });
  }

 private:
  // A socket libcurl asked to watch.
  class Socket : public folly::EventHandler {
   public:
    Socket(Client& client, curl_socket_t fd)
        : folly::EventHandler(
              &client.eventBase_, folly::NetworkSocket::fromFd(fd)),
          client_{client},
          fd_{fd} {}

    void handlerReady(uint16_t events) noexcept override {
      int flags = 0;
      if (events & READ) {
        flags |= CURL_CSELECT_IN;
      }
      if (events & WRITE) {
        flags |= CURL_CSELECT_OUT;
      }
      client_.socketAction(fd_, flags);
    }

   private:
    Client& client_;
    const curl_socket_t fd_;
  };

  // libcurl's callbacks get the client as a void pointer.
  static int onSocket(
      CURL* /*easy*/,
      curl_socket_t fd,
      int what,
      void* client,
      void* /*socket*/) {
    static_cast<Client*>(client)->watch(fd, what);
    return 0;
  }

  static int onTimer(CURLM* /*multi*/, long timeoutMs, void* client) {
    auto* self = static_cast<Client*>(client);
    if (timeoutMs < 0) {
      self->cancelTimeout();
    } else {
      self->scheduleTimeout(static_cast<uint32_t>(timeoutMs));
    }
    return 0;
  }

  static size_t onData(char* data, size_t size, size_t count, void* transfer) {
    auto& t = *static_cast<Transfer*>(transfer);
    const auto bytes = size * count;
    const auto needed = t.received + bytes + simdjson::SIMDJSON_PADDING;
    if (t.response == nullptr) {
      t.response = velox::AlignedBuffer::allocate<char>(
          std::max<size_t>(needed, 4096), t.request->pool());
    } else if (t.response->capacity() < needed) {
      velox::AlignedBuffer::reallocate<char>(
          &t.response, std::max(needed, 2 * t.response->capacity()));
    }
    std::memcpy(t.response->asMutable<char>() + t.received, data, bytes);
    t.received += bytes;
    return bytes;
  }

  void timeoutExpired() noexcept override {
    socketAction(CURL_SOCKET_TIMEOUT, 0);
  }

  void watch(curl_socket_t fd, int what) {
    if (what == CURL_POLL_REMOVE) {
      auto it = sockets_.find(fd);
      if (it != sockets_.end()) {
        it->second->unregisterHandler();
        // Not while its handlerReady() may be on the stack.
        eventBase_.runInLoop([socket = std::move(it->second)] {});
        sockets_.erase(it);
      }
      return;
    }
    auto& socket = sockets_[fd];
    if (socket == nullptr) {
      socket = std::make_unique<Socket>(*this, fd);
    }
    uint16_t events = folly::EventHandler::PERSIST;
    if (what & CURL_POLL_IN) {
      events |= folly::EventHandler::READ;
    }
    if (what & CURL_POLL_OUT) {
      events |= folly::EventHandler::WRITE;
    }
    socket->unregisterHandler();
    socket->registerHandler(events);
  }

  void socketAction(curl_socket_t fd, int flags) {
    int running = 0;
    curl_multi_socket_action(multi_.get(), fd, flags, &running);
    int left = 0;
    while (CURLMsg* message = curl_multi_info_read(multi_.get(), &left)) {
      if (message->msg == CURLMSG_DONE) {
        finished(message->easy_handle, message->data.result);
      }
    }
    pump();
  }

  // Starts queued requests while concurrency and qps allow.
  void pump() {
    while (running_.size() < static_cast<size_t>(endpoint_.maxConcurrency) &&
           !queue_.empty()) {
      if (endpoint_.qps > 0) {
        const auto now = std::chrono::steady_clock::now();
        if (now < nextStart_) {
          if (!pumpScheduled_) {
            pumpScheduled_ = true;
            eventBase_.runAfterDelay(
                [this] {
                  pumpScheduled_ = false;
                  pump();
                },
                static_cast<uint32_t>(
                    std::chrono::ceil<std::chrono::milliseconds>(
                        nextStart_ - now)
                        .count()));
          }
          return;
        }
        nextStart_ = now +
            std::chrono::duration_cast<std::chrono::steady_clock::duration>(
                         std::chrono::duration<double>(1 / endpoint_.qps));
      }
      auto transfer = std::move(queue_.front());
      queue_.pop_front();
      start(std::move(transfer));
    }
  }

  void start(std::unique_ptr<Transfer> transfer) {
    ++transfer->attempts;
    transfer->received = 0;
    if (transfer->easy == nullptr) {
      transfer->easy.reset(curl_easy_init());
      CURL* easy = transfer->easy.get();
      curl_easy_setopt(easy, CURLOPT_URL, url_.c_str());
      curl_easy_setopt(easy, CURLOPT_HTTPHEADER, headers_.get());
      curl_easy_setopt(
          easy, CURLOPT_POSTFIELDS, transfer->request->as<char>());
      curl_easy_setopt(
          easy,
          CURLOPT_POSTFIELDSIZE_LARGE,
          static_cast<curl_off_t>(transfer->request->size()));
      curl_easy_setopt(easy, CURLOPT_WRITEFUNCTION, &Client::onData);
      curl_easy_setopt(easy, CURLOPT_WRITEDATA, transfer.get());
      curl_easy_setopt(easy, CURLOPT_ERRORBUFFER, transfer->curlError);
      curl_easy_setopt(easy, CURLOPT_TIMEOUT_MS, kAttemptTimeout.count());
      curl_easy_setopt(easy, CURLOPT_NOSIGNAL, 1L);
    }
    CURL* easy = transfer->easy.get();
    auto& running = running_[easy];
    running = std::move(transfer);
    if (auto code = curl_multi_add_handle(multi_.get(), easy);
        code != CURLM_OK) {
      auto failed = std::move(running);
      running_.erase(easy);
      complete(*failed, {nullptr, curl_multi_strerror(code)});
    }
  }

  void finished(CURL* easy, CURLcode code) {
    curl_multi_remove_handle(multi_.get(), easy);
    auto it = running_.find(easy);
    auto transfer = std::move(it->second);
    running_.erase(it);

    long status = 0;
    curl_easy_getinfo(easy, CURLINFO_RESPONSE_CODE, &status);
    AiAnswer answer;
    bool retry = false;
    if (code != CURLE_OK) {
      retry = retryable(code);
      answer.error = transfer->curlError[0] != '\0'
          ? std::string(transfer->curlError)
          : std::string(curl_easy_strerror(code));
    } else if (status == 200) {
      answer = parse(*transfer);
    } else {
      retry = status == 429 || status >= 500;
      answer.error = fmt::format(
          "HTTP {}: {}", status, transfer->body().substr(0, kQuotedBytes));
    }

    if (retry && transfer->attempts < kMaxAttempts) {
      const auto backoff = kFirstBackoff * (1 << (transfer->attempts - 1));
      eventBase_.runAfterDelay(
          [this, transfer = std::move(transfer)]() mutable {
            queue_.push_front(std::move(transfer));
            pump();
          },
          static_cast<uint32_t>(backoff.count()));
      return;
    }
    complete(*transfer, std::move(answer));
  }

  // The message content of a chat completion.
  AiAnswer parse(Transfer& transfer) {
    const auto body = transfer.body();
    std::string_view content;
    if (transfer.response != nullptr) {
      simdjson::padded_string_view json(
          body.data(), body.size(), transfer.response->capacity());
      simdjson::ondemand::document document;
      if (!parser_.iterate(json).get(document) &&
          !document.at_pointer("/choices/0/message/content")
               .get_string()
               .get(content)) {
        auto text = velox::AlignedBuffer::allocate<char>(
            content.size(), pool_.get());
        std::memcpy(text->asMutable<char>(), content.data(), content.size());
        return {std::move(text), ""};
      }
    }
    return {
        nullptr,
        fmt::format("Unexpected response: {}", body.substr(0, kQuotedBytes))};
  }

  void complete(Transfer& transfer, AiAnswer answer) {
    transfer.promise.setValue(std::move(answer));
  }

  static inline std::atomic<int64_t> poolCounter{0};

  folly::EventBase& eventBase_;
  const AiEndpoint endpoint_;
  const std::string url_;
  const std::shared_ptr<velox::memory::MemoryPool> pool_;
  std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers_{
      nullptr,
      curl_slist_free_all};
  std::unique_ptr<CURLM, decltype(&curl_multi_cleanup)> multi_{
      nullptr,
      curl_multi_cleanup};
  folly::F14FastMap<curl_socket_t, std::unique_ptr<Socket>> sockets_;
  std::deque<std::unique_ptr<Transfer>> queue_;
  folly::F14FastMap<CURL*, std::unique_ptr<Transfer>> running_;
  std::chrono::steady_clock::time_point nextStart_;
  bool pumpScheduled_{false};
  simdjson::ondemand::parser parser_;
};

class HttpBackend : public AiBackend {
 public:
  explicit HttpBackend(const AiEndpoint& endpoint)
      : client_{std::make_unique<Client>(endpoint, *rpc::nextIoEventBase())} {}

  folly::coro::Task<AiAnswer> generate(std::string_view prompt) override {
    auto transfer = std::make_unique<Transfer>();
    transfer->request =
        requestBody(client_->endpoint().model, prompt, client_->pool());
    auto answer = transfer->promise.getSemiFuture();
    client_->submit(std::move(transfer));
    co_return co_await std::move(answer);
  }

 private:
  const std::unique_ptr<Client> client_;
};

} // namespace

std::unique_ptr<AiBackend> makeHttpBackend(const AiEndpoint& endpoint) {
  return std::make_unique<HttpBackend>(endpoint);
}

} // namespace ember::ai
