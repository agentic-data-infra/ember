// AiBackend: what the AI functions call. A backend sends one prompt at a time
// and answers asynchronously; the functions send a batch's prompts
// concurrently and wait for all of them. HttpBackend, an OpenAI-compatible
// HTTP client, is the only backend so far.

#pragma once

#include <string>
#include <string_view>

#include <folly/coro/Task.h>

#include "ember/ai/AiEndpoint.h"
#include "velox/buffer/Buffer.h"

namespace ember::ai {

/// One answer: its text, or why there is none.
struct AiAnswer {
  /// The answer's text (its size is the buffer's size), from the backend's
  /// memory pool. Null when the call failed.
  facebook::velox::BufferPtr text;
  /// Why the call failed, after retries; empty on success.
  std::string error;
};

class AiBackend {
 public:
  virtual ~AiBackend() = default;

  /// Sends 'prompt' as a user message and returns the model's answer. Never
  /// throws for a failed call: the answer carries the error.
  virtual folly::coro::Task<AiAnswer> generate(std::string_view prompt) = 0;
};

/// The process's backend for 'endpoint', created on first use. Backends live
/// as long as the process, like its IO threads, so that requests in flight
/// and limits per process span tasks.
AiBackend& backendFor(const AiEndpoint& endpoint);

} // namespace ember::ai
