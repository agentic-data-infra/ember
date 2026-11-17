// AiBackend: what the AI functions call. A backend sends one prompt at a time
// and answers asynchronously; the RPC operator that runs the functions keeps
// many prompts in flight and never waits on one. HttpBackend, an
// OpenAI-compatible HTTP client, is the only backend so far.

#pragma once

#include <string>
#include <string_view>

#include <folly/futures/Future.h>

#include "ember/ai/AiEndpoint.h"
#include "velox/buffer/Buffer.h"
#include "velox/type/Type.h"

namespace ember::ai {

/// One answer: its text, or why there is none.
struct AiAnswer {
  /// The answer's text (its size is the buffer's size), from the backend's
  /// memory pool. Null when the call failed.
  facebook::velox::BufferPtr text;
  /// Why the call failed, after retries; empty on success.
  std::string error;
  /// Whether the call failed because the endpoint was overloaded: a 429,
  /// or a timeout.
  bool overloaded{false};
  /// Requests sent for it, retries included.
  int32_t attempts{0};
  /// Tokens of the prompt and the answer, as the endpoint reports them (0 if
  /// it does not).
  int64_t inputTokens{0};
  int64_t outputTokens{0};
};

class AiBackend {
 public:
  virtual ~AiBackend() = default;

  /// Sends 'prompt' as a user message and returns the model's answer. With
  /// a 'schema' (OutputSchema.h), the model is asked to answer with a JSON
  /// object of that schema; without one (null), in free text. The prompt is
  /// copied before this returns. Never fails for a failed call: the answer
  /// carries the error.
  virtual folly::SemiFuture<AiAnswer> generate(
      std::string_view prompt,
      facebook::velox::RowTypePtr schema) = 0;
};

/// The process's backend for 'endpoint', created on first use. Backends live
/// as long as the process, like its IO threads, so that requests in flight
/// and limits per process span tasks.
std::shared_ptr<AiBackend> backendFor(const AiEndpoint& endpoint);

} // namespace ember::ai
