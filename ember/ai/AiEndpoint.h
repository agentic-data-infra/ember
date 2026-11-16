// An AI endpoint: where ai.generate sends its requests. Endpoints are named
// in the session config, which is the only way to configure one:
//
//   ember.ai.endpoint.<name>.kind             http (the default and only kind)
//   ember.ai.endpoint.<name>.url              base URL of an OpenAI-compatible
//                                             API, e.g. http://localhost:8080/v1
//   ember.ai.endpoint.<name>.model            model name sent with each request
//   ember.ai.endpoint.<name>.api_key_env      environment variable with the API
//                                             key (optional: no key)
//   ember.ai.endpoint.<name>.max_concurrency  requests in flight per process
//                                             (default 16)
//   ember.ai.endpoint.<name>.qps              requests per second per process
//                                             (default 0: no limit)
//
// Plans carry only the name; every process resolves it from the session
// config its tasks run with, and reads the API key from its own environment.
// Stage keys include kind, url and model (changesAnswers), so changing the
// model invalidates reuse of stages that call it.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "velox/core/QueryConfig.h"

namespace ember::ai {

inline constexpr const char* kEndpointPrefix = "ember.ai.endpoint.";

/// Whether session config 'key' can change an endpoint's answers: its kind,
/// url or model. The API key, concurrency and qps cannot.
bool changesAnswers(std::string_view key);

struct AiEndpoint {
  std::string name;
  std::string kind;
  std::string url;
  std::string model;
  std::string apiKeyEnv;
  int32_t maxConcurrency{16};
  double qps{0};

  /// The endpoint 'name' as 'config' defines it. Throws a user error when
  /// the endpoint or its url or model is missing, or a value is invalid.
  static AiEndpoint fromConfig(
      const std::string& name,
      const facebook::velox::core::QueryConfig& config);

  /// Identifies the endpoint's whole configuration, so that two sessions that
  /// give one name different settings get different backends.
  std::string key() const;
};

} // namespace ember::ai
