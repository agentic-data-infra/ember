#include "ember/ai/AiEndpoint.h"

#include <fmt/format.h>

#include "velox/common/base/Exceptions.h"

namespace ember::ai {

namespace velox = facebook::velox;

bool changesAnswers(std::string_view key) {
  return key.starts_with(kEndpointPrefix) &&
      (key.ends_with(".kind") || key.ends_with(".url") ||
       key.ends_with(".model"));
}

AiEndpoint AiEndpoint::fromConfig(
    const std::string& name,
    const velox::core::QueryConfig& config) {
  const auto prefix = kEndpointPrefix + name + ".";
  auto get = [&](const char* field) {
    return config.get<std::string>(prefix + field);
  };

  AiEndpoint endpoint;
  endpoint.name = name;
  endpoint.kind = get("kind").value_or("http");
  VELOX_USER_CHECK_EQ(
      endpoint.kind, "http", "{}kind: the only kind is http", prefix);
  auto url = get("url");
  auto model = get("model");
  VELOX_USER_CHECK(
      url.has_value() && model.has_value(),
      "Unknown AI endpoint '{}': set {}url and {}model",
      name,
      prefix,
      prefix);
  endpoint.url = std::move(*url);
  endpoint.model = std::move(*model);
  endpoint.apiKeyEnv = get("api_key_env").value_or("");
  endpoint.maxConcurrency =
      config.get<int32_t>(prefix + "max_concurrency", endpoint.maxConcurrency);
  endpoint.qps = config.get<double>(prefix + "qps", endpoint.qps);
  VELOX_USER_CHECK_GT(
      endpoint.maxConcurrency, 0, "{}max_concurrency must be positive", prefix);
  VELOX_USER_CHECK_GE(endpoint.qps, 0, "{}qps must not be negative", prefix);
  return endpoint;
}

std::string AiEndpoint::key() const {
  return fmt::format(
      "{}\n{}\n{}\n{}\n{}\n{}\n{}",
      name,
      kind,
      url,
      model,
      apiKeyEnv,
      maxConcurrency,
      qps);
}

} // namespace ember::ai
