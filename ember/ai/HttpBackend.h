// HttpBackend: an AiBackend over an OpenAI-compatible chat completions API
// (hosted APIs, vLLM, SGLang, llama.cpp's server).
//
// Requests go through one libcurl multi handle per backend, driven by an
// event base of the process's IO pool (rpc/IoEventBases.h): no threads of
// its own. Per process and endpoint, at most max_concurrency requests are in
// flight and at most qps start per second. A request that times out, cannot
// connect, or gets 429 or 5xx is retried with exponential backoff; any other
// failure, and the last retry's, becomes the answer's error.
//
// Request and response bodies are buffers of the backend's memory pool (a
// leaf of Velox's memory manager, outside any query's arbitration, so the IO
// thread never waits for it). libcurl's and simdjson's own working buffers
// are not.

#pragma once

#include <memory>

#include "ember/ai/AiBackend.h"

namespace ember::ai {

/// A backend for an endpoint of kind http. Reads the API key from the
/// environment variable the endpoint names; throws a user error if it is set
/// but missing.
std::unique_ptr<AiBackend> makeHttpBackend(const AiEndpoint& endpoint);

} // namespace ember::ai
