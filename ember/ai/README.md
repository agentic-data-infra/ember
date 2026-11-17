# ember/ai

The AI functions: Velox async RPC functions that call an LLM per row. Design and plan:
[../../docs/ai-functions.md](../../docs/ai-functions.md). Programs use them
through `ember::ai::generate` (`ember/ai.h`) or `ember.ai.generate` in
Python.

| File | Contents |
|------|----------|
| `AiFunctions` | `registerAiFunctions()`: `ai_generate(prompt, endpoint)` returns `ROW(result VARCHAR, status VARCHAR)`; `ai_generate(prompt, endpoint, output)`, where `output` is a typed null of `resultType(schema)`, returns that type, parsed from the model's JSON answer with Velox's `try_cast` from JSON. It runs in Velox's RPC operator, which Axiom's optimizer plans for each call: one request per distinct prompt of an input, rows held until their answers arrive. A failed row, or an answer that does not fit, has null fields and the error in `status`. Deterministic, so stages that call it are reused like others. Requests in flight are limited per endpoint (`max_concurrency`), and a 429 or timeout makes the operator back off. Runtime stats `kStats`: `aiCalls`, `aiRetries`, `aiFailedRows`, `aiInputTokens`, `aiOutputTokens`, which tasks report to the driver; it does not keep stages with failed rows (see the design doc). Registered by `initializeRuntime()`. |
| `OutputSchema` | Output schemas: `resultType(schema)` (the fields plus `status`), `schemaOf(result)`, and `jsonSchema(schema)`, sent as the request's `response_format`. Fields may be boolean, integers, real, double, varchar, array or row. |
| `AiEndpoint` | An endpoint from the session config: `ember.ai.endpoint.<name>.{kind, url, model, api_key_env, max_concurrency, qps}`. Plans carry only the name; `changesAnswers(key)` puts `kind`, `url` and `model` into stage keys. |
| `AiBackend` | What the functions call: `generate(prompt, schema)` returns a `folly::SemiFuture`; a null schema asks for free text. An answer carries its attempts, token usage and whether a failure was overload. `backendFor(endpoint)` keeps one backend per endpoint for the life of the process. |
| `HttpBackend` | OpenAI-compatible chat completions over a libcurl multi handle on an event base of the process's IO pool (`rpc/IoEventBases.h`). Limits concurrency and qps per process; retries timeouts, connection errors, 429 and 5xx up to 4 attempts with exponential backoff from 500 ms; each attempt times out after 120 s. Request and response bodies come from the backend's memory pool. |

No driver thread waits for an answer. Worker heartbeats run on their own
thread, so a slow model does not make a worker look lost. Calls under
conditions and filters around them are handled by `ember/api/RpcCalls.h`.

Tests (`tests/`) run the function through the Dataframe API against
`MockChatServer`, a minimal chat completions server;
`ember/driver/tests/ClusterTest.cpp` runs it on worker processes.
