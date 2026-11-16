# ember/ai

The AI functions: Velox functions that call an LLM per row. Design and plan:
[../../docs/ai-functions.md](../../docs/ai-functions.md). Programs use them
through `ember::ai::generate` (`ember/ai.h`) or `ember.ai.generate` in
Python.

| File | Contents |
|------|----------|
| `AiFunctions` | `registerAiFunctions()`: `ai_generate(prompt, endpoint)` returns `ROW(result VARCHAR, status VARCHAR)`; `ai_generate(prompt, endpoint, output)`, where `output` is a typed null of `resultType(schema)`, returns that type, parsed from the model's JSON answer with Velox's `try_cast` from JSON. A batch's distinct prompts go out concurrently; a failed row, or an answer that does not fit, has null fields and the error in `status`. Registered by `initializeRuntime()`. |
| `OutputSchema` | Output schemas: `resultType(schema)` (the fields plus `status`), `schemaOf(result)`, and `jsonSchema(schema)`, sent as the request's `response_format`. Fields may be boolean, integers, real, double, varchar, array or row. |
| `AiEndpoint` | An endpoint from the session config: `ember.ai.endpoint.<name>.{kind, url, model, api_key_env, max_concurrency, qps}`. Plans carry only the name. |
| `AiBackend` | What the functions call: `generate(prompt, schema)` as a `folly::coro::Task`; a null schema asks for free text. `backendFor(endpoint)` keeps one backend per endpoint for the life of the process. |
| `HttpBackend` | OpenAI-compatible chat completions over a libcurl multi handle on an event base of the process's IO pool (`rpc/IoEventBases.h`). Limits concurrency and qps per process; retries timeouts, connection errors, 429 and 5xx up to 4 attempts with exponential backoff from 500 ms; each attempt times out after 120 s. Request and response bodies come from the backend's memory pool. |

The driver thread that evaluates `ai_generate` waits for the batch's answers,
like the sinks in `common/README.md` ("Waiting on driver threads"). Worker
heartbeats run on their own thread, so a slow model does not make a worker
look lost.

Tests (`tests/`) run the function through the Dataframe API against
`MockChatServer`, a minimal chat completions server.
