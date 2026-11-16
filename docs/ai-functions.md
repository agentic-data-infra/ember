# AI functions

Design for calling LLMs from Dataframe programs. Status: proposal; nothing
here is implemented yet.

Agents in the discovery loop filter, label and score large experiment
outputs. They should be able to use an LLM like any other function: in
`select`, in `filter`, inside `when`/`otherwise`, next to ordinary
expressions.

Role models: the API follows BigQuery's `AI.GENERATE`, `AI.CLASSIFY`,
`AI.SCORE` and `AI.IF`, which are functions inside SQL expressions, not
separate operators. From Ray Data's LLM processors we take three lessons:
self-hosted models (vLLM, SGLang) belong behind an endpoint, a failed row
does not fail the job, and a rerun resumes where the last one stopped (here
through the response cache).

## API

`ember.ai` (Python) and `ember::ai` (C++) add four column functions. Each
row is one prompt and one request; the result is typed.

| Function | Returns | Meaning |
|---|---|---|
| `ai.generate(prompt, endpoint=)` | `ROW(result VARCHAR, status VARCHAR)` | Free text. `status` is empty on success, the error otherwise. |
| `ai.classify(text, categories, endpoint=)` | `VARCHAR` | One of `categories`. |
| `ai.score(prompt, endpoint=)` | `DOUBLE` | A number, e.g. a rating on the scale the prompt asks for. |
| `ai.if_(prompt, endpoint=)` | `BOOLEAN` | Whether the prompt's question holds. |

`prompt` and `text` are any `VARCHAR` expressions, usually built with
`concat`; there is no templating language. A row
whose call fails after retries gets `status` set (`generate`) or null (the
others); the task does not fail. Failures are counted in the stage
statistics.

BigQuery's log triage example in Ember:

```python
from ember import ai
from ember.functions import col, concat, lit, when

def prompt(text):
    return concat(lit(text), col("log_entry"))

triaged = (logs
    .filter(ai.if_(prompt("Error or warning, not routine INFO or cron: "),
                   endpoint="hydrogen"))
    .select("service", "log_entry",
        ai.classify(col("log_entry"),
                    ["Database", "Auth / TLS", "Rate Limit", "Compute", "Streaming"],
                    endpoint="hydrogen").alias("domain"),
        ai.score(prompt("Score customer-facing outage blast radius 1-10: "),
                 endpoint="hydrogen").alias("severity")))

report = (triaged
    .select("service", "domain", "severity",
        when(col("severity") >= 8,
             ai.generate(prompt("Plain text, <=14 words, root cause + fix: "),
                         endpoint="carbon").getField("result"))
        .otherwise(
             ai.generate(prompt("Plain text, <=10 words, triage summary: "),
                         endpoint="hydrogen").getField("result"))
        .alias("diagnosis"))
    .orderBy(col("severity").desc()))
```

The Dataframe API gains `when`/`otherwise` and `Column.getField` for this;
both map to existing Velox expressions (`switch`, field access).

## Why functions, not an operator

The AI functions are Velox vector functions. Velox's expression evaluation
already gives the semantics the example relies on:

- **Conditionals:** `when`/`otherwise` (`SwitchExpr`) evaluates each branch
  only on the rows that take it, so `carbon` sees only rows with
  `severity >= 8`.
- **Filters:** `AND` (`ConjunctExpr`) evaluates a conjunct only on rows that
  passed the earlier ones and reorders conjuncts by measured cost, so an
  expensive `ai.if_` runs after cheap filters.
- **Struct results:** `getField("result")` is ordinary field access.
- **Duplicates:** common subexpression elimination calls an identical AI
  expression once per row.

Axiom plans them as ordinary function calls, so the optimizer needs no
changes. A dedicated operator would have to pull the calls out of their
expressions and reimplement the conditional masking.

Rows vs batches: a vector function receives a batch of rows (about 1000).
For an AI function that is a batch of independent prompts, sent as
concurrent requests. Rows are never packed into one prompt; GPU batching is
the inference server's job, and Ember's job is to keep enough requests in
flight. A worker with 16 driver threads and 64 requests in flight per batch
has about 1000 concurrent calls, more than a provider's rate limit allows,
so blocking a driver thread for a batch does not limit throughput.

## Endpoints

An endpoint is a name in the session config; the plan carries only the
name. This is the one way to configure an endpoint:

| Key | Meaning |
|---|---|
| `ember.ai.endpoint.<name>.kind` | Backend: `http` (the only kind at first). |
| `ember.ai.endpoint.<name>.url` | Base URL of an OpenAI-compatible API, e.g. `https://api.openai.com/v1` or a vLLM server. |
| `ember.ai.endpoint.<name>.model` | Model name sent with each request. |
| `ember.ai.endpoint.<name>.api_key_env` | Environment variable that holds the API key on the driver and workers. |
| `ember.ai.endpoint.<name>.max_concurrency` | Requests in flight per process. |
| `ember.ai.endpoint.<name>.qps` | Requests per second per process. |

Session config reaches the workers with each task. API keys never do: each
process reads its key from its own environment, so keys stay out of plans,
logs and stage keys.

## Execution

- **Backend interface.** The functions do not speak HTTP. They call an
  `AiBackend` (submit requests, get futures), one per endpoint per process.
  `HttpBackend` is the first implementation.
- **HttpBackend.** OpenAI-compatible chat completions over libcurl (a new
  dependency) on folly futures, driven by Ember's existing IO executor (no
  new background threads). Structured output through a JSON schema
  (`classify` sends an enum of the categories, `score` a number, `if_` a
  boolean). Retries with backoff on 429 and 5xx. A per-request timeout below
  `ember.cluster.heartbeat_timeout_ms`, so a slow model never looks like a
  hung worker. `max_concurrency` and `qps` limit each process.
- **Per batch.** The function takes the rows Velox selects, drops duplicate
  prompts, looks them up in the cache, sends the misses concurrently, waits,
  and fills the result vector.

## Cache and reuse

Responses are cached by a hash of (model, parameters, output schema,
prompt). The cache lives in Kiln when the session has Kiln metadata
(`ember.kiln.metadata`), shared by workers and sessions, and in
`ember.local.dir` otherwise. Only successful answers are cached.

The first answer for a prompt becomes the answer. That makes task retries
and reruns consistent and free: a rerun pays only for prompts that never got
an answer, and an agent that adds rows pays only for the new ones. It also
lets the AI functions be marked deterministic, so stage keys
([../ember/driver/StageKey.h](../ember/driver/StageKey.h)) and stage reuse
work as for any other function. The endpoint's model and parameters go into
the stage keys like other config that changes results.

## Statistics

Calls, cache hits, failures, and input and output tokens are Velox runtime
stats of the operator that evaluates the expression. They show up in the
stage statistics and in Python, so an agent can read what an experiment
cost.

## Later: models next to the data

Some workloads want the model on the same GPUs as the job, e.g. to avoid
sending images over HTTP. The API does not change; these are additions. Three choices in the first
version keep them open: the backend interface is not HTTP-specific,
endpoints have a `kind`, and inputs are typed columns (images can later come
in as `VARBINARY` or path columns).

1. **Node-local server.** vLLM or SGLang on each node's GPUs; an endpoint of
   kind `local` resolves on each worker to its node's server. Large inputs
   travel as local paths or Kiln references, not bytes.
2. **In-process engine.** An `AiBackend` that runs an inference engine
   inside the worker. Driver threads submit into one queue per process and
   the engine batches across them. Needs GPU-aware scheduling, models that
   stay loaded across tasks, and GPU memory shared with Velox.
3. **AI stages on GPU workers.** The planner gives a projection or filter
   that contains AI calls its own stage, placed on GPU workers, so the GPU
   and CPU parts scale independently. The whole expression moves, so
   conditional masking still happens in Velox.

## Plan

| Step | Contents |
|---|---|
| 1 | Endpoint config, `AiBackend`, `HttpBackend`, `ai.generate`, `getField`, C++ and Python bindings. Tests against a mock OpenAI-compatible server: a call, a filter, nulls, retry after 429, a failed row. |
| 2 | `ai.classify`, `ai.score`, `ai.if_`, `when`/`otherwise`; tests that a branch sees only its rows; the triage example as a test. |
| 3 | Response cache (Kiln, local fallback); mark the functions deterministic; tests for reruns and added rows. |
| 4 | Runtime statistics. |
| 5 | A manual check against a real endpoint. velox-dev has no GPU, so: llama.cpp's `llama-server` on CPU (OpenAI-compatible, JSON-schema output) with a small instruct model such as Qwen2.5-0.5B or 1.5B (GGUF), under a memory cap. |

Left out for now: adapters other than OpenAI-compatible, rate limits shared
across the job, Python callbacks for prompts or parsing, embeddings,
multimodal inputs, providers' batch APIs, and the GPU steps above.
