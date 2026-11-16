# AI functions

Design for calling LLMs from Dataframe programs. Status: step 1 of the
plan below is implemented (`ai.generate` without `output`, `getField`; see
[../ember/ai/README.md](../ember/ai/README.md)); the rest is proposed.

Agents in the discovery loop filter, label and score large experiment
outputs. They should be able to use an LLM like any other function: in
`select`, in `filter`, inside `when`/`otherwise`, next to ordinary
expressions.

Role models: the API follows BigQuery's AI functions (`AI.GENERATE` and
friends), which are functions inside SQL expressions, not separate
operators. From Ray Data's LLM processors we take three lessons: self-hosted
models (vLLM, SGLang) belong behind an endpoint, a failed row does not fail
the job, and a rerun resumes where the last one stopped (here through the
response cache).

## API

One AI function, plus two general Dataframe additions.

| API | Meaning |
|---|---|
| `ai.generate(prompt, endpoint=, output=None)` | One request per row to the named endpoint, with `prompt` as the user message. Without `output` it returns `ROW(result VARCHAR, status VARCHAR)`. With `output`, a schema such as `{"label": "varchar", "score": "double"}`, the schema goes to the model as a JSON schema and the answer comes back typed: `ROW(label VARCHAR, score DOUBLE, status VARCHAR)`. `status` is empty on success; a failed row has null fields and the error in `status`. |
| `Column.getField(name)` | A field of a `ROW` column, e.g. `ai.generate(...).getField("result")`. |
| `when(cond, value).when(...).otherwise(value)` | A conditional expression (CASE WHEN). Velox evaluates each branch only on the rows that take it. |

`ember.ai` is the Python module, `ember::ai` the C++ namespace. `prompt` is
any `VARCHAR` expression, usually built with `concat`; there is no
templating language. A failed row never fails the task; failures are
counted in the stage statistics.

`getField` and `when`/`otherwise` are general: they are useful without any
LLM and map to existing Velox expressions (field access and `switch`).

### Example

```python
import ember
from ember import ai
from ember.functions import avg, col, concat, count, lit

session = (ember.Session.builder
    .master("local[*]")
    .config("ember.ai.endpoint.small.kind", "http")
    .config("ember.ai.endpoint.small.url", "http://localhost:8080/v1")
    .config("ember.ai.endpoint.small.model", "qwen2.5-1.5b-instruct")
    .config("ember.ai.endpoint.small.api_key_env", "LLM_API_KEY")
    .getOrCreate())

reviews = session.read.parquet("/data/reviews")   # id, product, text

judged = reviews.withColumn("judgment", ai.generate(
    concat(lit("Is this product review positive? "
               "Rate its sentiment from 0 to 1. Review: "), col("text")),
    endpoint="small",
    output={"positive": "boolean", "sentiment": "double"}))

summary = (judged
    .filter(col("judgment").getField("status") == "")
    .groupBy("product")
    .agg(avg(col("judgment").getField("sentiment")).alias("avg_sentiment"),
         count(col("id")).alias("reviews"))
    .orderBy(col("avg_sentiment").desc()))
```

BigQuery's log triage example with the same API: an LLM filter, typed
labels, and routing rows to different models:

```python
from ember.functions import when

def prompt(text):
    return concat(lit(text), col("log_entry"))

def ask(text, endpoint):
    return ai.generate(prompt(text), endpoint=endpoint).getField("result")

triaged = (logs
    .filter(ai.generate(
        prompt("Is this an error or warning, not routine INFO or cron? "),
        endpoint="hydrogen", output={"relevant": "boolean"})
        .getField("relevant"))
    .withColumn("t", ai.generate(
        prompt("Domain (one of Database, Auth / TLS, Rate Limit, Compute, "
               "Streaming) and customer-facing outage blast radius 1-10: "),
        endpoint="hydrogen",
        output={"domain": "varchar", "severity": "double"}))
    .select("service", "log_entry",
            col("t").getField("domain").alias("domain"),
            col("t").getField("severity").alias("severity")))

report = (triaged
    .select("service", "domain", "severity",
        when(col("severity") >= 8,
             ask("Plain text, <=14 words, root cause + fix: ", "carbon"))
        .otherwise(ask("Plain text, <=10 words, triage summary: ", "hydrogen"))
        .alias("diagnosis"))
    .orderBy(col("severity").desc()))
```

A row whose filter call fails has a null `relevant` and is dropped. Only
rows with `severity >= 8` reach `carbon`; the rest go to `hydrogen`.

## Why a function, not an operator

`ai.generate` is a Velox vector function. Velox's expression evaluation
already gives the semantics programs rely on:

- **Conditionals:** `when`/`otherwise` (`SwitchExpr`) evaluates each branch
  only on the rows that take it.
- **Filters:** `AND` (`ConjunctExpr`) evaluates a conjunct only on rows that
  passed the earlier ones and reorders conjuncts by measured cost, so an
  expensive AI condition runs after cheap filters.
- **Struct results:** `getField` is ordinary field access.
- **Duplicates:** common subexpression elimination calls an identical AI
  expression once per row.

Axiom plans it as an ordinary function call, so the optimizer needs no
changes. A dedicated operator would have to pull the calls out of their
expressions and reimplement the conditional masking.

The return type must be known when the plan is built. The API turns the
`output` schema into the result's `ROW` type (the schema's fields plus
`status`) and passes it as a constant argument: a typed null of that type.
The function is registered with a type variable, roughly
`ai_generate(VARCHAR prompt, VARCHAR endpoint, T output) -> T`, so Velox's
signature binding (and Axiom's type resolution, which uses the same
signatures) fixes the return type from that argument's type. Without
`output`, the argument is a null of `ROW(result VARCHAR, status VARCHAR)`.
At run time the function derives the JSON schema it sends from the same
type, and parses each answer into its fields.

Rows vs batches: a vector function receives a batch of rows (about 1000).
For `ai.generate` that is a batch of independent prompts, sent as
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
| `ember.ai.endpoint.<name>.url` | Base URL of an OpenAI-compatible API, e.g. `https://api.openai.com/v1` or a vLLM or llama.cpp server. |
| `ember.ai.endpoint.<name>.model` | Model name sent with each request. |
| `ember.ai.endpoint.<name>.api_key_env` | Environment variable that holds the API key on the driver and workers. |
| `ember.ai.endpoint.<name>.max_concurrency` | Requests in flight per process. |
| `ember.ai.endpoint.<name>.qps` | Requests per second per process. |

Session config reaches the workers with each task. API keys never do: each
process reads its key from its own environment, so keys stay out of plans,
logs and stage keys.

## Execution

- **Backend interface.** The function does not speak HTTP. It calls an
  `AiBackend` (submit requests, get futures), one per endpoint per process.
  `HttpBackend` is the first implementation.
- **HttpBackend.** OpenAI-compatible chat completions over libcurl (a new
  dependency) on folly futures, driven by Ember's existing IO executor (no
  new background threads). Structured output: when a call has an `output`
  schema, the request carries it as a JSON schema (`response_format`). Retries with backoff on 429, 5xx, timeouts and connection errors. Each
  attempt times out after 120 s; worker heartbeats run on their own thread,
  so a slow model never makes a worker look lost. `max_concurrency` and `qps` limit each process.
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
lets `ai.generate` be marked deterministic, so stage keys
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
sending images over HTTP. The API does not change; these are additions.
Three choices in the first version keep them open: the backend interface is
not HTTP-specific, endpoints have a `kind`, and inputs are typed columns
(images can later come in as `VARBINARY` or path columns).

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
| 1 | Endpoint config, `AiBackend`, `HttpBackend`, `ai.generate` without `output`, `getField`, C++ and Python bindings. Tests against a mock OpenAI-compatible server: a call, a filter, nulls, retry after 429, a failed row. |
| 2 | `output` schemas (typed `ROW` results), `when`/`otherwise`; tests that a branch sees only its rows and that answers parse into typed fields. |
| 3 | Response cache (Kiln, local fallback); mark `ai.generate` deterministic; tests for reruns and added rows. |
| 4 | Runtime statistics. |
| 5 | A manual check against a real endpoint. velox-dev has no GPU, so: llama.cpp's `llama-server` on CPU (OpenAI-compatible, JSON-schema output) with a small instruct model such as Qwen2.5-0.5B or 1.5B (GGUF), under a memory cap. |

Left out for now: convenience functions such as `classify`, `score` or
`if_` (thin helpers over `generate` with a fixed prompt and output schema,
easy to add later), adapters other than OpenAI-compatible, rate limits shared across the job,
Python callbacks for prompts or parsing, embeddings, multimodal inputs,
providers' batch APIs, and the GPU steps above.
