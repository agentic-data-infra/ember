# AI functions

Design for calling LLMs from Dataframe programs. Status: steps 1 to 7 of
the plan below are implemented (`ai.generate` with and without `output`,
`getField`, `when`/`otherwise`, stage reuse, statistics, and since step 7
Velox's async RPC path; see [../ember/ai/README.md](../ember/ai/README.md)
and [Execution on the async RPC path](#execution-on-the-async-rpc-path)).

Agents in the discovery loop filter, label and score large experiment
outputs. They should be able to use an LLM like any other function: in
`select`, in `filter`, inside `when`/`otherwise`, next to ordinary
expressions.

Role models: the API follows BigQuery's AI functions (`AI.GENERATE` and
friends), which are functions inside SQL expressions, not separate
operators. From Ray Data's LLM processors we take three lessons: self-hosted
models (vLLM, SGLang) belong behind an endpoint, a failed row does not fail
the job, and a rerun does not pay again for work that is done (here through
stage reuse).

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

In steps 1 to 5, `ai.generate` was a Velox vector function. Velox's
expression evaluation gives the semantics programs rely on:

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

That operator exists upstream, and since step 7 `ai.generate` runs on it.
The API stays a function: the planner pulls the calls out, and Ember keeps
the masking with a rewrite of its own (see
[Execution on the async RPC path](#execution-on-the-async-rpc-path)).

The return type must be known when the plan is built. The API turns the
`output` schema into the result's `ROW` type (the schema's fields plus
`status`) and passes it as a constant argument: a typed null of that type.
The function is registered with a type variable, roughly
`ai_generate(VARCHAR prompt, VARCHAR endpoint, T output) -> T`, so Velox's
signature binding (and Axiom's type resolution, which uses the same
signatures) fixes the return type from that argument's type. Without
`output` the call has two arguments and a second signature,
`ai_generate(VARCHAR, VARCHAR) -> ROW(result VARCHAR, status VARCHAR)`, so a
schema with a single `result` field still means a JSON answer. The function
does not use Velox's default null handling: the null argument would null
every row. At run time it derives the JSON schema it sends from the same
type, and parses the answers with Velox's cast from JSON to `ROW`
(`try_cast`, fields matched by name), so there is no parser of its own; an
answer the cast rejects gets null fields and a status.

Rows vs batches: each row is an independent prompt, sent as its own
request. Rows are never packed into one prompt; GPU batching is the
inference server's job, and Ember's job is to keep enough requests in
flight. No driver thread waits for an answer: the RPC operator holds rows
until their answers arrive, and the thread runs other work meanwhile.

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
- **Per input.** The RPC operator hands the function its input rows. The
  function sends one request per distinct prompt and returns a future per
  row; the operator emits rows as their answers arrive.
- **Planning.** Axiom's optimizer lifts each call into an RPC plan node and
  never copies it, so a filter on an answer does not call the model twice.
  Ember's rewrite (below) keeps the calls on the rows that reach them.

## Execution on the async RPC path

Velox and Axiom have a second way to call out of a query, built for model
inference: an `AsyncRPCFunction` (`velox/expression/rpc`) runs in its own
operator, `RPCOperator` (`velox/exec/rpc`). Axiom's v2 optimizer, which
Ember uses, lifts each such call out of its expression into an RPC plan
node. The operator sends requests without blocking the driver thread and
holds rows until their answers arrive. It also limits requests in flight
per backend and backs off when the backend sheds load. Velox's design
([async-rpc.md](https://github.com/facebookincubator/velox/blob/main/velox/docs/designs/async-rpc.md))
states the problem as LLM inference, embeddings and vector search, and
Meta's own LLM functions run on it. The vector function of steps 1 to 5 is
the other upstream pattern (`RemoteVectorFunction`, used for REST UDFs):
it waits inside the expression. Ember moves to the path built for LLMs, so
it follows where upstream goes rather than fighting it.

### What a spike showed

A throwaway echo `AsyncRPCFunction`, registered in Ember and run in
`local[2]` with no change to Velox or Axiom:

- The plan gets an RPC node in the map stage, and a filter, a `groupBy` and
  nested calls all work. Plan fragments go through serialization even
  locally, so the RPC node already survives the trip to a worker.
- The stage gets a stage key, and a rerun makes no calls.
- A filter on the answer calls once per row: no copy into the scan, so
  Axiom's lifting does what `ai_generate`'s Axiom metadata does today.
- The result type can come from the typed-null `output` argument: the
  function reads it in `initialize()`, which the operator calls before it
  checks the result type.
- **Not supported:** a call inside `when`/`otherwise` (also `if`,
  `coalesce` and `try`) is rejected: "Inference function is not supported
  under SWITCH". And in `filter(cheap & ai)` the call runs on every row,
  not only on rows where `cheap` holds: the RPC node goes below the whole
  filter, and even on Parquet nothing of the filter is pushed into the
  scan. As two filters, the cheap one goes below the RPC node and the call
  runs only on its rows. Before, the scan took `cheap` and Velox's `AND`
  skipped the rest.

### Design

1. **`ai_generate` is an `AsyncRPCFunction`**, registered in Velox's async
   RPC function registry with the same two signatures; the vector function
   and its Axiom metadata are gone. `HttpBackend` returns a future per
   prompt, and `dispatchPerRow` returns one per distinct prompt (duplicate
   rows share it). `buildOutput` builds the same `ROW(..., status)` as
   before. It gets no evaluation context, so the function builds its own
   for the JSON cast. A null prompt gets an immediate null-input response
   and never reaches the model.
2. **Filter split.** Ember splits a filter's top-level `&` into a filter of
   the conditions without AI calls, followed by a filter of the rest. The
   optimizer then pushes the cheap filter into the scan as before, and the
   model sees only its rows. Axiom also keeps every condition above an RPC
   node, even one that does not read the answer. So a filter after a
   `withColumn` that computes a column with AI calls puts its conditions
   that do not read the column before the `withColumn`, through any number
   of such steps.
3. **Guards.** Before Ember hands a projection or filter to Axiom, it
   rewrites every AI call that is still under a condition. The call moves into
   a projection of its own, and its prompt is wrapped as
   `if(guard, prompt, null)`. The guard is the condition under which Velox
   would have evaluated the call:

   | Call under | Guard |
   |---|---|
   | `when(c1, x).when(c2, ai(p))` | `c2 and not c1` (null counts as false) |
   | `otherwise(ai(p))` | no condition matched |
   | `a & ai(p)` | `a` is not false |
   | `a \| ai(p)` | `a` is not true |
   | `coalesce(x, ai(p))` | `x` is null |

   Rows outside the guard get a null prompt, so they make no call, and the
   original expression reads the new column. Calls are made on exactly
   the rows that make them today. A call under nested conditions gets all
   their guards, joined with `and`. Inside a filter, AI
   conditions run after the cheap ones, in a fixed order; Velox's adaptive
   reordering of `AND` no longer applies to them. `try` stays unsupported:
   a failed call is already a status, not an error.
4. **Admission.** `admissionKey()` identifies the endpoint and
   `configuredCeiling()` is its `max_concurrency`, so all operators of a
   process share one limit per endpoint. A 429 answer or a timeout reports
   `kOverloaded`, so the operator's window backs off; `qps` stays in
   `HttpBackend`. Retries stay in `HttpBackend` and are reported through
   `numRetriesAttempted()`.
5. **Statistics.** The `ai*` runtime stats keep their names and meaning,
   added in `buildOutput` on the driver thread, so `TaskResult::stats`,
   `StageAttemptInfo::stats` and `session.stats()` do not change. The RPC
   operator's own stats (requests, errors, latency) come on top.
6. **Registration.** The RPC plan node translator and the function are
   registered where Ember registers its functions today, on the driver and
   on workers.

The API, the endpoint config, stage keys and reuse do not change.

### Costs and risks

- The async RPC code is new upstream and still changing, and its only open
  source users are demos. Ember pins its Velox, so an upstream change
  reaches Ember only when the pin moves.
- The guard rewrite is Ember's own planner code: an expression walk over
  `when`, `and`, `or` and `coalesce`, about the size of the stage-key code.
- A cluster test runs an AI stage on separate worker processes.

## Reuse

`ai_generate` is marked deterministic, so stages that call it get stage keys
([../ember/driver/StageKey.h](../ember/driver/StageKey.h)) and are reused like
any other stage: through `ember.reuse.outputs` within a session, `cache()`,
and the Kiln stage memo (`ember.reuse.kiln`) across sessions. A rerun of an
unchanged program makes no calls. The endpoint's `kind`, `url` and `model`
go into the stage keys like other config that changes results, so a new
model invalidates reuse; `api_key_env`, `max_concurrency` and `qps` do not.

Failed rows are not reused. `ai_generate` counts the rows it gives a status
(a failed call, or an answer that does not fit the schema) in the runtime
stat `aiFailedRows`; each task reports the sum in its result. The driver does
not keep the output of a stage with any failed rows, nor of the stages that
read it, so a rerun of the same program asks the model again, e.g. after an
outage. The current job still uses the output. An explicit `cache()` keeps
its result either way, until `unpersist()`.

Models do not keep the promise of determinism. That leaves one open
problem:

- **Lost map outputs.** When a worker dies after some consumers have read a
  map output of an AI stage, the recomputed task may get different answers,
  so downstream can see a mix of old and new answers, e.g. a row counted
  under two labels. It takes a lost worker in the middle of a job. A fix
  would treat such stages as indeterminate and rerun all tasks of their
  consumers when one of their outputs is lost.

Dropped: a cache of answers per prompt in Kiln, which would make answers
consistent across task retries and let an agent that adds rows pay only for
the new ones. Its per-row writes from a large cluster would contend on Kiln
(many compare-and-swaps on the same refs, one winner each). An answer cache
inside each process was dropped too: it would add a new kind of worker
memory for a per-process saving, where stage reuse needs no new mechanism.

## Statistics

`ai_generate` keeps Velox runtime stats on the operator that evaluates it:

| Stat | Counts |
|---|---|
| `aiCalls` | Prompts sent (distinct per batch), whether answered or not. |
| `aiRetries` | Requests sent again after a 429, 5xx, timeout or connection error. |
| `aiFailedRows` | Rows with a status: failed calls and answers that do not fit the schema. |
| `aiInputTokens`, `aiOutputTokens` | Tokens of the answered prompts and of the answers, from the response's `usage` (0 if the endpoint sends none). |

Each task sums them into its result (`TaskResult::stats`). The driver sums
those per stage attempt (`StageAttemptInfo::stats`, seen by
`onStageCompleted`) and for the session: `Session::stats()` in C++,
`session.stats()` in Python, as a map from name to total since the session
started. An agent takes the difference around an action to see what it cost:

```python
before = ember.stats()
judged.write.parquet("judged/")
cost = {k: v - before.get(k, 0) for k, v in ember.stats().items()}
```

A reused stage makes no calls and adds nothing.

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
| 3 | Mark `ai.generate` deterministic; the endpoint's `kind`, `url` and `model` in stage keys; stages with failed rows are not kept; tests that a rerun makes no calls, that a new model changes the keys, and that a rerun asks again for failed rows. |
| 4 | Runtime statistics: calls, retries, failed rows and tokens per task, summed per stage attempt and per session (`Session::stats()`, `session.stats()`). |
| 5 | A manual check against a real endpoint. velox-dev has no GPU, so: llama.cpp's `llama-server` on CPU (OpenAI-compatible, JSON-schema output) with a small instruct model such as Qwen2.5-0.5B or 1.5B (GGUF), under a memory cap. |
| 6 | The filter split, and the guard rewrite for AI calls under `when`/`otherwise`, `&`, `\|` and `coalesce`. It already works with the vector function, so the existing tests show it keeps calls to the same rows. |
| 7 | `ai_generate` as an `AsyncRPCFunction` on the RPC operator: admission per endpoint, statistics, registration; the vector function and the Axiom metadata go away. All existing tests pass unchanged, plus a cluster test with an AI stage on separate worker processes, and a repeat of step 5's manual check. |

Left out for now: convenience functions such as `classify`, `score` or
`if_` (thin helpers over `generate` with a fixed prompt and output schema,
easy to add later), adapters other than OpenAI-compatible, rate limits shared across the job,
Python callbacks for prompts or parsing, embeddings, multimodal inputs,
providers' batch APIs (the RPC operator's batch mode would fit them), and
the GPU steps above.
