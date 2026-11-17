// The AI functions as Velox async RPC functions:
//
//   ai_generate(prompt VARCHAR, endpoint VARCHAR)
//       -> ROW(result VARCHAR, status VARCHAR)
//   ai_generate(prompt VARCHAR, endpoint VARCHAR, output T) -> T
//
// sends each row's prompt to the endpoint the session config names (a
// constant; see AiEndpoint.h). In the second form, 'output' is a typed null
// of resultType(schema) (OutputSchema.h): the model answers with a JSON
// object of the schema, which Velox's cast from JSON parses into the fields.
// 'status' is empty on success; a failed row, or an answer that does not fit
// the schema, has null fields and the error in 'status', and does not fail
// the task. A null prompt gives a null row and makes no call.
//
// Axiom's optimizer runs each call in an RPC plan node, which Velox's RPC
// operator executes: it sends one request per distinct prompt of an input
// and holds rows until their answers arrive, without blocking the driver
// thread. Operators that call one endpoint share its max_concurrency, and
// back off on a 429 or a timeout. The node never copies the call, so a
// filter on an answer calls once per row; ember/api/RpcCalls.h keeps calls
// under conditions and next to cheap filters to the rows that reach them.
//
// Deterministic, so stages that call it get stage keys and are
// reused like any other: a rerun of an unchanged program makes no calls. The
// keys include the endpoint's kind, url and model (AiEndpoint.h). Rows with a
// status count in the runtime stat kFailedRowsStat; a stage with any is not
// kept for reuse, so a rerun asks again. Models do not keep the promise of
// determinism: a recomputed lost map output may get different answers than
// the ones downstream already read.
//
// Runtime stats of the RPC operator (kStats), which tasks
// report to the driver: calls (distinct prompts sent), retries, failed rows,
// and input and output tokens as the endpoint reports them.

#pragma once

#include <array>

namespace ember::ai {

inline constexpr const char* kGenerateFunction = "ai_generate";

/// Runtime stats of the operator that evaluates ai_generate.
inline constexpr const char* kCallsStat = "aiCalls";
inline constexpr const char* kRetriesStat = "aiRetries";
/// Rows with a non-empty status.
inline constexpr const char* kFailedRowsStat = "aiFailedRows";
inline constexpr const char* kInputTokensStat = "aiInputTokens";
inline constexpr const char* kOutputTokensStat = "aiOutputTokens";
inline constexpr std::array<const char*, 5> kStats{
    kCallsStat,
    kRetriesStat,
    kFailedRowsStat,
    kInputTokensStat,
    kOutputTokensStat};

/// Registers the AI functions. Safe to call more than once.
void registerAiFunctions();

} // namespace ember::ai
