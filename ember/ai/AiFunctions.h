// The AI functions as Velox functions:
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
// the task. A null prompt gives a null row. A batch's distinct prompts go out
// concurrently, and the driver thread waits for their answers.
//
// Deterministic, so stages that call it get stage keys and are reused like
// any other: a rerun of an unchanged program makes no calls. The keys include
// the endpoint's kind, url and model (AiEndpoint.h). Models do not keep the
// promise: a recomputed lost map output may get different answers than the
// ones downstream already read, and a reused stage keeps its failed rows.

#pragma once

namespace ember::ai {

inline constexpr const char* kGenerateFunction = "ai_generate";

/// Registers the AI functions. Safe to call more than once.
void registerAiFunctions();

} // namespace ember::ai
