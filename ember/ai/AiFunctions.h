// The AI functions as Velox functions:
//
//   ai_generate(prompt VARCHAR, endpoint VARCHAR)
//       -> ROW(result VARCHAR, status VARCHAR)
//
// sends each row's prompt to the endpoint the session config names (a
// constant; see AiEndpoint.h). 'status' is empty on success; a failed row
// has a null 'result' and the error in 'status', and does not fail the task.
// A null prompt gives a null row. A batch's distinct prompts go out
// concurrently, and the driver thread waits for their answers.
//
// Nondeterministic: answers are not cached yet, so stages that call it are
// never reused.

#pragma once

namespace ember::ai {

inline constexpr const char* kGenerateFunction = "ai_generate";

/// Registers the AI functions. Safe to call more than once.
void registerAiFunctions();

} // namespace ember::ai
