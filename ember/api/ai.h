// AI functions: Dataframe expressions that call an LLM per row. See
// docs/ai-functions.md and, for endpoints, ember/ai/AiEndpoint.h.
//
//   Session::builder()
//       .config("ember.ai.endpoint.small.url", "http://localhost:8080/v1")
//       .config("ember.ai.endpoint.small.model", "qwen2.5-1.5b-instruct")
//       ...
//   df.select(ai::generate(concat(lit("Summarize: "), col("text")), "small")
//                 .getField("result"));

#pragma once

#include <string>

#include "ember/Column.h"

namespace ember::ai {

/// Sends each row's 'prompt' to 'endpoint' and returns
/// ROW(result VARCHAR, status VARCHAR): the answer and an empty status, or a
/// null result and the error when the call failed. A null prompt gives a null
/// row.
Column generate(const Column& prompt, const std::string& endpoint);

} // namespace ember::ai
