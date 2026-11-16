// AI functions: Dataframe expressions that call an LLM per row. See
// docs/ai-functions.md and, for endpoints, ember/ai/AiEndpoint.h.
//
//   Session::builder()
//       .config("ember.ai.endpoint.small.url", "http://localhost:8080/v1")
//       .config("ember.ai.endpoint.small.model", "qwen2.5-1.5b-instruct")
//       ...
//   df.select(ai::generate(concat(lit("Summarize: "), col("text")), "small")
//                 .getField("result"));
//   df.select(ai::generate(col("text"), "small",
//                          ROW({"educational", "topic"}, {BOOLEAN(), VARCHAR()}))
//                 .getField("educational"));

#pragma once

#include <string>

#include "ember/Column.h"

namespace ember::ai {

/// Sends each row's 'prompt' to 'endpoint' and returns
/// ROW(result VARCHAR, status VARCHAR): the answer and an empty status, or a
/// null result and the error when the call failed. A null prompt gives a null
/// row.
Column generate(const Column& prompt, const std::string& endpoint);

/// Like generate(prompt, endpoint), but the model answers with a JSON object
/// of 'output' (fields of boolean, integer, real, double, varchar, array or
/// row types), parsed into typed fields: the result is 'output's fields plus
/// status VARCHAR. An answer that does not fit has null fields and the error
/// in status.
Column generate(
    const Column& prompt,
    const std::string& endpoint,
    const facebook::velox::RowTypePtr& output);

} // namespace ember::ai
