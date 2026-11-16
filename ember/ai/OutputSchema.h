// Output schemas of ai_generate: the ROW type a program asks the model to
// answer in. Fields may be BOOLEAN, integers, REAL, DOUBLE, VARCHAR, and
// ARRAYs and ROWs of these. The function's result is the schema's fields
// plus 'status'.

#pragma once

#include <string>

#include "velox/type/Type.h"

namespace ember::ai {

/// The field after the schema's fields: empty on success, else the error.
inline constexpr const char* kStatusField = "status";

/// The result type for 'schema': its fields plus status VARCHAR. Throws a
/// user error for an empty schema, a field named status, or a field type
/// that has no JSON schema.
facebook::velox::RowTypePtr resultType(
    const facebook::velox::RowTypePtr& schema);

/// The schema's fields of a result type made by resultType().
facebook::velox::RowTypePtr schemaOf(const facebook::velox::RowType& result);

/// 'schema' as a JSON schema: an object that requires each field and allows
/// no others.
std::string jsonSchema(const facebook::velox::RowType& schema);

} // namespace ember::ai
