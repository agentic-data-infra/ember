#include "ember/ai/OutputSchema.h"

#include <folly/json.h>

#include "velox/common/base/Exceptions.h"

namespace ember::ai {

namespace velox = facebook::velox;

namespace {

folly::dynamic schemaOfType(const velox::Type& type) {
  switch (type.kind()) {
    case velox::TypeKind::BOOLEAN:
      return folly::dynamic::object("type", "boolean");
    case velox::TypeKind::TINYINT:
    case velox::TypeKind::SMALLINT:
    case velox::TypeKind::INTEGER:
    case velox::TypeKind::BIGINT:
      return folly::dynamic::object("type", "integer");
    case velox::TypeKind::REAL:
    case velox::TypeKind::DOUBLE:
      return folly::dynamic::object("type", "number");
    case velox::TypeKind::VARCHAR:
      return folly::dynamic::object("type", "string");
    case velox::TypeKind::ARRAY:
      return folly::dynamic::object("type", "array")(
          "items", schemaOfType(*type.childAt(0)));
    case velox::TypeKind::ROW: {
      const auto& row = type.asRow();
      folly::dynamic properties = folly::dynamic::object();
      folly::dynamic required = folly::dynamic::array();
      for (auto i = 0; i < row.size(); ++i) {
        properties[row.nameOf(i)] = schemaOfType(*row.childAt(i));
        required.push_back(row.nameOf(i));
      }
      return folly::dynamic::object("type", "object")(
          "properties", std::move(properties))("required", std::move(required))(
          "additionalProperties", false);
    }
    default:
      VELOX_USER_FAIL(
          "AI output schemas cannot hold {}: use boolean, integers, real, "
          "double, varchar, array or row",
          type.toString());
  }
}

} // namespace

velox::RowTypePtr resultType(const velox::RowTypePtr& schema) {
  VELOX_USER_CHECK_NOT_NULL(schema, "AI output schema is missing");
  VELOX_USER_CHECK_GT(schema->size(), 0, "AI output schema has no fields");
  VELOX_USER_CHECK(
      !schema->containsChild(kStatusField),
      "AI output schema cannot have a field named {}",
      kStatusField);
  schemaOfType(*schema);
  auto names = schema->names();
  auto types = schema->children();
  names.push_back(kStatusField);
  types.push_back(velox::VARCHAR());
  return velox::ROW(std::move(names), std::move(types));
}

velox::RowTypePtr schemaOf(const velox::RowType& result) {
  auto names = result.names();
  auto types = result.children();
  names.pop_back();
  types.pop_back();
  return velox::ROW(std::move(names), std::move(types));
}

std::string jsonSchema(const velox::RowType& schema) {
  return folly::toJson(schemaOfType(schema));
}

} // namespace ember::ai
