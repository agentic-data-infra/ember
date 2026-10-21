#include "ember/functions.h"

#include "velox/parse/Expressions.h"

namespace ember::functions {

namespace lp = facebook::axiom::logical_plan;
namespace core = facebook::velox::core;

namespace {

std::vector<lp::ExprApi> exprs(const std::vector<Column>& columns) {
  std::vector<lp::ExprApi> result;
  result.reserve(columns.size());
  for (const auto& column : columns) {
    result.push_back(column.expr());
  }
  return result;
}

Column fn(const std::string& name, const std::vector<Column>& args) {
  return Column(lp::Call(name, exprs(args)));
}

bool isStar(const Column& column) {
  const auto* field =
      dynamic_cast<const core::FieldAccessExpr*>(column.expr().expr().get());
  return field != nullptr && field->isRootColumn() && field->name() == "*";
}

} // namespace

Column col(const std::string& name) {
  return Column(name);
}

Column column(const std::string& name) {
  return Column(name);
}

Column expr(const std::string& sql) {
  lp::ExprApi parsed = lp::Sql(sql);
  // explode(...) in SQL text is a generator, as in selectExpr.
  const auto* call =
      dynamic_cast<const core::CallExpr*>(parsed.expr().get());
  if (call != nullptr && call->name() == "explode" &&
      call->inputs().size() == 1) {
    auto result = Column::makeExplode(
        Column(lp::ExprApi(call->inputs()[0], std::nullopt)));
    return parsed.alias() ? result.as(*parsed.alias()) : result;
  }
  return Column(std::move(parsed));
}

Column call(const std::string& name, const std::vector<Column>& args) {
  return fn(name, args);
}

Column explode(const Column& array) {
  return Column::makeExplode(array);
}

Column split(const Column& str, const std::string& pattern) {
  return fn("split", {str, lit(pattern)});
}

Column split(const Column& str, const std::string& pattern, int32_t limit) {
  return fn("split", {str, lit(pattern), lit(limit)});
}

Column lower(const Column& str) {
  return fn("lower", {str});
}

Column upper(const Column& str) {
  return fn("upper", {str});
}

Column length(const Column& str) {
  return fn("length", {str});
}

Column trim(const Column& str) {
  return fn("trim", {str});
}

Column ltrim(const Column& str) {
  return fn("ltrim", {str});
}

Column rtrim(const Column& str) {
  return fn("rtrim", {str});
}

Column concat(const std::vector<Column>& columns) {
  return fn("concat", columns);
}

Column substring(const Column& str, int32_t pos, int32_t len) {
  return fn("substring", {str, lit(pos), lit(len)});
}

Column regexp_replace(
    const Column& str,
    const std::string& pattern,
    const std::string& replacement) {
  return fn("regexp_replace", {str, lit(pattern), lit(replacement)});
}

Column size(const Column& collection) {
  // legacySizeOfNull = true: size(null) is -1.
  return fn("size", {collection, lit(true)});
}

Column array_contains(const Column& array, const Column& value) {
  return fn("array_contains", {array, value});
}

Column abs(const Column& value) {
  return fn("abs", {value});
}

Column coalesce(const std::vector<Column>& columns) {
  return fn("coalesce", columns);
}

Column count(const Column& value) {
  if (isStar(value)) {
    return countAll();
  }
  return fn("count", {value});
}

Column countAll() {
  return Column(lp::Call("count"));
}

Column countDistinct(const Column& value) {
  return Column(lp::Call("count", value.expr()).distinct());
}

Column sum(const Column& value) {
  return fn("sum", {value});
}

Column avg(const Column& value) {
  return fn("avg", {value});
}

Column mean(const Column& value) {
  return avg(value);
}

Column min(const Column& value) {
  return fn("min", {value});
}

Column max(const Column& value) {
  return fn("max", {value});
}

Column first(const Column& value) {
  return fn("arbitrary", {value});
}

Column collect_list(const Column& value) {
  return fn("array_agg", {value});
}

} // namespace ember::functions
