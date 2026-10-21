#include "ember/Column.h"

namespace ember {

namespace lp = facebook::axiom::logical_plan;
using facebook::velox::Variant;

Column lit(const char* value) {
  return lit(std::string(value));
}

Column lit(std::string value) {
  return Column(lp::Lit(Variant(std::move(value))));
}

Column lit(bool value) {
  return Column(lp::Lit(Variant(value)));
}

Column lit(int32_t value) {
  return Column(lp::Lit(Variant(value)));
}

Column lit(int64_t value) {
  return Column(lp::Lit(Variant(value)));
}

Column lit(double value) {
  return Column(lp::Lit(Variant(value)));
}

Column lit(Variant value) {
  return Column(lp::Lit(std::move(value)));
}

Column::Column(const char* name) : expr_{lp::Col(name)} {}

Column::Column(const std::string& name) : expr_{lp::Col(name)} {}

Column Column::as(std::string alias) const {
  return Column(expr_.as(std::move(alias)), sortOrder_, explode_);
}

Column Column::withSortOrder(bool ascending, bool nullsFirst) const {
  return Column(expr_, SortOrder{ascending, nullsFirst}, explode_);
}

Column Column::asc() const {
  return withSortOrder(true, true);
}

Column Column::asc_nulls_first() const {
  return withSortOrder(true, true);
}

Column Column::asc_nulls_last() const {
  return withSortOrder(true, false);
}

Column Column::desc() const {
  return withSortOrder(false, false);
}

Column Column::desc_nulls_first() const {
  return withSortOrder(false, true);
}

Column Column::desc_nulls_last() const {
  return withSortOrder(false, false);
}

Column Column::cast(facebook::velox::TypePtr type) const {
  return Column(lp::Cast(std::move(type), expr_));
}

Column Column::isNull() const {
  return Column(lp::Call("is_null", expr_));
}

Column Column::isNotNull() const {
  return !isNull();
}

Column Column::isin(const std::vector<Column>& values) const {
  std::vector<lp::ExprApi> args{expr_};
  for (const auto& value : values) {
    args.push_back(value.expr());
  }
  return Column(lp::Call("in", args));
}

#define EMBER_COLUMN_OP(op)                                   \
  Column Column::operator op(const Column& other) const {     \
    return Column(expr_ op other.expr_);                      \
  }

EMBER_COLUMN_OP(+)
EMBER_COLUMN_OP(-)
EMBER_COLUMN_OP(*)
EMBER_COLUMN_OP(/)
EMBER_COLUMN_OP(%)
EMBER_COLUMN_OP(==)
EMBER_COLUMN_OP(!=)
EMBER_COLUMN_OP(<)
EMBER_COLUMN_OP(<=)
EMBER_COLUMN_OP(>)
EMBER_COLUMN_OP(>=)
EMBER_COLUMN_OP(&&)
EMBER_COLUMN_OP(||)
#undef EMBER_COLUMN_OP

Column Column::operator!() const {
  return Column(lp::Call("not", expr_));
}

Column Column::operator-() const {
  return Column(lp::Call("negate", expr_));
}

Column Column::makeExplode(const Column& array) {
  return Column(array.expr_, std::nullopt, /*explode=*/true);
}

std::string Column::toString() const {
  auto text = expr_.expr()->toString();
  if (explode_) {
    text = "explode(" + text + ")";
  }
  if (alias().has_value()) {
    text += " AS " + *alias();
  }
  return text;
}

inline namespace literals {
Column operator""_c(const char* name, size_t size) {
  return Column(std::string(name, size));
}
} // namespace literals

} // namespace ember
