// Column: a DataFrame expression. Wraps an (untyped) Axiom
// expression; types are resolved when the expression is added to a DataFrame.

#pragma once

#include <optional>
#include <string>
#include <type_traits>
#include <vector>

#include "axiom/logical_plan/ExprApi.h"

namespace ember {

class Column;

namespace detail {
template <typename T>
inline constexpr bool kIsColumn = std::is_same_v<std::decay_t<T>, Column>;
} // namespace detail

/// Literal column; see also functions::lit.
Column lit(const char* value);
Column lit(std::string value);
Column lit(bool value);
Column lit(int32_t value);
Column lit(int64_t value);
Column lit(double value);
Column lit(facebook::velox::Variant value);

class Column {
 public:
  /// Sort direction of a column used in orderBy. Defaults: ascending
  /// sorts put nulls first, descending sorts put them last.
  struct SortOrder {
    bool ascending{true};
    bool nullsFirst{true};
  };

  explicit Column(facebook::axiom::logical_plan::ExprApi expr)
      : expr_{std::move(expr)} {}

  /// Implicit conversion from a column name, so APIs taking columns also take
  /// names.
  /* implicit */ Column(const char* name);
  /* implicit */ Column(const std::string& name);

  const facebook::axiom::logical_plan::ExprApi& expr() const {
    return expr_;
  }

  /// The alias set by as(), if any.
  const std::optional<std::string>& alias() const {
    return expr_.alias();
  }

  Column as(std::string alias) const;
  Column alias(std::string alias) const {
    return as(std::move(alias));
  }
  Column name(std::string alias) const {
    return as(std::move(alias));
  }

  Column asc() const;
  Column asc_nulls_first() const;
  Column asc_nulls_last() const;
  Column desc() const;
  Column desc_nulls_first() const;
  Column desc_nulls_last() const;

  const std::optional<SortOrder>& sortOrder() const {
    return sortOrder_;
  }

  /// True for explode(...): a generator that select() turns into an unnest.
  bool isExplode() const {
    return explode_;
  }

  Column cast(facebook::velox::TypePtr type) const;
  Column isNull() const;
  Column isNotNull() const;
  Column isin(const std::vector<Column>& values) const;

  Column operator+(const Column& other) const;
  Column operator-(const Column& other) const;
  Column operator*(const Column& other) const;
  Column operator/(const Column& other) const;
  Column operator%(const Column& other) const;
  Column operator==(const Column& other) const;
  Column operator!=(const Column& other) const;
  Column operator<(const Column& other) const;
  Column operator<=(const Column& other) const;
  Column operator>(const Column& other) const;
  Column operator>=(const Column& other) const;
  Column operator&&(const Column& other) const;
  Column operator||(const Column& other) const;
  Column operator!() const;
  Column operator-() const;

#define EMBER_COLUMN_LITERAL_OP(op)                              \
  template <typename T, typename = std::enable_if_t<!detail::kIsColumn<T>>> \
  Column operator op(const T& value) const {                     \
    return *this op lit(value);                                  \
  }

  EMBER_COLUMN_LITERAL_OP(+)
  EMBER_COLUMN_LITERAL_OP(-)
  EMBER_COLUMN_LITERAL_OP(*)
  EMBER_COLUMN_LITERAL_OP(/)
  EMBER_COLUMN_LITERAL_OP(%)
  EMBER_COLUMN_LITERAL_OP(==)
  EMBER_COLUMN_LITERAL_OP(!=)
  EMBER_COLUMN_LITERAL_OP(<)
  EMBER_COLUMN_LITERAL_OP(<=)
  EMBER_COLUMN_LITERAL_OP(>)
  EMBER_COLUMN_LITERAL_OP(>=)
  EMBER_COLUMN_LITERAL_OP(&&)
  EMBER_COLUMN_LITERAL_OP(||)
#undef EMBER_COLUMN_LITERAL_OP

  /// Builds an explode(...) generator column; see functions::explode.
  static Column makeExplode(const Column& array);

  std::string toString() const;

 private:
  Column(
      facebook::axiom::logical_plan::ExprApi expr,
      std::optional<SortOrder> sortOrder,
      bool explode)
      : expr_{std::move(expr)}, sortOrder_{sortOrder}, explode_{explode} {}

  Column withSortOrder(bool ascending, bool nullsFirst) const;

  facebook::axiom::logical_plan::ExprApi expr_;
  std::optional<SortOrder> sortOrder_;
  bool explode_{false};
};

// Literal on the left-hand side, e.g. 1 + col("x"). Not for == and !=:
// C++20 already rewrites "x" == col into col == "x".
#define EMBER_LITERAL_COLUMN_OP(op)                                       \
  template <typename T, typename = std::enable_if_t<!detail::kIsColumn<T>>> \
  Column operator op(const T& value, const Column& column) {              \
    return lit(value) op column;                                          \
  }

EMBER_LITERAL_COLUMN_OP(+)
EMBER_LITERAL_COLUMN_OP(-)
EMBER_LITERAL_COLUMN_OP(*)
EMBER_LITERAL_COLUMN_OP(/)
EMBER_LITERAL_COLUMN_OP(%)
EMBER_LITERAL_COLUMN_OP(<)
EMBER_LITERAL_COLUMN_OP(<=)
EMBER_LITERAL_COLUMN_OP(>)
EMBER_LITERAL_COLUMN_OP(>=)
#undef EMBER_LITERAL_COLUMN_OP

/// "word"_c is col("word"), standing in for Scala's $"word".
inline namespace literals {
Column operator""_c(const char* name, size_t size);
} // namespace literals

} // namespace ember
