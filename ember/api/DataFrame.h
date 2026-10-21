// DataFrame: an immutable, lazily evaluated relation.
//
// A DataFrame is a cheap value: a session plus a shared, immutable chain of
// plan steps. Each transformation returns a new DataFrame and runs nothing;
// the Axiom logical plan is built from the chain (and validated, so errors
// surface where the transformation is written).
// Actions (collect, count, show) hand the plan to the session, which optimizes
// it into a multi-fragment Velox plan and runs it.

#pragma once

#include <functional>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "ember/Column.h"
#include "velox/vector/ComplexVector.h"

namespace ember {

class GroupedData;
class SessionState;

/// One result row.
class Row {
 public:
  explicit Row(std::vector<facebook::velox::Variant> values)
      : values_{std::move(values)} {}

  size_t size() const {
    return values_.size();
  }

  const facebook::velox::Variant& operator[](size_t i) const {
    return values_.at(i);
  }

  bool isNullAt(size_t i) const {
    return values_.at(i).isNull();
  }

  template <typename T>
  T get(size_t i) const {
    return values_.at(i).value<T>();
  }

  const std::string& getString(size_t i) const {
    return values_.at(i).value<std::string>();
  }

  int64_t getLong(size_t i) const {
    return get<int64_t>(i);
  }

  const std::vector<facebook::velox::Variant>& values() const {
    return values_;
  }

  bool operator==(const Row& other) const = default;

  std::string toString() const;

 private:
  std::vector<facebook::velox::Variant> values_;
};

/// One immutable step of a DataFrame's plan: applies an operation to the
/// builder that replays the steps before it.
struct PlanStep {
  using Apply = std::function<void(
      facebook::axiom::logical_plan::PlanBuilder& builder,
      const facebook::axiom::logical_plan::PlanBuilder::Context& context)>;

  std::shared_ptr<const PlanStep> parent;
  Apply apply;
};

class DataFrame {
 public:
  DataFrame(
      std::shared_ptr<SessionState> session,
      std::shared_ptr<const PlanStep> step);

  // Transformations.

  DataFrame select(const std::vector<Column>& columns) const;

  template <typename... T>
  DataFrame select(const Column& first, const T&... rest) const {
    return select(std::vector<Column>{first, Column(rest)...});
  }

  /// SQL expression strings, e.g. selectExpr({"lower(value) AS v"}).
  DataFrame selectExpr(const std::vector<std::string>& exprs) const;

  /// Alias of select, for Koski familiarity.
  DataFrame map(const std::vector<Column>& columns) const {
    return select(columns);
  }

  DataFrame filter(const Column& condition) const;
  /// A SQL condition, e.g. filter("salary > 50").
  DataFrame filter(const std::string& condition) const;
  DataFrame filter(const char* condition) const {
    return filter(std::string(condition));
  }
  DataFrame where(const Column& condition) const {
    return filter(condition);
  }
  DataFrame where(const std::string& condition) const {
    return filter(condition);
  }
  DataFrame where(const char* condition) const {
    return filter(std::string(condition));
  }

  /// Adds column 'name', or replaces it if it exists.
  DataFrame withColumn(const std::string& name, const Column& column) const;
  DataFrame withColumnRenamed(
      const std::string& existing,
      const std::string& name) const;
  DataFrame drop(const std::vector<std::string>& names) const;

  GroupedData groupBy(const std::vector<Column>& columns) const;

  template <typename... T>
  GroupedData groupBy(const Column& first, const T&... rest) const;

  /// Aggregates over the whole DataFrame: groupBy().agg(...).
  DataFrame agg(const std::vector<Column>& aggregates) const;

  DataFrame orderBy(const std::vector<Column>& columns) const;

  template <typename... T>
  DataFrame orderBy(const Column& first, const T&... rest) const {
    return orderBy(std::vector<Column>{first, Column(rest)...});
  }

  DataFrame sort(const std::vector<Column>& columns) const {
    return orderBy(columns);
  }

  DataFrame limit(int64_t n) const;
  DataFrame distinct() const;

  /// Union by position, keeping duplicates.
  DataFrame unionAll(const DataFrame& other) const;
  DataFrame union_(const DataFrame& other) const {
    return unionAll(other);
  }

  /// Equi-join on columns with the same names on both sides. 'how' is one of
  /// inner, left, right, full (or outer).
  DataFrame join(
      const DataFrame& right,
      const std::vector<std::string>& usingColumns,
      const std::string& how = "inner") const;

  // Actions.

  std::vector<Row> collect() const;
  std::vector<facebook::velox::RowVectorPtr> collectBatches() const;
  int64_t count() const;
  std::vector<Row> take(int64_t n) const {
    return limit(n).collect();
  }

  /// Prints the first 'numRows' rows as a table. With
  /// 'truncate', values longer than 20 characters are cut and cells are
  /// right-aligned.
  void show(int32_t numRows = 20, bool truncate = true) const;
  std::string showString(int32_t numRows = 20, bool truncate = true) const;

  // Schema and plans.

  facebook::velox::RowTypePtr schema() const;
  std::vector<std::string> columns() const;
  void printSchema() const;

  /// The Axiom logical plan, with output columns named as in schema().
  facebook::axiom::logical_plan::LogicalPlanNodePtr logicalPlan() const;

  /// The optimized plan: Velox plan fragments, one per stage.
  facebook::axiom::optimizer::MultiFragmentPlanPtr physicalPlan() const;

  /// Logical plan and the optimized multi-fragment Velox plan.
  std::string explainString() const;
  void explain() const;

  const std::shared_ptr<SessionState>& session() const {
    return session_;
  }

 private:
  friend class GroupedData;

  DataFrame then(PlanStep::Apply apply) const;

  // Replays the plan into a fresh builder.
  facebook::axiom::logical_plan::PlanBuilder builder(
      const facebook::axiom::logical_plan::PlanBuilder::Context& context) const;

  std::shared_ptr<SessionState> session_;
  std::shared_ptr<const PlanStep> step_;
  // Output type of the built plan, computed (and validated) on construction.
  facebook::velox::RowTypePtr schema_;
};

/// Result of groupBy: aggregations over groups.
class GroupedData {
 public:
  GroupedData(DataFrame df, std::vector<Column> keys)
      : df_{std::move(df)}, keys_{std::move(keys)} {}

  DataFrame agg(const std::vector<Column>& aggregates) const;

  template <typename... T>
  DataFrame agg(const Column& first, const T&... rest) const {
    return agg(std::vector<Column>{first, Column(rest)...});
  }

  /// Adds a "count" column with the number of rows in each group.
  DataFrame count() const;
  DataFrame sum(const std::vector<std::string>& columns) const;
  DataFrame avg(const std::vector<std::string>& columns) const;
  DataFrame min(const std::vector<std::string>& columns) const;
  DataFrame max(const std::vector<std::string>& columns) const;

 private:
  DataFrame aggEach(
      const std::string& function,
      const std::vector<std::string>& columns) const;

  DataFrame df_;
  std::vector<Column> keys_;
};

template <typename... T>
GroupedData DataFrame::groupBy(const Column& first, const T&... rest) const {
  return groupBy(std::vector<Column>{first, Column(rest)...});
}

} // namespace ember
