// Dataframe: an immutable, lazily evaluated relation.
//
// A Dataframe is a cheap value: a session plus a shared, immutable chain of
// plan steps. Each transformation appends one step, in constant time, and
// builds and runs nothing. compile() builds the Axiom logical plan from the
// chain and optimizes it into a multi-fragment Velox plan, once; actions
// (collect, count, show) compile and then run the plan.

#pragma once

#include <functional>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/optimizer/MultiFragmentPlan.h"
#include "ember/Column.h"
#include "velox/vector/ComplexVector.h"

namespace ember {

struct CachedTable;
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

/// The plans of a Dataframe, built by Dataframe::compile().
struct CompiledPlan {
  /// Axiom logical plan, with output columns named as in 'schema'.
  facebook::axiom::logical_plan::LogicalPlanNodePtr logicalPlan;
  facebook::velox::RowTypePtr schema;
  /// Optimized plan: Velox plan fragments, one per stage.
  facebook::axiom::optimizer::MultiFragmentPlanPtr physicalPlan;
};

/// One immutable step of a Dataframe's plan: applies an operation to the
/// builder that replays the steps before it. Caches the plans built from the
/// chain ending here, shared by every copy of the Dataframe.
class PlanStep {
 public:
  using Apply = std::function<void(
      facebook::axiom::logical_plan::PlanBuilder& builder,
      const facebook::axiom::logical_plan::PlanBuilder::Context& context)>;

  PlanStep(
      std::shared_ptr<const PlanStep> parent,
      Apply apply,
      std::shared_ptr<const CachedTable> cached = nullptr)
      : parent_{std::move(parent)},
        apply_{std::move(apply)},
        cached_{std::move(cached)} {}

  const std::shared_ptr<const PlanStep>& parent() const {
    return parent_;
  }

  const Apply& apply() const {
    return apply_;
  }

 private:
  friend class Dataframe;

  const std::shared_ptr<const PlanStep> parent_;
  const Apply apply_;
  // The table a cache() step scans.
  const std::shared_ptr<const CachedTable> cached_;

  mutable std::mutex mutex_;
  // Logical plan and schema only.
  mutable std::shared_ptr<const CompiledPlan> analyzed_;
  // All plans.
  mutable std::shared_ptr<const CompiledPlan> compiled_;
};

class Dataframe {
 public:
  Dataframe(
      std::shared_ptr<SessionState> session,
      std::shared_ptr<const PlanStep> step);

  // Transformations. Each returns a new Dataframe; the receiver is unchanged.

  [[nodiscard]] Dataframe select(const std::vector<Column>& columns) const;

  template <typename... T>
  [[nodiscard]] Dataframe select(const Column& first, const T&... rest) const {
    return select(std::vector<Column>{first, Column(rest)...});
  }

  /// SQL expression strings, e.g. selectExpr({"lower(value) AS v"}).
  [[nodiscard]] Dataframe selectExpr(const std::vector<std::string>& exprs) const;

  /// Alias of select, for Koski familiarity.
  [[nodiscard]] Dataframe map(const std::vector<Column>& columns) const {
    return select(columns);
  }

  [[nodiscard]] Dataframe filter(const Column& condition) const;
  /// A SQL condition, e.g. filter("salary > 50").
  [[nodiscard]] Dataframe filter(const std::string& condition) const;
  [[nodiscard]] Dataframe filter(const char* condition) const {
    return filter(std::string(condition));
  }
  [[nodiscard]] Dataframe where(const Column& condition) const {
    return filter(condition);
  }
  [[nodiscard]] Dataframe where(const std::string& condition) const {
    return filter(condition);
  }
  [[nodiscard]] Dataframe where(const char* condition) const {
    return filter(std::string(condition));
  }

  /// Adds column 'name', or replaces it if it exists.
  [[nodiscard]] Dataframe withColumn(const std::string& name, const Column& column) const;
  [[nodiscard]] Dataframe withColumnRenamed(
      const std::string& existing,
      const std::string& name) const;
  [[nodiscard]] Dataframe drop(const std::vector<std::string>& names) const;

  [[nodiscard]] GroupedData groupBy(const std::vector<Column>& columns) const;

  template <typename... T>
  [[nodiscard]] GroupedData groupBy(const Column& first, const T&... rest) const;

  /// Aggregates over the whole Dataframe: groupBy().agg(...).
  [[nodiscard]] Dataframe agg(const std::vector<Column>& aggregates) const;

  [[nodiscard]] Dataframe orderBy(const std::vector<Column>& columns) const;

  template <typename... T>
  [[nodiscard]] Dataframe orderBy(const Column& first, const T&... rest) const {
    return orderBy(std::vector<Column>{first, Column(rest)...});
  }

  [[nodiscard]] Dataframe sort(const std::vector<Column>& columns) const {
    return orderBy(columns);
  }

  [[nodiscard]] Dataframe limit(int64_t n) const;
  [[nodiscard]] Dataframe distinct() const;

  /// Union by position, keeping duplicates.
  [[nodiscard]] Dataframe unionAll(const Dataframe& other) const;
  [[nodiscard]] Dataframe union_(const Dataframe& other) const {
    return unionAll(other);
  }

  /// Equi-join on columns with the same names on both sides. 'how' is one of
  /// inner, left, right, full (or outer).
  [[nodiscard]] Dataframe join(
      const Dataframe& right,
      const std::vector<std::string>& usingColumns,
      const std::string& how = "inner") const;

  /// Marks this Dataframe's result to be kept: the returned Dataframe reads
  /// the same rows, but plans that use it are cut there. The first action
  /// that needs it computes this Dataframe and stores the result on the
  /// executors, one part per task; later actions read the stored result, in
  /// any plan built on the returned Dataframe. Lost parts are recomputed.
  /// The optimizer does not move work across the cut, e.g. a filter
  /// downstream is not pushed into the cached plan.
  ///
  /// Use the returned Dataframe: this one is unchanged, and plans built on
  /// it compute everything.
  [[nodiscard]] Dataframe cache() const;

  /// Alias of cache(); results are kept on the executors' local disks.
  [[nodiscard]] Dataframe persist() const {
    return cache();
  }

  /// On a Dataframe returned by cache(): drops the stored result, which the
  /// next action that needs it computes again. Otherwise does nothing.
  const Dataframe& unpersist() const;

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

  // Compilation, schema and plans.

  /// Builds the logical plan and optimizes it into a Velox plan. Actions call
  /// it before running; the result is cached, so later calls (and later
  /// actions on this Dataframe or its copies) do not build it again.
  /// Errors in the transformations surface here, unless the session sets
  /// "ember.eager_analysis" to "true", which checks each transformation.
  std::shared_ptr<const CompiledPlan> compile() const;

  /// Builds only the logical plan and schema (cached like compile()).
  facebook::velox::RowTypePtr schema() const;
  std::vector<std::string> columns() const;
  void printSchema() const;

  /// compile()->logicalPlan, building only the logical plan.
  facebook::axiom::logical_plan::LogicalPlanNodePtr logicalPlan() const;

  /// compile()->physicalPlan.
  facebook::axiom::optimizer::MultiFragmentPlanPtr physicalPlan() const {
    return compile()->physicalPlan;
  }

  /// Logical plan and the optimized multi-fragment Velox plan.
  std::string explainString() const;
  void explain() const;

  const std::shared_ptr<SessionState>& session() const {
    return session_;
  }

 private:
  friend class GroupedData;

  Dataframe then(PlanStep::Apply apply) const;

  // Replays the plan into a fresh builder.
  facebook::axiom::logical_plan::PlanBuilder builder(
      const facebook::axiom::logical_plan::PlanBuilder::Context& context) const;

  // Builds (once) the logical plan and schema.
  std::shared_ptr<const CompiledPlan> analyze() const;

  std::shared_ptr<SessionState> session_;
  std::shared_ptr<const PlanStep> step_;
};

/// Result of groupBy: aggregations over groups.
class GroupedData {
 public:
  GroupedData(Dataframe df, std::vector<Column> keys)
      : df_{std::move(df)}, keys_{std::move(keys)} {}

  [[nodiscard]] Dataframe agg(const std::vector<Column>& aggregates) const;

  template <typename... T>
  [[nodiscard]] Dataframe agg(const Column& first, const T&... rest) const {
    return agg(std::vector<Column>{first, Column(rest)...});
  }

  /// Adds a "count" column with the number of rows in each group.
  [[nodiscard]] Dataframe count() const;
  [[nodiscard]] Dataframe sum(const std::vector<std::string>& columns) const;
  [[nodiscard]] Dataframe avg(const std::vector<std::string>& columns) const;
  [[nodiscard]] Dataframe min(const std::vector<std::string>& columns) const;
  [[nodiscard]] Dataframe max(const std::vector<std::string>& columns) const;

 private:
  Dataframe aggEach(
      const std::string& function,
      const std::vector<std::string>& columns) const;

  Dataframe df_;
  std::vector<Column> keys_;
};

template <typename... T>
GroupedData Dataframe::groupBy(const Column& first, const T&... rest) const {
  return groupBy(std::vector<Column>{first, Column(rest)...});
}

} // namespace ember
