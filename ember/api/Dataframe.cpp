#include "ember/Dataframe.h"

#include <algorithm>
#include <iostream>
#include <sstream>

#include "axiom/logical_plan/PlanPrinter.h"
#include "axiom/optimizer/ToVelox.h"
#include "ember/CachedTable.h"
#include "ember/Session.h"
#include "ember/functions.h"

namespace ember {

namespace lp = facebook::axiom::logical_plan;
namespace velox = facebook::velox;

namespace {

std::vector<lp::ExprApi> exprs(const std::vector<Column>& columns) {
  std::vector<lp::ExprApi> result;
  result.reserve(columns.size());
  for (const auto& column : columns) {
    result.push_back(column.expr());
  }
  return result;
}

std::vector<lp::ExprApi> columnRefs(const velox::RowTypePtr& type) {
  std::vector<lp::ExprApi> result;
  for (const auto& name : type->names()) {
    result.push_back(lp::Col(name));
  }
  return result;
}

lp::JoinType joinType(const std::string& how) {
  if (how == "inner") {
    return lp::JoinType::kInner;
  }
  if (how == "left" || how == "left_outer" || how == "leftouter") {
    return lp::JoinType::kLeft;
  }
  if (how == "right" || how == "right_outer" || how == "rightouter") {
    return lp::JoinType::kRight;
  }
  if (how == "full" || how == "outer" || how == "full_outer" ||
      how == "fullouter") {
    return lp::JoinType::kFull;
  }
  VELOX_USER_FAIL("Unsupported join type: {}", how);
}

// Type names for printSchema.
std::string schemaTypeName(const velox::Type& type) {
  switch (type.kind()) {
    case velox::TypeKind::VARCHAR:
      return "string";
    case velox::TypeKind::BIGINT:
      return "long";
    case velox::TypeKind::INTEGER:
      return "integer";
    case velox::TypeKind::SMALLINT:
      return "short";
    case velox::TypeKind::TINYINT:
      return "byte";
    case velox::TypeKind::DOUBLE:
      return "double";
    case velox::TypeKind::REAL:
      return "float";
    case velox::TypeKind::BOOLEAN:
      return "boolean";
    case velox::TypeKind::VARBINARY:
      return "binary";
    case velox::TypeKind::ARRAY:
      return "array<" + schemaTypeName(*type.childAt(0)) + ">";
    case velox::TypeKind::MAP:
      return "map<" + schemaTypeName(*type.childAt(0)) + "," +
          schemaTypeName(*type.childAt(1)) + ">";
    default: {
      auto name = type.toString();
      std::transform(name.begin(), name.end(), name.begin(), ::tolower);
      return name;
    }
  }
}

std::string cellString(const velox::BaseVector& vector, velox::vector_size_t i) {
  if (vector.isNullAt(i)) {
    return "NULL";
  }
  return vector.toString(i);
}

} // namespace

std::string Row::toString() const {
  std::string result = "[";
  for (size_t i = 0; i < values_.size(); ++i) {
    if (i > 0) {
      result += ",";
    }
    result += values_[i].isNull()
        ? "null"
        : values_[i].toString(values_[i].inferType());
  }
  return result + "]";
}

Dataframe::Dataframe(
    std::shared_ptr<SessionState> session,
    std::shared_ptr<const PlanStep> step)
    : session_{std::move(session)}, step_{std::move(step)} {
  VELOX_CHECK_NOT_NULL(session_);
  VELOX_CHECK_NOT_NULL(step_);
  if (session_->eagerAnalysis()) {
    analyze();
  }
}

lp::PlanBuilder Dataframe::builder(
    const lp::PlanBuilder::Context& context) const {
  std::vector<const PlanStep*> steps;
  for (const auto* step = step_.get(); step != nullptr;
       step = step->parent().get()) {
    steps.push_back(step);
  }
  lp::PlanBuilder builder(context);
  for (auto it = steps.rbegin(); it != steps.rend(); ++it) {
    (*it)->apply()(builder, context);
  }
  return builder;
}

std::shared_ptr<const CompiledPlan> Dataframe::analyze() const {
  std::lock_guard<std::mutex> l(step_->mutex_);
  if (step_->analyzed_ == nullptr) {
    auto plan = builder(session_->planContext()).build();
    step_->analyzed_ = std::make_shared<const CompiledPlan>(
        CompiledPlan{plan, plan->outputType(), nullptr});
  }
  return step_->analyzed_;
}

std::shared_ptr<const CompiledPlan> Dataframe::compile() const {
  auto analyzed = analyze();
  std::lock_guard<std::mutex> l(step_->mutex_);
  if (step_->compiled_ == nullptr) {
    step_->compiled_ = std::make_shared<const CompiledPlan>(CompiledPlan{
        analyzed->logicalPlan,
        analyzed->schema,
        session_->optimize(analyzed->logicalPlan).plan});
  }
  return step_->compiled_;
}

lp::LogicalPlanNodePtr Dataframe::logicalPlan() const {
  return analyze()->logicalPlan;
}

Dataframe Dataframe::then(PlanStep::Apply apply) const {
  return Dataframe(
      session_, std::make_shared<const PlanStep>(step_, std::move(apply)));
}

Dataframe Dataframe::select(const std::vector<Column>& columns) const {
  const auto numExplodes = std::count_if(
      columns.begin(), columns.end(), [](const auto& c) {
        return c.isExplode();
      });
  VELOX_USER_CHECK_LE(
      numExplodes, 1, "Only one explode is allowed per select");

  if (numExplodes == 0) {
    return then([projections = exprs(columns)](auto& builder, const auto&) {
      builder.project(projections);
    });
  }

  // select(a, explode(arr) AS x) is unnest(arr AS x) then project(a, x).
  std::optional<lp::ExprApi> unnested;
  std::vector<lp::ExprApi> projections;
  for (const auto& column : columns) {
    if (column.isExplode()) {
      // An unaliased explode output is named "col".
      const auto alias = column.alias().value_or("col");
      unnested =
          lp::ExprApi(column.expr().expr(), std::nullopt).unnestAs(alias);
      projections.push_back(lp::Col(alias));
    } else {
      projections.push_back(column.expr());
    }
  }
  return then([unnested, projections](auto& builder, const auto&) {
    builder.unnest({*unnested}).project(projections);
  });
}

Dataframe Dataframe::selectExpr(const std::vector<std::string>& sqls) const {
  std::vector<Column> columns;
  columns.reserve(sqls.size());
  for (const auto& sql : sqls) {
    columns.push_back(functions::expr(sql));
  }
  return select(columns);
}

Dataframe Dataframe::filter(const Column& condition) const {
  return then([predicate = condition.expr()](auto& builder, const auto&) {
    builder.filter(predicate);
  });
}

Dataframe Dataframe::filter(const std::string& condition) const {
  return filter(functions::expr(condition));
}

// withColumn, withColumnRenamed and drop depend on the input's columns, which
// are only known once the plan is built, so they map columns at build time.

Dataframe Dataframe::withColumn(const std::string& name, const Column& column)
    const {
  return then([name, expr = column.expr()](auto& builder, const auto&) {
    if (!builder.hasColumn(name)) {
      builder.with({expr.as(name)});
      return;
    }
    std::vector<lp::ExprApi> projections;
    for (const auto& input : builder.findOrAssignOutputNames()) {
      projections.push_back(
          input.outputName() == name ? expr.as(name)
                                     : input.toCol().as(input.outputName()));
    }
    builder.project(projections);
  });
}

Dataframe Dataframe::withColumnRenamed(
    const std::string& existing,
    const std::string& name) const {
  return then([existing, name](auto& builder, const auto&) {
    std::vector<lp::ExprApi> projections;
    for (const auto& input : builder.findOrAssignOutputNames()) {
      projections.push_back(input.toCol().as(
          input.outputName() == existing ? name : input.outputName()));
    }
    builder.project(projections);
  });
}

Dataframe Dataframe::drop(const std::vector<std::string>& names) const {
  return then([names](auto& builder, const auto&) {
    std::vector<lp::ExprApi> projections;
    for (const auto& input : builder.findOrAssignOutputNames()) {
      if (std::find(names.begin(), names.end(), input.outputName()) ==
          names.end()) {
        projections.push_back(input.toCol().as(input.outputName()));
      }
    }
    builder.project(projections);
  });
}

GroupedData Dataframe::groupBy(const std::vector<Column>& columns) const {
  return GroupedData(*this, columns);
}

Dataframe Dataframe::agg(const std::vector<Column>& aggregates) const {
  return GroupedData(*this, {}).agg(aggregates);
}

Dataframe Dataframe::orderBy(const std::vector<Column>& columns) const {
  std::vector<lp::SortKey> keys;
  keys.reserve(columns.size());
  for (const auto& column : columns) {
    const auto order = column.sortOrder().value_or(Column::SortOrder{});
    keys.emplace_back(column.expr(), order.ascending, order.nullsFirst);
  }
  return then([keys](auto& builder, const auto&) { builder.sort(keys); });
}

Dataframe Dataframe::limit(int64_t n) const {
  return then([n](auto& builder, const auto&) { builder.limit(n); });
}

Dataframe Dataframe::distinct() const {
  return then([](auto& builder, const auto&) { builder.distinct(); });
}

Dataframe Dataframe::unionAll(const Dataframe& other) const {
  VELOX_USER_CHECK(
      other.session_ == session_, "Cannot union Dataframes of two sessions");
  return then([other](auto& builder, const auto& context) {
    builder.unionAll(other.builder(context));
  });
}

Dataframe Dataframe::join(
    const Dataframe& right,
    const std::vector<std::string>& usingColumns,
    const std::string& how) const {
  VELOX_USER_CHECK(
      right.session_ == session_, "Cannot join Dataframes of two sessions");
  return then([right, usingColumns, type = joinType(how)](
                  auto& builder, const auto& context) {
    builder.joinUsing(right.builder(context), usingColumns, type);
  });
}

std::vector<velox::RowVectorPtr> Dataframe::collectBatches() const {
  auto compiled = compile();
  auto batches = session_->execute(compiled->physicalPlan);
  // Name result columns as in schema().
  for (auto& batch : batches) {
    batch = std::make_shared<velox::RowVector>(
        batch->pool(),
        compiled->schema,
        batch->nulls(),
        batch->size(),
        batch->children());
  }
  return batches;
}

std::vector<Row> Dataframe::collect() const {
  std::vector<Row> rows;
  for (const auto& batch : collectBatches()) {
    for (velox::vector_size_t i = 0; i < batch->size(); ++i) {
      std::vector<velox::Variant> values;
      values.reserve(batch->childrenSize());
      for (const auto& child : batch->children()) {
        values.push_back(child->variantAt(i));
      }
      rows.emplace_back(std::move(values));
    }
  }
  return rows;
}

int64_t Dataframe::count() const {
  auto rows = agg({functions::countAll().as("count")}).collect();
  VELOX_CHECK_EQ(rows.size(), 1);
  return rows[0].getLong(0);
}

std::string Dataframe::showString(int32_t numRows, bool truncate) const {
  constexpr size_t kTruncateWidth = 20;
  const auto schema = this->schema();
  const auto numColumns = schema->size();

  std::vector<std::vector<std::string>> cells;
  cells.push_back(schema->names());
  bool hasMore = false;
  for (const auto& batch : limit(numRows + 1).collectBatches()) {
    for (velox::vector_size_t i = 0; i < batch->size(); ++i) {
      if (cells.size() > static_cast<size_t>(numRows)) {
        hasMore = true;
        break;
      }
      std::vector<std::string> row;
      for (const auto& child : batch->children()) {
        row.push_back(cellString(*child, i));
      }
      cells.push_back(std::move(row));
    }
  }

  std::vector<size_t> widths(numColumns, 3);
  for (auto& row : cells) {
    for (size_t c = 0; c < numColumns; ++c) {
      if (truncate && row[c].size() > kTruncateWidth) {
        row[c] = row[c].substr(0, kTruncateWidth - 3) + "...";
      }
      widths[c] = std::max(widths[c], row[c].size());
    }
  }

  std::ostringstream out;
  std::string separator = "+";
  for (auto width : widths) {
    separator += std::string(width, '-') + "+";
  }
  auto printRow = [&](const std::vector<std::string>& row) {
    out << "|";
    for (size_t c = 0; c < numColumns; ++c) {
      const auto pad = std::string(widths[c] - row[c].size(), ' ');
      // Truncated tables are right-aligned, the others left-aligned.
      out << (truncate ? pad + row[c] : row[c] + pad) << "|";
    }
    out << "\n";
  };
  out << separator << "\n";
  printRow(cells[0]);
  out << separator << "\n";
  for (size_t r = 1; r < cells.size(); ++r) {
    printRow(cells[r]);
  }
  out << separator << "\n";
  if (hasMore) {
    out << "only showing top " << numRows << (numRows == 1 ? " row" : " rows")
        << "\n";
  }
  return out.str();
}

void Dataframe::show(int32_t numRows, bool truncate) const {
  std::cout << showString(numRows, truncate) << std::endl;
}

velox::RowTypePtr Dataframe::schema() const {
  return analyze()->schema;
}

std::vector<std::string> Dataframe::columns() const {
  return schema()->names();
}

void Dataframe::printSchema() const {
  const auto schema = this->schema();
  std::cout << "root\n";
  for (size_t i = 0; i < schema->size(); ++i) {
    std::cout << " |-- " << schema->nameOf(i) << ": "
              << schemaTypeName(*schema->childAt(i)) << " (nullable = true)\n";
  }
  std::cout << std::endl;
}

std::string Dataframe::explainString() const {
  auto compiled = compile();
  return "== Logical Plan ==\n" +
      lp::PlanPrinter::toText(*compiled->logicalPlan) +
      "\n== Physical Plan ==\n" + compiled->physicalPlan->toString(false) +
      "\n== Stages ==\n" + session_->explainStages(compiled->physicalPlan);
}

void Dataframe::explain() const {
  std::cout << explainString() << std::endl;
}

Dataframe Dataframe::cache() const {
  auto table = registerCachedTable(
      schema(), [prefix = *this] { return prefix.physicalPlan(); });
  return Dataframe(
      session_,
      std::make_shared<const PlanStep>(
          nullptr,
          [id = table->id](lp::PlanBuilder& builder, const auto& /*context*/) {
            builder.tableScan(SessionState::fileConnectorId(), kCachedSchema, id);
          },
          table));
}

const Dataframe& Dataframe::unpersist() const {
  if (step_->cached_ != nullptr) {
    session_->uncache(step_->cached_->id);
  }
  return *this;
}

Dataframe GroupedData::agg(const std::vector<Column>& aggregates) const {
  return df_.then([keys = exprs(keys_), aggregates = exprs(aggregates)](
                      auto& builder, const auto&) {
    builder.aggregate(keys, aggregates);
  });
}

Dataframe GroupedData::count() const {
  return agg({functions::countAll().as("count")});
}

Dataframe GroupedData::aggEach(
    const std::string& function,
    const std::vector<std::string>& columns) const {
  std::vector<Column> aggregates;
  for (const auto& column : columns) {
    aggregates.push_back(
        functions::call(function, {Column(column)})
            .as(fmt::format("{}({})", function, column)));
  }
  return agg(aggregates);
}

Dataframe GroupedData::sum(const std::vector<std::string>& columns) const {
  return aggEach("sum", columns);
}

Dataframe GroupedData::avg(const std::vector<std::string>& columns) const {
  return aggEach("avg", columns);
}

Dataframe GroupedData::min(const std::vector<std::string>& columns) const {
  return aggEach("min", columns);
}

Dataframe GroupedData::max(const std::vector<std::string>& columns) const {
  return aggEach("max", columns);
}

} // namespace ember
