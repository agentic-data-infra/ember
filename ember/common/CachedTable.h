// Cached tables: how a cached Dataframe appears in the plans that read it.
//
// Dataframe::cache() registers a CachedTable and starts a new plan with a scan
// of it: table <id> under the file connector schema kCachedSchema. The
// optimizer plans that scan like a file scan, without crossing it, so nothing
// downstream of cache() moves into the cached plan. The driver never runs
// such a scan: it materializes the cached plan as a stored shuffle and reads
// that instead (driver/StagePlan.h, driver/DagScheduler.h).

#pragma once

#include <functional>
#include <memory>
#include <string>

#include "axiom/optimizer/MultiFragmentPlan.h"

namespace ember {

inline constexpr const char* kCachedSchema = "cached";

struct CachedTable {
  CachedTable(
      std::string id,
      facebook::velox::RowTypePtr schema,
      std::function<facebook::axiom::optimizer::MultiFragmentPlanPtr()> plan)
      : id{std::move(id)}, schema{std::move(schema)}, plan{std::move(plan)} {}

  /// Unique in the process, e.g. "cache_3".
  std::string id;
  /// The cached Dataframe's schema.
  facebook::velox::RowTypePtr schema;
  /// Compiles the cached Dataframe's plan.
  std::function<facebook::axiom::optimizer::MultiFragmentPlanPtr()> plan;

  /// Unregisters the table.
  ~CachedTable();

  CachedTable(const CachedTable&) = delete;
  CachedTable& operator=(const CachedTable&) = delete;
};

/// Registers a table with a new id. The registry holds it weakly: it is
/// found as long as something holds the returned pointer.
std::shared_ptr<const CachedTable> registerCachedTable(
    facebook::velox::RowTypePtr schema,
    std::function<facebook::axiom::optimizer::MultiFragmentPlanPtr()> plan);

/// The table with 'id', or nullptr if it is gone.
std::shared_ptr<const CachedTable> findCachedTable(const std::string& id);

/// Registers the file connector handler that gives cached tables their
/// schema. Safe to call more than once.
void registerCachedTableHandler();

} // namespace ember
