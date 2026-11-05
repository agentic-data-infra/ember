// Cuts an optimized multi-fragment plan into stages.
//
// Each Axiom fragment becomes one stage. A fragment that feeds another ends
// in a PartitionedOutput; it becomes a map stage whose root is a ShuffleWrite.
// An Exchange or MergeExchange reading another fragment stays as it is: its
// splits are the shuffle's block URLs. The fragment of the final stage returns
// rows to the driver.
//
// A stage's plan is serialized once, with Velox serde, and sent with each of
// its tasks: the task binary. Each stage gets a content key (StageKey.h).
//
// A scan of a cached table (CachedConnector.h) stays too, as a cached input
// reading the shuffle that stores the cached plan's output: task p of the
// stage reads every block of map outputs p, p + numTasks, ... A cached plan
// itself is planned with 'materialize' set, so that its last stage writes
// that shuffle instead of returning rows.
//
// A global sort without a limit runs as one merge task by default: the stage
// before it sorts its partitions, the result stage merges them. With
// ember.sort.range_partitions, Axiom plans a range shuffle and one sorting
// result task per range instead (ember/axiom-extension), and the driver
// concatenates the tasks' rows in partition order, which is sorted order.

#pragma once

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <folly/dynamic.h>

#include "axiom/optimizer/MultiFragmentPlan.h"
#include "ember/shuffle/ShuffleConfig.h"

namespace ember::driver {

struct Stage {
  /// The Axiom fragment id.
  int32_t stageId;

  /// Tasks the stage runs; std::nullopt for a source stage, whose task count
  /// comes from its splits.
  std::optional<int32_t> numTasks;

  /// The plan: the task binary, and its deserialized form.
  folly::dynamic fragment;
  facebook::velox::core::PlanNodePtr plan;

  /// Stages whose shuffle output this stage reads.
  std::vector<int32_t> parents;

  /// Shuffle this stage writes (map stages only). Shuffle ids start as the
  /// ids of the stages writing them; the scheduler renumbers them to be
  /// unique in its session (StagePlan::renumberShuffles).
  std::optional<int32_t> shuffleId;

  /// Scans of the original fragment, which the driver enumerates splits for.
  std::vector<facebook::velox::core::TableScanNodePtr> scans;

  /// Ids of exchanges and cached scans, and the shuffle each reads.
  /// 'broadcast' reads take partition 0 of every map output.
  struct ShuffleInput {
    facebook::velox::core::PlanNodeId nodeId;
    int32_t shuffleId;
    bool broadcast;
    /// Set for a read of a cached table. Its shuffle id is a negative
    /// placeholder until the scheduler knows the stored shuffle.
    std::optional<std::string> cacheId;
    /// The key of the cached output, if it has one.
    std::optional<std::string> cacheKey;
  };
  std::vector<ShuffleInput> shuffleInputs;

  /// Content hash of the stage and everything upstream of it (StageKey.h);
  /// std::nullopt if the stage is nondeterministic or reads inputs whose
  /// identity is unknown, and then 'noKeyReason' says why.
  std::optional<std::string> key;
  std::string noKeyReason;

  /// Keys of the plan's nodes, by node id. Nodes without a key are absent.
  std::map<facebook::velox::core::PlanNodeId, std::string> nodeKeys;
};

struct StagePlan {
  /// Stages in dependency order: each stage after the stages it reads.
  std::vector<Stage> stages;

  const Stage& resultStage() const {
    return stages.back();
  }

  /// One line per stage plus its plan, as explain() shows it. 'note' adds to
  /// a stage's key, e.g. that it reuses stored output.
  std::string toString(
      const std::function<std::string(const Stage&)>& note = nullptr) const;

  /// Changes shuffle ids, in the stages' fields and their task binaries, per
  /// 'newIds' (old id to new id; ids not in it stay).
  void renumberShuffles(const std::map<int32_t, int32_t>& newIds);
};

/// The key of the stored output of the cached table with the given id.
using CachedKeyResolver =
    std::function<std::optional<std::string>(const std::string&)>;

/// Cuts 'plan' into stages. 'pool' holds constants of the deserialized plans.
/// 'keyConfig' is the query config that can change results; it goes into the
/// stage keys. 'cachedKey' resolves scans of cached tables. With
/// 'materialize', the plan's output is written to a shuffle with one partition
/// per map output: the last stage is the map stage that writes it, and there
/// is no result stage. 'target' is where the stages' shuffles go.
StagePlan planStages(
    const facebook::axiom::optimizer::MultiFragmentPlan& plan,
    facebook::velox::memory::MemoryPool* pool,
    const std::map<std::string, std::string>& keyConfig = {},
    const CachedKeyResolver& cachedKey = nullptr,
    bool materialize = false,
    const shuffle::ShuffleTarget& target = {});

} // namespace ember::driver
