// Cuts an optimized multi-fragment plan into stages.
//
// Each Axiom fragment becomes one stage. A fragment that feeds another ends
// in a PartitionedOutput; it becomes a map stage whose root is a ShuffleWrite.
// Each Exchange or MergeExchange reading another fragment becomes a
// ShuffleRead. The fragment of the final stage returns rows to the driver.
//
// A stage's plan is serialized once, with Velox serde, and sent with each of
// its tasks: the task binary. File scans are rewritten to serializable
// handles on the way. Each stage gets a content key (StageKey.h).

#pragma once

#include <functional>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include <folly/dynamic.h>

#include "axiom/optimizer/MultiFragmentPlan.h"

namespace ember::driver {

struct Stage {
  /// The Axiom fragment id.
  int32_t stageId;

  /// Tasks the stage runs; std::nullopt for a source stage, whose task count
  /// comes from its splits.
  std::optional<int32_t> numTasks;

  /// Rewritten plan: the task binary, and its deserialized form.
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

  /// ShuffleRead node ids and the shuffle each reads. 'broadcast' reads take
  /// partition 0 of every map output.
  struct ShuffleInput {
    facebook::velox::core::PlanNodeId nodeId;
    int32_t shuffleId;
    bool broadcast;
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
  /// a keyed stage's key, e.g. that it reuses stored output.
  std::string toString(
      const std::function<std::string(const Stage&)>& note = nullptr) const;

  /// Changes shuffle ids, in the stages' fields and their task binaries, per
  /// 'newIds' (old id to new id; ids not in it stay).
  void renumberShuffles(
      const std::map<int32_t, int32_t>& newIds,
      facebook::velox::memory::MemoryPool* pool);
};

/// Cuts 'plan' into stages. 'pool' holds constants of the deserialized plans.
/// 'keyConfig' is the query config that can change results; it goes into the
/// stage keys.
StagePlan planStages(
    const facebook::axiom::optimizer::MultiFragmentPlan& plan,
    facebook::velox::memory::MemoryPool* pool,
    const std::map<std::string, std::string>& keyConfig = {});

} // namespace ember::driver
