// Cuts an optimized multi-fragment plan into stages.
//
// Each Axiom fragment becomes one stage. A fragment that feeds another ends
// in a PartitionedOutput; it becomes a map stage whose root is a ShuffleWrite.
// Each Exchange or MergeExchange reading another fragment becomes a
// ShuffleRead. The fragment of the final stage returns rows to the driver.
//
// A stage's plan is serialized once, with Velox serde, and sent with each of
// its tasks: the task binary. File scans are rewritten to serializable
// handles on the way.

#pragma once

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

  /// Shuffle this stage writes (map stages only). Shuffle ids are the ids of
  /// the stages writing them.
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
};

struct StagePlan {
  /// Stages in dependency order: each stage after the stages it reads.
  std::vector<Stage> stages;

  const Stage& resultStage() const {
    return stages.back();
  }

  /// One line per stage plus its plan, as explain() shows it.
  std::string toString() const;
};

/// Cuts 'plan' into stages. 'pool' holds constants of the deserialized plans.
StagePlan planStages(
    const facebook::axiom::optimizer::MultiFragmentPlan& plan,
    facebook::velox::memory::MemoryPool* pool);

} // namespace ember::driver
