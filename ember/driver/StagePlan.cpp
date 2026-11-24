#include "ember/driver/StagePlan.h"

#include <map>
#include <set>
#include <sstream>

#include "axiom/connectors/file/core/FileTable.h"
#include "ember/CachedConnector.h"
#include "ember/ParquetConnector.h"
#include "ember/driver/StageKey.h"
#include "ember/shuffle/ShuffleWrite.h"

namespace ember::driver {

namespace core = facebook::velox::core;
namespace file = facebook::axiom::connector::file;
namespace optimizer = facebook::axiom::optimizer;
namespace velox = facebook::velox;

namespace {

struct ReadInfo {
  int32_t shuffleId;
  bool broadcast;
};

// Notes what 'node' and its sources read: the shuffle of each exchange, the
// stored output of each cached table and the files of each other scan.
void collectInputs(
    const core::PlanNodePtr& node,
    const std::map<core::PlanNodeId, ReadInfo>& reads,
    const CachedKeyResolver& cachedKey,
    int32_t& lastPlaceholder,
    Stage& stage) {
  if (node->is<core::ExchangeNode>()) {
    auto it = reads.find(node->id());
    VELOX_CHECK(
        it != reads.end(), "Exchange {} reads no fragment", node->id());
    stage.shuffleInputs.push_back(
        {node->id(), it->second.shuffleId, it->second.broadcast});
    return;
  }
  if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
    const auto& handle = scan->tableHandle();
    if (handle->connectorId() == kCachedConnectorId) {
      VELOX_CHECK_NOT_NULL(
          cachedKey, "No resolver for cached table {}", handle->name());
      stage.shuffleInputs.push_back(
          {scan->id(),
           --lastPlaceholder,
           false,
           handle->name(),
           cachedKey(handle->name())});
      return;
    }
    VELOX_USER_CHECK(
        handle->connectorId() == kParquetConnectorId ||
            dynamic_cast<const file::FileTableHandle*>(handle.get()) != nullptr,
        "Only file scans can be scheduled, not {}",
        handle->toString());
    stage.scans.push_back(std::move(scan));
    return;
  }
  for (const auto& source : node->sources()) {
    collectInputs(source, reads, cachedKey, lastPlaceholder, stage);
  }
}

void visit(
    const core::MultiStagePlan& plan,
    const core::Stage& stage,
    std::set<int32_t>& visited,
    std::vector<const core::Stage*>& order) {
  if (!visited.insert(stage.id).second) {
    return;
  }
  for (const auto& input : stage.inputs) {
    visit(plan, plan.stage(input.producerStageId), visited, order);
  }
  order.push_back(&stage);
}

// Makes the last stage of 'plan' write its output to a shuffle, one partition
// per task. A last stage that only gathers its input is dropped, and the
// stage before it, which writes one partition per task already, is last.
void materializeOutput(StagePlan& plan, const shuffle::ShuffleTarget& target) {
  auto& last = plan.stages.back();
  if (last.plan->is<core::ExchangeNode>() &&
      !last.plan->is<core::MergeExchangeNode>() &&
      last.shuffleInputs.size() == 1 && !last.shuffleInputs[0].broadcast &&
      !last.shuffleInputs[0].cacheId.has_value()) {
    plan.stages.pop_back();
    return;
  }
  last.shuffleId = last.stageId;
  auto write = std::make_shared<shuffle::ShuffleWriteNode>(
      last.plan->id() + "_cached",
      *last.shuffleId,
      core::PartitionedOutputNode::Kind::kPartitioned,
      std::vector<core::TypedExprPtr>{},
      1,
      std::make_shared<core::GatherPartitionFunctionSpec>(),
      last.plan->outputType(),
      "Presto",
      target,
      last.plan);
  last.fragment = write->serialize();
  last.plan = std::move(write);
}

} // namespace

StagePlan planStages(
    const optimizer::MultiFragmentPlan& plan,
    velox::memory::MemoryPool* pool,
    const std::map<std::string, std::string>& keyConfig,
    const CachedKeyResolver& cachedKey,
    bool materialize,
    const shuffle::ShuffleTarget& target) {
  VELOX_CHECK(!plan.empty());
  const auto& stagePlan = plan.stagePlan();
  for (const auto& stage : stagePlan.stages()) {
    const auto& scans = plan.scans(stage.id);
    VELOX_USER_CHECK(
        scans.groupedNodes.empty(), "Grouped execution is not supported yet");
    VELOX_USER_CHECK(
        scans.sampledScans.empty(), "Sampled scans are not supported yet");
  }

  auto numTasks = [](const core::Stage& stage) -> std::optional<int32_t> {
    if (stage.numRemotePartitions.has_value()) {
      return *stage.numRemotePartitions;
    }
    if (stage.type == core::StageType::kSource) {
      return std::nullopt;
    }
    return 1;
  };

  // The root stage is last; everything it needs comes before it.
  std::set<int32_t> visited;
  std::vector<const core::Stage*> order;
  visit(stagePlan, stagePlan.root(), visited, order);
  VELOX_CHECK_EQ(
      order.size(),
      stagePlan.stages().size(),
      "Stages unreachable from the result");

  StagePlan result;
  int32_t lastPlaceholder = 0;
  for (const auto* axiomStage : order) {
    Stage stage;
    stage.stageId = axiomStage->id;
    stage.numTasks = numTasks(*axiomStage);

    std::map<core::PlanNodeId, ReadInfo> reads;
    for (const auto& input : axiomStage->inputs) {
      const auto* output = stagePlan.stage(input.producerStageId)
                               .plan->as<core::PartitionedOutputNode>();
      VELOX_CHECK_NOT_NULL(
          output, "Stage {} does not end in an output", input.producerStageId);
      reads[input.exchangeId] = {input.producerStageId, output->isBroadcast()};
      stage.parents.push_back(input.producerStageId);
    }

    const auto& root = axiomStage->plan;
    const auto* output = root->as<core::PartitionedOutputNode>();
    // With remote output (a range-sorted result), the scheduler collects the
    // rows of each task.
    auto plan = output != nullptr ? output->sources()[0] : root;
    collectInputs(plan, reads, cachedKey, lastPlaceholder, stage);
    if (const auto* consumer = stagePlan.consumer(axiomStage->id)) {
      VELOX_CHECK_NOT_NULL(output);
      int32_t numPartitions = output->numPartitions();
      if (output->isArbitrary()) {
        auto consumerTasks = numTasks(*consumer);
        VELOX_USER_CHECK(
            consumerTasks.has_value(),
            "Arbitrary output into a source fragment is not supported");
        numPartitions = *consumerTasks;
      }
      stage.shuffleId = axiomStage->id;
      plan = shuffle::ShuffleWriteNode::fromPartitionedOutput(
          *output, *stage.shuffleId, numPartitions, target);
    }

    stage.fragment = plan->serialize();
    // The executor's view of the plan; also checks that it round-trips.
    stage.plan =
        velox::ISerializable::deserialize<core::PlanNode>(stage.fragment, pool);
    result.stages.push_back(std::move(stage));
  }
  VELOX_USER_CHECK(
      result.resultStage().numTasks.has_value(),
      "The result stage must have a fixed task count");
  if (materialize) {
    materializeOutput(result, target);
  }
  computeStageKeys(result, keyConfig);
  return result;
}

std::string StagePlan::toString(
    const std::function<std::string(const Stage&)>& note) const {
  std::stringstream out;
  for (const auto& stage : stages) {
    out << "Stage " << stage.stageId;
    const auto extra = note != nullptr ? note(stage) : std::string();
    if (stage.key.has_value()) {
      out << " [key " << shortKey(*stage.key) << extra << "]: ";
    } else {
      out << " [no key: " << stage.noKeyReason << extra << "]: ";
    }
    if (stage.numTasks.has_value()) {
      out << *stage.numTasks << (*stage.numTasks == 1 ? " task" : " tasks");
    } else {
      out << "one task per split";
    }
    if (!stage.parents.empty()) {
      out << ", after stage" << (stage.parents.size() > 1 ? "s" : "");
      for (size_t i = 0; i < stage.parents.size(); ++i) {
        out << (i > 0 ? ", " : " ") << stage.parents[i];
      }
    }
    out << std::endl << stage.plan->toString(true, true) << std::endl;
  }
  return out.str();
}

void StagePlan::renumberShuffles(const std::map<int32_t, int32_t>& newIds) {
  auto renumber = [&](int32_t& id) {
    if (auto it = newIds.find(id); it != newIds.end()) {
      id = it->second;
    }
  };
  for (auto& stage : stages) {
    if (stage.shuffleId.has_value()) {
      renumber(*stage.shuffleId);
      const auto* write = stage.plan->as<shuffle::ShuffleWriteNode>();
      VELOX_CHECK_NOT_NULL(write);
      stage.plan = write->withShuffleId(*stage.shuffleId);
      stage.fragment = stage.plan->serialize();
    }
    for (auto& input : stage.shuffleInputs) {
      renumber(input.shuffleId);
    }
  }
}

} // namespace ember::driver
