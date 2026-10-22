#include "ember/driver/StagePlan.h"

#include <map>
#include <set>
#include <sstream>

#include "ember/CachedTable.h"
#include "ember/FileSerde.h"
#include "ember/driver/StageKey.h"
#include "ember/shuffle/ShuffleRead.h"
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

core::TableScanNodePtr serializableScan(const core::TableScanNode& scan) {
  const auto* handle =
      dynamic_cast<const file::FileTableHandle*>(scan.tableHandle().get());
  VELOX_USER_CHECK_NOT_NULL(
      handle,
      "Only file scans can be scheduled, not {}",
      scan.tableHandle()->toString());
  velox::connector::ColumnHandleMap assignments;
  for (const auto& [name, column] : scan.assignments()) {
    assignments[name] =
        std::make_shared<SerializableFileColumnHandle>(column->name());
  }
  return std::make_shared<core::TableScanNode>(
      scan.id(),
      scan.outputType(),
      SerializableFileTableHandle::from(*handle),
      assignments);
}

// A scan of a cached table: a read of its stored output, projected to the
// scan's columns.
folly::dynamic cachedRead(
    const core::TableScanNode& scan,
    const std::string& id,
    const CachedOutputResolver& cachedOutput,
    int32_t& lastPlaceholder,
    Stage& stage) {
  VELOX_CHECK_NOT_NULL(cachedOutput, "No resolver for cached table {}", id);
  const auto output = cachedOutput(id);
  const auto shuffleId = --lastPlaceholder;
  stage.shuffleInputs.push_back(
      {scan.id(), shuffleId, false, id, output.key});
  auto read = std::make_shared<shuffle::ShuffleReadNode>(
      shuffleId,
      std::make_shared<core::ExchangeNode>(
          scan.id(), output.type, output.serdeKind));
  std::vector<std::string> names;
  std::vector<core::TypedExprPtr> columns;
  for (const auto& name : scan.outputType()->names()) {
    const auto index =
        output.schema->getChildIdx(scan.assignments().at(name)->name());
    names.push_back(name);
    columns.push_back(std::make_shared<core::FieldAccessTypedExpr>(
        output.type->childAt(index), output.type->nameOf(index)));
  }
  auto json = core::ProjectNode(
                  scan.id() + "_cached", std::move(names), std::move(columns), read)
                  .serialize();
  json["sources"][0] = read->serialize();
  return json;
}

// Serializes 'node', replacing exchanges with shuffle reads and file handles
// with serializable ones. Node types serialize their sources under "sources"
// in the order of sources(), so rewritten children are patched in there.
folly::dynamic rewrite(
    const core::PlanNodePtr& node,
    const std::map<core::PlanNodeId, ReadInfo>& reads,
    const CachedOutputResolver& cachedOutput,
    int32_t& lastPlaceholder,
    Stage& stage) {
  if (auto exchange = std::dynamic_pointer_cast<const core::ExchangeNode>(node)) {
    auto it = reads.find(node->id());
    VELOX_CHECK(
        it != reads.end(), "Exchange {} reads no fragment", node->id());
    stage.shuffleInputs.push_back(
        {node->id(), it->second.shuffleId, it->second.broadcast});
    return shuffle::ShuffleReadNode(it->second.shuffleId, exchange).serialize();
  }
  if (auto scan = std::dynamic_pointer_cast<const core::TableScanNode>(node)) {
    if (const auto* handle = dynamic_cast<const file::FileTableHandle*>(
            scan->tableHandle().get());
        handle != nullptr &&
        handle->schemaTableName().schema == kCachedSchema) {
      return cachedRead(
          *scan, handle->filePath(), cachedOutput, lastPlaceholder, stage);
    }
    stage.scans.push_back(scan);
    return serializableScan(*scan)->serialize();
  }
  auto json = node->serialize();
  const auto& sources = node->sources();
  for (size_t i = 0; i < sources.size(); ++i) {
    json["sources"][i] =
        rewrite(sources[i], reads, cachedOutput, lastPlaceholder, stage);
  }
  return json;
}

void visit(
    const optimizer::ExecutableFragment& fragment,
    const std::map<int32_t, const optimizer::ExecutableFragment*>& byId,
    std::set<int32_t>& visited,
    std::vector<const optimizer::ExecutableFragment*>& order) {
  if (!visited.insert(fragment.fragmentId).second) {
    return;
  }
  for (const auto& input : fragment.inputStages) {
    visit(*byId.at(input.producerFragmentId), byId, visited, order);
  }
  order.push_back(&fragment);
}

// Makes the last stage of 'plan' write its output to a shuffle, one partition
// per task. A last stage that only gathers its input is dropped, and the
// stage before it, which writes one partition per task already, is last.
void materializeOutput(StagePlan& plan, velox::memory::MemoryPool* pool) {
  auto& last = plan.stages.back();
  const auto* gather = last.plan->as<shuffle::ShuffleReadNode>();
  if (gather != nullptr && !gather->isMerge() &&
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
      last.plan);
  auto fragment = write->serialize();
  fragment["sources"][0] = std::move(last.fragment);
  last.fragment = std::move(fragment);
  last.plan =
      velox::ISerializable::deserialize<core::PlanNode>(last.fragment, pool);
}

} // namespace

StagePlan planStages(
    const optimizer::MultiFragmentPlan& plan,
    velox::memory::MemoryPool* pool,
    const std::map<std::string, std::string>& keyConfig,
    const CachedOutputResolver& cachedOutput,
    bool materialize) {
  const auto& fragments = plan.fragments();
  VELOX_CHECK(!fragments.empty());

  std::map<int32_t, const optimizer::ExecutableFragment*> byId;
  std::map<int32_t, const optimizer::ExecutableFragment*> consumers;
  for (const auto& fragment : fragments) {
    VELOX_USER_CHECK(
        fragment.groupedNodes.empty(),
        "Grouped execution is not supported yet");
    VELOX_USER_CHECK(
        fragment.sampledScans.empty(), "Sampled scans are not supported yet");
    byId[fragment.fragmentId] = &fragment;
    for (const auto& input : fragment.inputStages) {
      VELOX_CHECK(
          consumers.emplace(input.producerFragmentId, &fragment).second,
          "Fragment {} has more than one consumer",
          input.producerFragmentId);
    }
  }

  auto numTasks =
      [](const optimizer::ExecutableFragment& f) -> std::optional<int32_t> {
    if (f.numRemotePartitions.has_value()) {
      return *f.numRemotePartitions;
    }
    if (f.type == optimizer::FragmentType::kSource) {
      return std::nullopt;
    }
    return 1;
  };

  // The result fragment is last; everything it needs comes before it.
  std::set<int32_t> visited;
  std::vector<const optimizer::ExecutableFragment*> order;
  visit(fragments.back(), byId, visited, order);
  VELOX_CHECK_EQ(
      order.size(), fragments.size(), "Fragments unreachable from the result");

  StagePlan result;
  int32_t lastPlaceholder = 0;
  for (const auto* fragment : order) {
    Stage stage;
    stage.stageId = fragment->fragmentId;
    stage.numTasks = numTasks(*fragment);

    std::map<core::PlanNodeId, ReadInfo> reads;
    for (const auto& input : fragment->inputStages) {
      const auto& producer = *byId.at(input.producerFragmentId);
      const auto* output = producer.fragment.planNode
                               ->as<core::PartitionedOutputNode>();
      VELOX_CHECK_NOT_NULL(
          output, "Fragment {} does not end in an output", producer.fragmentId);
      reads[input.consumerNodeId] = {
          input.producerFragmentId, output->isBroadcast()};
      stage.parents.push_back(input.producerFragmentId);
    }

    const auto& root = fragment->fragment.planNode;
    if (auto it = consumers.find(fragment->fragmentId); it != consumers.end()) {
      const auto* output = root->as<core::PartitionedOutputNode>();
      VELOX_CHECK_NOT_NULL(output);
      int32_t numPartitions = output->numPartitions();
      if (output->isArbitrary()) {
        auto consumerTasks = numTasks(*it->second);
        VELOX_USER_CHECK(
            consumerTasks.has_value(),
            "Arbitrary output into a source fragment is not supported");
        numPartitions = *consumerTasks;
      }
      stage.shuffleId = fragment->fragmentId;
      auto write = shuffle::ShuffleWriteNode::fromPartitionedOutput(
          *output, *stage.shuffleId, numPartitions);
      stage.fragment = write->serialize();
      stage.fragment["sources"][0] = rewrite(
          output->sources()[0], reads, cachedOutput, lastPlaceholder, stage);
    } else {
      VELOX_USER_CHECK(
          fragment == order.back(),
          "Fragment {} has no consumer",
          fragment->fragmentId);
      VELOX_USER_CHECK_NULL(
          root->as<core::PartitionedOutputNode>(),
          "Remote output is not supported");
      stage.fragment =
          rewrite(root, reads, cachedOutput, lastPlaceholder, stage);
    }

    // The executor's view of the plan; also checks that it round-trips.
    stage.plan =
        velox::ISerializable::deserialize<core::PlanNode>(stage.fragment, pool);
    result.stages.push_back(std::move(stage));
  }
  VELOX_USER_CHECK_EQ(
      result.resultStage().numTasks.value_or(0),
      1,
      "The result stage must run one task");
  if (materialize) {
    materializeOutput(result, pool);
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

namespace {

void renumberNode(
    folly::dynamic& node,
    const std::map<int32_t, int32_t>& newIds) {
  const auto& name = node["name"].getString();
  if (name == "ShuffleReadNode" || name == "ShuffleWriteNode") {
    if (auto it = newIds.find(node["shuffleId"].asInt()); it != newIds.end()) {
      node["shuffleId"] = it->second;
    }
  }
  if (auto* sources = node.get_ptr("sources")) {
    for (auto& source : *sources) {
      renumberNode(source, newIds);
    }
  }
}

} // namespace

void StagePlan::renumberShuffles(
    const std::map<int32_t, int32_t>& newIds,
    velox::memory::MemoryPool* pool) {
  auto renumber = [&](int32_t& id) {
    if (auto it = newIds.find(id); it != newIds.end()) {
      id = it->second;
    }
  };
  for (auto& stage : stages) {
    if (stage.shuffleId.has_value()) {
      renumber(*stage.shuffleId);
    }
    for (auto& input : stage.shuffleInputs) {
      renumber(input.shuffleId);
    }
    renumberNode(stage.fragment, newIds);
    stage.plan =
        velox::ISerializable::deserialize<core::PlanNode>(stage.fragment, pool);
  }
}

} // namespace ember::driver
