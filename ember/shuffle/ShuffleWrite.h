// ShuffleWrite: the root of a map stage's fragment. Partitions its input like
// Velox's PartitionedOutput and writes it to the executor's shuffle store
// instead of in-memory output buffers.

#pragma once

#include "ember/shuffle/ShuffleConfig.h"
#include "velox/core/PlanNode.h"
#include "velox/exec/Operator.h"

namespace ember::shuffle {

class ShuffleWriteNode : public facebook::velox::core::PlanNode {
 public:
  using Kind = facebook::velox::core::PartitionedOutputNode::Kind;

  /// Writes 'source' to shuffle 'shuffleId' in 'numPartitions' reduce
  /// partitions. 'kind', 'keys', 'partitionFunctionSpec', 'outputType' and
  /// 'serdeKind' mean what they mean on PartitionedOutputNode. A broadcast
  /// writes one partition, which every reducer reads. 'target' says where the
  /// output goes.
  ShuffleWriteNode(
      const facebook::velox::core::PlanNodeId& id,
      int32_t shuffleId,
      Kind kind,
      std::vector<facebook::velox::core::TypedExprPtr> keys,
      int32_t numPartitions,
      facebook::velox::core::PartitionFunctionSpecPtr partitionFunctionSpec,
      facebook::velox::RowTypePtr outputType,
      std::string serdeKind,
      ShuffleTarget target,
      facebook::velox::core::PlanNodePtr source);

  /// A ShuffleWriteNode with the partitioning of 'output', reading from its
  /// source.
  static std::shared_ptr<const ShuffleWriteNode> fromPartitionedOutput(
      const facebook::velox::core::PartitionedOutputNode& output,
      int32_t shuffleId,
      int32_t numPartitions,
      ShuffleTarget target);

  int32_t shuffleId() const {
    return shuffleId_;
  }

  Kind kind() const {
    return kind_;
  }

  const std::vector<facebook::velox::core::TypedExprPtr>& keys() const {
    return keys_;
  }

  int32_t numPartitions() const {
    return numPartitions_;
  }

  const facebook::velox::core::PartitionFunctionSpecPtr& partitionFunctionSpec()
      const {
    return partitionFunctionSpec_;
  }

  const std::string& serdeKind() const {
    return serdeKind_;
  }

  const ShuffleTarget& target() const {
    return target_;
  }

  const facebook::velox::RowTypePtr& outputType() const override {
    return outputType_;
  }

  const std::vector<facebook::velox::core::PlanNodePtr>& sources()
      const override {
    return sources_;
  }

  std::string_view name() const override {
    return "ShuffleWrite";
  }

  /// This node, writing shuffle 'shuffleId'.
  std::shared_ptr<const ShuffleWriteNode> withShuffleId(int32_t shuffleId) const;

  /// A copy of this node that partitions with 'spec'.
  std::shared_ptr<const ShuffleWriteNode> withPartitionFunction(
      facebook::velox::core::PartitionFunctionSpecPtr spec) const;

  /// A copy of this node that writes to Kiln bucket 'bucket'.
  std::shared_ptr<const ShuffleWriteNode> withKilnBucket(uint64_t bucket) const;

  folly::dynamic serialize() const override;

  static facebook::velox::core::PlanNodePtr create(
      const folly::dynamic& obj,
      void* context);

 private:
  void addDetails(std::stringstream& stream) const override;

  const int32_t shuffleId_;
  const Kind kind_;
  const std::vector<facebook::velox::core::TypedExprPtr> keys_;
  const int32_t numPartitions_;
  const facebook::velox::core::PartitionFunctionSpecPtr partitionFunctionSpec_;
  const facebook::velox::RowTypePtr outputType_;
  const std::string serdeKind_;
  const ShuffleTarget target_;
  const std::vector<facebook::velox::core::PlanNodePtr> sources_;
};

class ShuffleWriteTranslator
    : public facebook::velox::exec::Operator::PlanNodeTranslator {
 public:
  std::unique_ptr<facebook::velox::exec::Operator> toOperator(
      facebook::velox::exec::DriverCtx* ctx,
      int32_t id,
      const facebook::velox::core::PlanNodePtr& node) override;
};

} // namespace ember::shuffle
