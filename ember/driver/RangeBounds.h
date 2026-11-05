// RangeBounds: the split points of a range-partitioned sort, taken from a
// sample of its keys as Spark's RangePartitioner does. The plan names a range shuffle without split points. Before a stage that writes
// one runs, the scheduler runs its tasks once with sampleFragment(), which
// returns a sample of keys of each task instead of writing the shuffle, and
// computes the split points with rangeBounds(). withBounds() is the stage's
// fragment with them.

#pragma once

#include <string>
#include <vector>

#include "ember/driver/StagePlan.h"
#include "velox/vector/ComplexVector.h"

namespace ember::driver {

/// Whether 'stage' writes a range shuffle of several partitions.
bool writesRanges(const Stage& stage);

/// The number of partitions of the range shuffle 'stage' writes.
int32_t numRanges(const Stage& stage);

/// A fragment that runs the input of the range shuffle 'stage' writes and
/// returns one row with a sample of its keys. 'numTasks' tasks run it.
folly::dynamic sampleFragment(const Stage& stage, int32_t numTasks);

/// The rows sampleFragment() returns.
facebook::velox::RowTypePtr sampleRowType();

/// At most 'numRanges' - 1 split points in ascending key order, which cut the
/// sampled rows of all tasks into ranges of about the same number of rows. A
/// task's sample stands for all the rows it read.
std::vector<std::string> rangeBounds(
    const std::vector<facebook::velox::RowVectorPtr>& samples,
    int32_t numRanges);

/// The fragment of 'stage' with split points 'bounds'.
folly::dynamic withBounds(
    const Stage& stage,
    const std::vector<std::string>& bounds);

} // namespace ember::driver
