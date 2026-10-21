// DagScheduler: runs a job, i.e. one optimized plan, stage by stage.
//
// It cuts the plan into stages (StagePlan.h) and submits each stage after the
// stages it reads, as a task set on the TaskScheduler. Source stages run one
// task per input split; the driver enumerates splits through Axiom's split
// managers and cuts text files into byte ranges. Map stages register their
// outputs with the MapOutputTracker, which gives each reduce task its blocks.
// The result stage's rows come back with its task result.

#pragma once

#include <atomic>
#include <map>
#include <string>

#include "axiom/optimizer/MultiFragmentPlan.h"
#include "ember/driver/MapOutputTracker.h"
#include "ember/driver/StagePlan.h"
#include "ember/driver/TaskScheduler.h"

namespace ember::driver {

struct DagSchedulerOptions {
  std::string appName{"ember"};
  /// Tasks per stage when nothing else decides it, and the target number of
  /// splits of a file scan.
  int32_t parallelism{1};
  int32_t maxTaskAttempts{4};
  /// Velox query config plus Ember's keys, passed to every task.
  std::map<std::string, std::string> config;
};

class DagScheduler {
 public:
  /// Config key: largest byte range of a text file one task reads. By
  /// default, a file scan's bytes spread over 'parallelism' tasks, with
  /// ranges between 4MB and 128MB.
  static constexpr const char* kMaxSplitBytes = "ember.files.max_split_bytes";

  DagScheduler(
      std::shared_ptr<SchedulerBackend> backend,
      DagSchedulerOptions options);

  /// Runs 'plan' and returns its rows, allocated from 'pool'.
  std::vector<facebook::velox::RowVectorPtr> runJob(
      const facebook::axiom::optimizer::MultiFragmentPlan& plan,
      facebook::velox::memory::MemoryPool* pool);

  /// The stages 'plan' runs as.
  StagePlan stagePlan(const facebook::axiom::optimizer::MultiFragmentPlan& plan);

  SchedulerBackend& backend() {
    return taskScheduler_.backend();
  }

 private:
  struct JobContext;

  // Serialized splits for each task of a source stage, per scan node.
  std::vector<std::map<std::string, std::vector<folly::dynamic>>> scanSplits(
      const Stage& stage,
      JobContext& job);

  const DagSchedulerOptions options_;
  TaskScheduler taskScheduler_;
  MapOutputTracker mapOutputTracker_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> planPool_;
  std::atomic<int64_t> jobCounter_{0};
};

} // namespace ember::driver
