// DagScheduler: runs a job, i.e. one optimized plan, stage by stage.
//
// It cuts the plan into stages (StagePlan.h) and submits each stage after the
// stages it reads, as a task set on the TaskScheduler. Source stages run one
// task per input split; the driver enumerates splits through Axiom's split
// managers and cuts text files into byte ranges. Map stages register their
// outputs with the MapOutputTracker, which gives each reduce task its blocks.
// The result stage's rows come back with its task result.
//
// Exact reuse: a map stage with a key keeps its shuffle output after the job,
// in a memo by key. A later job's stage with the same key reads the stored
// output instead of running; stages only it reads do not run either. Stored
// outputs lost with their executor are recomputed as in any job.
//
// Cached Dataframes: a stage that reads a cached table (CachedTable.h) first
// materializes it, as a job of its own over the cached plan whose last stage
// writes one shuffle partition per task. That output stays in the memo, under
// its stage key (or the table's id, if it has none), whether or not other
// outputs are kept; later jobs, and lost outputs, rerun only what is missing.

#pragma once

#include <atomic>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>

#include "axiom/optimizer/MultiFragmentPlan.h"
#include "ember/driver/MapOutputTracker.h"
#include "ember/driver/StagePlan.h"
#include "ember/driver/TaskScheduler.h"

namespace ember::driver {

/// One attempt of a stage, as reported to DagSchedulerOptions::onStageCompleted.
struct StageAttemptInfo {
  std::string jobId;
  int32_t stageId{0};
  int32_t attempt{0};
  /// The stage's content key, if it has one (StageKey.h).
  std::optional<std::string> stageKey;
  /// The shuffle it writes, for a map stage.
  std::optional<int32_t> shuffleId;
  /// The stage read stored outputs of an earlier job instead of running; then
  /// 'partitions' is empty.
  bool reused{false};
  /// Partitions (map ids, for a map stage) the attempt ran.
  std::vector<int32_t> partitions;
  /// Where each map output the attempt committed lives, by map id.
  std::map<int32_t, std::string> outputLocations;
  /// Blocks its tasks could not fetch.
  std::vector<shuffle::FetchFailure> fetchFailures;
};

struct DagSchedulerOptions {
  std::string appName{"ember"};
  /// Tasks per stage when nothing else decides it, and the target number of
  /// splits of a file scan.
  int32_t parallelism{1};
  int32_t maxTaskAttempts{4};
  /// Attempts of a stage that may end with partitions missing (their inputs
  /// were lost, or their outputs) before the job fails.
  int32_t maxStageAttempts{4};
  /// Velox query config plus Ember's keys, passed to every task.
  std::map<std::string, std::string> config;
  /// Called on the job's thread after each stage attempt.
  std::function<void(const StageAttemptInfo&)> onStageCompleted;
};

class DagScheduler {
 public:
  /// Config key: largest byte range of a text file one task reads. By
  /// default, a file scan's bytes spread over 'parallelism' tasks, with
  /// ranges between 4MB and 128MB.
  static constexpr const char* kMaxSplitBytes = "ember.files.max_split_bytes";
  /// Config key: keep keyed shuffle outputs for later jobs to reuse (default
  /// true).
  static constexpr const char* kReuseOutputs = "ember.reuse.outputs";

  DagScheduler(
      std::shared_ptr<SchedulerBackend> backend,
      DagSchedulerOptions options);
  ~DagScheduler();

  /// Runs 'plan' and returns its rows, allocated from 'pool'.
  std::vector<facebook::velox::RowVectorPtr> runJob(
      const facebook::axiom::optimizer::MultiFragmentPlan& plan,
      facebook::velox::memory::MemoryPool* pool);

  /// The stages 'plan' runs as.
  StagePlan stagePlan(const facebook::axiom::optimizer::MultiFragmentPlan& plan);

  /// The stages 'plan' runs as, marking those that would reuse stored output.
  std::string explain(const facebook::axiom::optimizer::MultiFragmentPlan& plan);

  /// Makes sure the output of cached table 'id' is stored, running its plan
  /// if needed, and returns the shuffle that holds it.
  int32_t materialize(const std::string& id);

  /// Drops the stored output of cached table 'id'. Returns false if none was
  /// stored.
  bool uncache(const std::string& id);

  SchedulerBackend& backend() {
    return taskScheduler_.backend();
  }

 private:
  struct JobContext;
  struct StageState;

  // Runs 'stages' as one job. The last stage is the result stage, whose rows
  // are returned, or, with 'outputKey', a map stage whose output is kept in
  // the memo under that key; then 'outputShuffle' is set to its shuffle.
  std::vector<facebook::velox::RowVectorPtr> runStages(
      StagePlan& stages,
      const std::optional<std::string>& outputKey,
      facebook::velox::memory::MemoryPool* pool,
      int32_t* outputShuffle = nullptr);

  // Runs 'stage' until all its partitions are done, first rerunning lost
  // outputs of the stages it reads.
  void runStage(StageState& stage, JobContext& job);

  // The stages of cached table 'id', planned to store its output.
  StagePlan cachedStagePlan(const std::string& id);

  CachedOutput cachedOutput(const std::string& id);

  // The memo key of a cached table's output.
  static std::string cacheMemoKey(
      const std::string& id,
      const std::optional<std::string>& key) {
    return key.value_or("cached " + id);
  }

  // Serialized splits for each task of a source stage, per scan node.
  std::vector<std::map<std::string, std::vector<folly::dynamic>>> scanSplits(
      const Stage& stage,
      JobContext& job);

  // The shuffle stored under 'key', if it can be reused by a stage with
  // 'numTasks' tasks: one with as many map outputs.
  std::optional<int32_t> storedShuffle(
      const std::optional<std::string>& key,
      int32_t numTasks);

  const DagSchedulerOptions options_;
  const bool reuseOutputs_;
  // Config that goes into stage keys: what is not Ember's own, and splits.
  std::map<std::string, std::string> keyConfig_;
  TaskScheduler taskScheduler_;
  MapOutputTracker mapOutputTracker_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> planPool_;
  std::atomic<int64_t> jobCounter_{0};
  std::atomic<int32_t> shuffleCounter_{0};

  // Stored shuffle outputs by stage key. They stay registered with the
  // tracker, so losses are tracked as for any shuffle.
  std::mutex memoMutex_;
  std::map<std::string, int32_t> memo_;
};

} // namespace ember::driver
