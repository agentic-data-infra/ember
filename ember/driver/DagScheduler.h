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
// outputs lost with their executor are recomputed as in any job. With
// ember.reuse.kiln, kept outputs in Kiln also go into Kiln's catalog, where
// later programs find them (KilnMemo.h).
//
// With ember.shuffle.store=kiln, each shuffle the session writes goes into
// a Kiln bucket of its own, which is deleted with the shuffle: after its job,
// or for kept outputs, when they are replaced, uncached, or the session ends.
// Outputs stored in Kiln's catalog keep their chunks until Kiln evicts them.
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
#include "ember/driver/KilnMemo.h"
#include "ember/driver/MapOutputTracker.h"
#include "ember/driver/StagePlan.h"
#include "ember/driver/TaskScheduler.h"

namespace kiln::client {
class Client;
}

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
  /// Config key: largest byte range of a file one task reads. By default, a
  /// file scan's bytes spread over 'parallelism' tasks, with ranges between
  /// 4MB and 128MB. A text range reads the lines that start in it, a Parquet
  /// range the row groups. With one scan in a stage, a task takes ranges
  /// until it holds about this many bytes, each range counting 4MB extra.
  static constexpr const char* kMaxSplitBytes = "ember.files.max_split_bytes";
  /// Config key: file scans read files local to each node (default false).
  /// Every node has its own files at the scan's path, as each machine's local
  /// disks do; the driver asks a worker on each node for them, and their
  /// tasks run only on that node. The driver must see the path too, to plan
  /// the scan, and the nodes are those with workers when the job starts.
  /// Only stages with one scan; their stages have no keys.
  static constexpr const char* kNodeLocalFiles = "ember.files.node_local";
  /// Config key: keep keyed shuffle outputs for later jobs to reuse (default
  /// true).
  static constexpr const char* kReuseOutputs = "ember.reuse.outputs";
  /// Config key: also store kept outputs in Kiln's catalog, and look stage
  /// keys up there, so that later programs reuse them (KilnMemo.h; default
  /// false). Needs the Kiln shuffle store.
  static constexpr const char* kReuseInKiln = "ember.reuse.kiln";
  /// Config key: run a global sort without a limit as this many range-sorted
  /// result tasks instead of one merge task (StagePlan.h; default 0, off).
  /// The ranges are fixed splits of the first sort key's bytes, which suits
  /// uniformly random keys.
  static constexpr const char* kRangeSortPartitions =
      "ember.sort.range_partitions";

  DagScheduler(
      std::shared_ptr<SchedulerBackend> backend,
      DagSchedulerOptions options);
  ~DagScheduler();

  /// Runs 'plan' and returns its rows, allocated from 'pool'.
  std::vector<facebook::velox::RowVectorPtr> runJob(
      const facebook::axiom::optimizer::MultiFragmentPlan& plan,
      facebook::velox::memory::MemoryPool* pool);

  /// Runs 'plan' with each result task writing its rows to a part file of
  /// 'files' (TaskDescription.h). Returns the number of rows written. A
  /// kiln:// directory names a dataset that must not exist yet; it appears
  /// when the job succeeds, with all its parts (KilnFiles.h).
  int64_t write(
      const facebook::axiom::optimizer::MultiFragmentPlan& plan,
      const OutputFiles& files);

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

  // What a result task wrote to its part.
  struct WrittenPart {
    int64_t rows{0};
    // For a kiln:// directory: the chunk (hex), empty for no rows.
    std::string chunk;
    int64_t bytes{0};
  };

  // Runs 'stages' as one job. The last stage is the result stage, whose rows
  // are returned, or written to 'outputFiles' (then 'writtenParts' is set to
  // the parts, by partition), or, with 'outputKey', a map stage whose output
  // is kept in the memo under that key; then 'outputShuffle' is set to its
  // shuffle.
  std::vector<facebook::velox::RowVectorPtr> runStages(
      StagePlan& stages,
      const std::optional<std::string>& outputKey,
      facebook::velox::memory::MemoryPool* pool,
      int32_t* outputShuffle = nullptr,
      const OutputFiles* outputFiles = nullptr,
      std::vector<WrittenPart>* writtenParts = nullptr);

  // write() to a kiln:// directory: the parts go into a new bucket, and are
  // published as a new dataset once they are all written.
  int64_t writeKilnDataset(StagePlan& stages, const OutputFiles& files);

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

  // Serialized splits for each task of a source stage, per scan node, and the
  // node each task must run on ("" for any).
  std::vector<std::map<std::string, std::vector<folly::dynamic>>> scanSplits(
      const Stage& stage,
      JobContext& job,
      std::vector<std::string>& nodes);

  // The files of 'scan' and their sizes, as the driver sees them.
  std::vector<rpc::FileInfo> listFiles(
      const facebook::velox::core::TableScanNode& scan,
      JobContext& job);

  // The shuffle stored under 'key', if it can be reused by a stage with
  // 'numTasks' tasks: one with as many map outputs. Looks in Kiln's catalog
  // if the session has none.
  std::optional<int32_t> storedShuffle(
      const std::optional<std::string>& key,
      int32_t numTasks);

  // Stores the complete output of shuffle 'shuffleId', kept under 'key', in
  // Kiln's catalog, with ember.reuse.kiln.
  void storeInKiln(const std::string& key, int32_t shuffleId);

  // Drops a shuffle's output: at the executors, in the tracker, and its Kiln
  // bucket if it has one.
  void removeShuffle(int32_t shuffleId);

  void deleteKilnBucket(uint64_t bucket);

  const DagSchedulerOptions options_;
  const bool reuseOutputs_;
  const int32_t rangeSortPartitions_;
  const bool nodeLocalFiles_;
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
  // Kiln's catalog, with ember.reuse.kiln.
  const std::unique_ptr<KilnMemo> kilnMemo_;

  // With ember.shuffle.store=kiln: the Kiln client, and the bucket of each
  // shuffle the session wrote that has not been removed or stored in Kiln's
  // catalog.
  std::shared_ptr<kiln::client::Client> kiln_;
  std::mutex bucketsMutex_;
  std::map<int32_t, uint64_t> kilnBuckets_;
};

} // namespace ember::driver
