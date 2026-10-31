// Session: the entry point of an Ember program.
//
//   auto ember = Session::builder()
//                    .appName("DataframeWordCount")
//                    .master("local[*]")
//                    .getOrCreate();
//   Dataframe lines = ember.read().text(path);
//
// Masters:
//   local, local[N], local[*]  run in this process: N-way parallel stages on
//                              one local executor with N task slots.
//   ember://host:port          this program is the driver. It serves at
//                              'port'; ember-worker processes, started
//                              separately (on any host) with
//                              --driver=host:port, register there and run the
//                              tasks. The session waits for
//                              "ember.cluster.workers" workers (default 1).
// Either way stages exchange data through the file shuffle.

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "ember/Dataframe.h"
#include "ember/DataframeReader.h"
#include "ember/DataframeWriter.h"
#include "axiom/connectors/ConnectorSession.h"

namespace ember::driver {
class DagScheduler;
struct StageAttemptInfo;
}

namespace facebook::axiom::optimizer {
struct PlanAndStats;
}

namespace ember {

struct OutputFiles;

/// Shared state behind a Session and its Dataframes.
class SessionState {
 public:
  using StageListener = std::function<void(const driver::StageAttemptInfo&)>;

  SessionState(
      std::string appName,
      std::string master,
      std::map<std::string, std::string> config,
      StageListener onStageCompleted = nullptr);
  ~SessionState();

  const std::string& appName() const {
    return appName_;
  }

  const std::string& master() const {
    return master_;
  }

  /// Parallelism from the master URL: local = 1, local[N] = N,
  /// local[*] = number of cores; for a cluster, the workers' slots at start,
  /// unless "ember.default.parallelism" sets it.
  int32_t parallelism() const {
    return parallelism_;
  }

  const std::map<std::string, std::string>& config() const {
    return config_;
  }

  /// Config "ember.eager_analysis" = "true": every transformation builds its
  /// plan right away, so errors surface where they are written.
  bool eagerAnalysis() const {
    return eagerAnalysis_;
  }

  /// Config key: directory for shuffle files. Defaults to <tmp>/ember.
  static constexpr const char* kLocalDirectory = "ember.local.dir";
  /// Config key: tasks per stage, overriding what the master implies.
  static constexpr const char* kDefaultParallelism = "ember.default.parallelism";
  /// Config key: workers to wait for before running anything.
  static constexpr const char* kClusterWorkers = "ember.cluster.workers";
  /// Config key: how long to wait for them, in ms (default 60000).
  static constexpr const char* kRegistrationTimeoutMs =
      "ember.cluster.registration_timeout_ms";
  /// Config key: how long a worker may miss heartbeats before it is lost, in
  /// ms (default 30000).
  static constexpr const char* kHeartbeatTimeoutMs =
      "ember.cluster.heartbeat_timeout_ms";
  /// Config key: interface the driver serves on (default all).
  static constexpr const char* kDriverBindHost = "ember.driver.bind_host";

  /// Connector that reads files (text, parquet).
  static const std::string& fileConnectorId();

  /// Context for building logical plans in this session.
  facebook::axiom::logical_plan::PlanBuilder::Context planContext() const;

  /// Optimizes 'plan' into a multi-fragment Velox plan.
  facebook::axiom::optimizer::PlanAndStats optimize(
      const facebook::axiom::logical_plan::LogicalPlanNodePtr& plan);

  /// Runs an optimized plan through Ember's scheduler, returning all result
  /// batches.
  std::vector<facebook::velox::RowVectorPtr> execute(
      const facebook::axiom::optimizer::MultiFragmentPlanPtr& plan);

  /// Runs an optimized plan whose result tasks write their rows to 'files'.
  /// Returns the number of rows written.
  int64_t write(
      const facebook::axiom::optimizer::MultiFragmentPlanPtr& plan,
      const OutputFiles& files);

  /// Optimizes and runs 'plan', which ends in a table write, then commits
  /// the write (or aborts it if the job fails). 'properties' are the
  /// connectors' session properties. Returns the number of rows written.
  int64_t writeTable(
      const facebook::axiom::logical_plan::LogicalPlanNodePtr& plan,
      facebook::axiom::connector::ConnectorProperties properties);

  /// The stages 'plan' runs as, with their shuffle reads and writes.
  std::string explainStages(
      const facebook::axiom::optimizer::MultiFragmentPlanPtr& plan);

  /// Drops the stored output of cached table 'id' (Dataframe::unpersist).
  void uncache(const std::string& id);

  void stop();

 private:
  std::string nextQueryId();

  const std::string appName_;
  const std::string master_;
  const std::map<std::string, std::string> config_;
  int32_t parallelism_;
  const bool eagerAnalysis_;
  std::unique_ptr<driver::DagScheduler> scheduler_;
  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool_;
  // Holds collected results, which outlive each query's pools.
  std::shared_ptr<facebook::velox::memory::MemoryPool> resultPool_;
  // Holds optimized plans, which may outlive the query (explain).
  std::shared_ptr<facebook::velox::memory::MemoryPool> optimizerPool_;
  facebook::axiom::logical_plan::ExprResolver::FunctionRewriteHook
      functionHook_;
  std::atomic<int64_t> queryCounter_{0};
};

class Session {
 public:
  class Builder {
   public:
    Builder& appName(std::string name);
    Builder& master(std::string master);
    Builder& config(const std::string& key, std::string value);
    /// Called after each stage attempt of every job, e.g. to watch stages
    /// rerun after a worker is lost.
    Builder& onStageCompleted(SessionState::StageListener listener);

    /// Returns the active session if there is one, else creates it.
    Session getOrCreate();

   private:
    std::string appName_{"ember"};
    std::string master_{"local[*]"};
    std::map<std::string, std::string> config_;
    SessionState::StageListener onStageCompleted_;
  };

  static Builder builder();

  /// The active session, if any.
  static std::optional<Session> active();

  DataframeReader read() const;

  /// A Dataframe from literal rows, backed by a Values node (Koski's
  /// inlineTable).
  Dataframe createDataframe(
      const facebook::velox::RowTypePtr& type,
      std::vector<std::vector<facebook::velox::Variant>> rows) const;
  Dataframe createDataframe(
      const std::vector<facebook::velox::RowVectorPtr>& data) const;
  Dataframe inlineTable(
      const facebook::velox::RowTypePtr& type,
      std::vector<std::vector<facebook::velox::Variant>> rows) const {
    return createDataframe(type, std::move(rows));
  }

  const SessionState& state() const {
    return *state_;
  }

  /// Ends the session; the next getOrCreate() creates a new one.
  void stop();

 private:
  explicit Session(std::shared_ptr<SessionState> state)
      : state_{std::move(state)} {}

  std::shared_ptr<SessionState> state_;
};

} // namespace ember
