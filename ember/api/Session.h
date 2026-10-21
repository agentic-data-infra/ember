// Session: the entry point of an Ember program.
//
//   auto ember = Session::builder()
//                    .appName("DataframeWordCount")
//                    .master("local[*]")
//                    .getOrCreate();
//   Dataframe lines = ember.read().text(path);
//
// For now every master runs in-process: "local[N]" plans N-way parallel
// stages, which Ember's scheduler runs on one local executor with N task
// slots, with a file shuffle between stages. Worker processes take over
// execution behind the same API in a later step.

#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "ember/Dataframe.h"
#include "ember/DataframeReader.h"

namespace ember::driver {
class DagScheduler;
}

namespace facebook::axiom::optimizer {
struct PlanAndStats;
}

namespace ember {

/// Shared state behind a Session and its Dataframes.
class SessionState {
 public:
  SessionState(
      std::string appName,
      std::string master,
      std::map<std::string, std::string> config);
  ~SessionState();

  const std::string& appName() const {
    return appName_;
  }

  const std::string& master() const {
    return master_;
  }

  /// Parallelism from the master URL: local = 1, local[N] = N,
  /// local[*] = number of cores.
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

  /// The stages 'plan' runs as, with their shuffle reads and writes.
  std::string explainStages(
      const facebook::axiom::optimizer::MultiFragmentPlanPtr& plan);

  void stop();

 private:
  std::string nextQueryId();

  const std::string appName_;
  const std::string master_;
  const std::map<std::string, std::string> config_;
  const int32_t parallelism_;
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

    /// Returns the active session if there is one, else creates it.
    Session getOrCreate();

   private:
    std::string appName_{"ember"};
    std::string master_{"local[*]"};
    std::map<std::string, std::string> config_;
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
