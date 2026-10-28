// What the driver sends an executor to run one task, and what comes back.
// Both travel as bytes, so the local backend and a remote executor see the
// same messages.

#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include <folly/dynamic.h>

#include "ember/shuffle/ShuffleInterface.h"

namespace ember {

/// Files a result task writes its rows to, instead of returning them.
struct OutputFiles {
  /// "gensort": each row's key and payload, as a 100-byte record.
  std::string format;
  /// Task p writes <directory>/part-NNNNN, NNNNN = p with 5 digits.
  std::string directory;
};

/// One task: a stage's plan fragment plus this task's splits.
struct TaskDescription {
  std::string jobId;
  int32_t stageId{0};
  int32_t stageAttempt{0};
  /// Index of the task in its stage: the map id of a map task, the reduce
  /// partition it reads.
  int32_t partition{0};
  int32_t attempt{0};

  /// The stage's Velox plan fragment, serialized with Velox serde.
  folly::dynamic fragment;

  /// Serialized splits per plan node: file ranges for scans, shuffle block
  /// URLs for shuffle reads. Every node that takes splits has an entry, maybe
  /// empty.
  std::map<std::string, std::vector<folly::dynamic>> splits;

  /// The final stage's task: its rows go back to the driver.
  bool resultTask{false};

  /// Set on a result task whose rows go to files; it returns only counts.
  std::optional<OutputFiles> outputFiles;

  /// Velox query config, plus Ember's own keys.
  std::map<std::string, std::string> config;

  /// Unique per attempt.
  std::string taskId() const;

  std::string serialize() const;
  static TaskDescription deserialize(const std::string& bytes);
};

enum class TaskState { kFinished, kFailed };

struct TaskResult {
  std::string taskId;
  TaskState state{TaskState::kFinished};
  std::string error;
  /// The error came from the user's program (e.g. a bad cast), not from Ember.
  bool userError{false};

  /// Set when the task failed because a shuffle block it reads is lost. The
  /// driver recomputes that map output instead of retrying the task.
  std::optional<shuffle::FetchFailure> fetchFailure;

  /// Set when a map task commits its shuffle output.
  std::optional<shuffle::MapStatus> mapStatus;

  /// A result task's rows, as serialized pages.
  std::vector<std::string> resultPages;

  int64_t outputRows{0};
  int64_t wallMicros{0};

  std::string serialize() const;
  static TaskResult deserialize(const std::string& bytes);
};

} // namespace ember
