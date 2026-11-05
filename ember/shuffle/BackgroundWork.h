// Runs work that may wait, on disks or on Kiln, off the driver thread, so that
// the thread can run other tasks' drivers. An operator starts the work, and
// reports blocked until it is done.

#pragma once

#include <exception>
#include <functional>
#include <memory>

#include <folly/Executor.h>
#include <folly/futures/Promise.h>
#include <folly/synchronization/Baton.h>

#include "velox/common/future/VeloxPromise.h"

namespace ember::shuffle {

/// One piece of work at a time. Not thread-safe: its owner makes one call at a
/// time, and the work has the owner's state to itself until it is done.
class BackgroundWork {
 public:
  explicit BackgroundWork(std::shared_ptr<folly::Executor> executor)
      : executor_{std::move(executor)} {}

  /// Waits for the work in flight.
  ~BackgroundWork();

  /// Starts 'work'. The last work must be done.
  void start(std::function<void()> work);

  /// True while work is in flight; then 'future' is valid and completes with
  /// it. When the work has failed, rethrows its exception here, once.
  bool blocked(facebook::velox::ContinueFuture* future);

  /// Waits for the work in flight, dropping its failure.
  void wait();

  /// Waits for the work in flight, leaving a failure for blocked() to rethrow.
  /// Returns false if it failed.
  bool finish();

 private:
  struct State {
    folly::Promise<folly::Unit> promise;
    folly::Baton<> done;
    std::exception_ptr error;
  };

  const std::shared_ptr<folly::Executor> executor_;
  std::shared_ptr<State> state_;
};

} // namespace ember::shuffle
