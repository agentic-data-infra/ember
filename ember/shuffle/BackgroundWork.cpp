#include "ember/shuffle/BackgroundWork.h"

#include "velox/common/base/Exceptions.h"

namespace ember::shuffle {

BackgroundWork::~BackgroundWork() {
  wait();
}

void BackgroundWork::start(std::function<void()> work) {
  VELOX_CHECK_NULL(state_, "Background work is in flight");
  state_ = std::make_shared<State>();
  executor_->add([state = state_, work = std::move(work)] {
    try {
      work();
    } catch (...) {
      state->error = std::current_exception();
    }
    state->done.post();
    state->promise.setValue();
  });
}

bool BackgroundWork::blocked(facebook::velox::ContinueFuture* future) {
  if (state_ == nullptr) {
    return false;
  }
  if (!state_->done.ready()) {
    *future = state_->promise.getSemiFuture();
    return true;
  }
  auto error = std::move(state_->error);
  state_ = nullptr;
  if (error != nullptr) {
    std::rethrow_exception(error);
  }
  return false;
}

void BackgroundWork::wait() {
  if (state_ != nullptr) {
    state_->done.wait();
    state_ = nullptr;
  }
}

bool BackgroundWork::finish() {
  if (state_ == nullptr) {
    return true;
  }
  state_->done.wait();
  return state_->error == nullptr;
}

} // namespace ember::shuffle
