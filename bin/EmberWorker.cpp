// ember-worker: runs an Ember driver's tasks.
//
//   ember-worker --driver=host:port [--slots=N] [--port=P] [--host=name]
//                [--local_dir=dir] [--persistent] [--fetch_timeout_ms=T]
//
// Start one per machine (or several on one machine), pointed at the address
// in the driver's master URL ember://host:port. The worker registers with the
// driver, retrying until it is up, runs its tasks and serves its shuffle
// blocks to the other workers on --port. It exits when the driver is done,
// or with --persistent waits for the next driver.

#include <csignal>
#include <filesystem>
#include <thread>

#include <folly/init/Init.h>
#include <folly/system/HardwareConcurrency.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include "ember/worker/Worker.h"

DEFINE_string(driver, "", "The driver's address, host:port.");
DEFINE_int32(slots, 0, "Tasks run at once; 0 for the number of cores.");
DEFINE_int32(threads, 0, "Threads for Velox drivers; 0 for 2 * slots.");
DEFINE_int32(port, 0, "Port to serve tasks and shuffle blocks on; 0 for any.");
DEFINE_string(bind_host, "", "Interface to serve on; empty for all.");
DEFINE_string(host, "", "Host name others reach this worker by.");
DEFINE_string(local_dir, "", "Directory for shuffle files; default <tmp>/ember.");
DEFINE_bool(persistent, false, "Wait for the next driver when one is done.");
DEFINE_bool(
    fetch_local_blocks_over_network,
    false,
    "Fetch this worker's own shuffle blocks over TCP too (for testing).");
DEFINE_int32(
    fetch_timeout_ms,
    120'000,
    "Milliseconds before a shuffle block fetch from another worker fails.");
DEFINE_int32(
    connect_timeout_s,
    60,
    "Seconds to keep trying to reach the driver; 0 for ever.");

namespace {
ember::Worker* activeWorker = nullptr;

void onSignal(int /*signal*/) {
  // Worker::stop only sets a flag and notifies.
  if (activeWorker != nullptr) {
    activeWorker->stop();
  }
}
} // namespace

int main(int argc, char** argv) {
  // Log to stderr unless told otherwise, so a launcher can collect it.
  FLAGS_logtostderr = true;
  folly::Init init(&argc, &argv);
  if (FLAGS_driver.empty()) {
    LOG(ERROR) << "--driver=host:port is required";
    return 2;
  }
  ember::WorkerOptions options;
  options.driver = ember::rpc::HostAndPort::parse(FLAGS_driver);
  options.bindHost = FLAGS_bind_host;
  options.advertisedHost = FLAGS_host;
  options.port = static_cast<uint16_t>(FLAGS_port);
  options.slots = FLAGS_slots > 0
      ? FLAGS_slots
      : static_cast<int32_t>(folly::available_concurrency());
  options.numThreads = FLAGS_threads;
  options.localDirectory = FLAGS_local_dir.empty()
      ? (std::filesystem::temp_directory_path() / "ember").string()
      : FLAGS_local_dir;
  options.persistent = FLAGS_persistent;
  options.fetchLocalBlocksOverNetwork = FLAGS_fetch_local_blocks_over_network;
  options.connectTimeout = std::chrono::seconds(FLAGS_connect_timeout_s);
  options.fetchTimeout = std::chrono::milliseconds(FLAGS_fetch_timeout_ms);

  ember::Worker worker(options);
  worker.start();
  activeWorker = &worker;
  std::signal(SIGTERM, onSignal);
  std::signal(SIGINT, onSignal);
  worker.run();
  activeWorker = nullptr;
  return 0;
}
