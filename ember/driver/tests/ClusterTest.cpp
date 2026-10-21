// Ember as a cluster on one machine: a driver (this test) and ember-worker
// processes started separately, as they would be on other hosts. Tasks reach
// the workers over RPC and every shuffle block travels between workers over
// TCP.

#include <gtest/gtest.h>

#include <signal.h>
#include <fcntl.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <filesystem>

#include "ember/rpc/RpcServer.h"
#include "ember/tests/WordCountUtil.h"
#include "ember/worker/Worker.h"
#include "velox/common/base/tests/GTestUtils.h"

extern char** environ;

namespace ember {
namespace {

namespace fs = std::filesystem;

// The worker binary, from the test's environment.
std::string workerBinary() {
  const char* path = ::getenv("EMBER_WORKER");
  VELOX_CHECK_NOT_NULL(path, "EMBER_WORKER is not set");
  return fs::absolute(path).string();
}

uint16_t freePort() {
  rpc::RpcServer server;
  server.start("", 0);
  return server.port();
}

class WorkerProcess {
 public:
  WorkerProcess(const std::vector<std::string>& args, const fs::path& log) {
    std::vector<char*> argv;
    argv.push_back(const_cast<char*>(binary_.c_str()));
    for (const auto& arg : args) {
      argv.push_back(const_cast<char*>(arg.c_str()));
    }
    argv.push_back(nullptr);
    posix_spawn_file_actions_t actions;
    posix_spawn_file_actions_init(&actions);
    posix_spawn_file_actions_addopen(
        &actions, STDOUT_FILENO, log.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    posix_spawn_file_actions_adddup2(&actions, STDOUT_FILENO, STDERR_FILENO);
    const int rc = ::posix_spawn(
        &pid_, binary_.c_str(), &actions, nullptr, argv.data(), environ);
    posix_spawn_file_actions_destroy(&actions);
    VELOX_CHECK_EQ(rc, 0, "Cannot start {}", binary_);
  }

  ~WorkerProcess() {
    kill();
  }

  void kill() {
    if (pid_ > 0) {
      ::kill(pid_, SIGKILL);
      ::waitpid(pid_, nullptr, 0);
      pid_ = -1;
    }
  }

  /// Waits up to 'timeout' for the process to exit. Returns its exit status,
  /// or nullopt if it is still running.
  std::optional<int> waitForExit(std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
      int status = 0;
      if (::waitpid(pid_, &status, WNOHANG) == pid_) {
        pid_ = -1;
        return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return std::nullopt;
  }

 private:
  const std::string binary_{workerBinary()};
  pid_t pid_{-1};
};

class ClusterTest : public testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_cluster_{}_{}", ::getpid(), testCounter_++);
    fs::remove_all(dir_);
    fs::create_directories(dir_ / "input");
    port_ = freePort();
  }

  void TearDown() override {
    if (session_.has_value()) {
      session_->stop();
      session_.reset();
    }
    workers_.clear();
    fs::remove_all(dir_);
  }

  void startWorkers(int32_t count, int32_t slots) {
    for (int32_t i = 0; i < count; ++i) {
      const auto id = workers_.size();
      workers_.push_back(std::make_unique<WorkerProcess>(
          std::vector<std::string>{
              fmt::format("--driver=localhost:{}", port_),
              fmt::format("--slots={}", slots),
              "--host=localhost",
              fmt::format("--local_dir={}", (dir_ / "workers").string()),
              // Even a worker's own blocks go over TCP.
              "--fetch_local_blocks_over_network"},
          dir_ / fmt::format("worker-{}.log", id)));
    }
  }

  Session& startDriver(int32_t expectedWorkers) {
    session_ = Session::builder()
                   .appName("ClusterTest")
                   .master(fmt::format("ember://localhost:{}", port_))
                   .config("ember.cluster.workers", std::to_string(expectedWorkers))
                   .config("ember.local.dir", (dir_ / "driver").string())
                   .getOrCreate();
    return *session_;
  }

  static inline int32_t testCounter_{0};
  fs::path dir_;
  uint16_t port_{0};
  std::vector<std::unique_ptr<WorkerProcess>> workers_;
  std::optional<Session> session_;
};

TEST_F(ClusterTest, wordCountMatchesShellPipeline) {
  test::writeCorpus(dir_ / "input", /*numFiles=*/6, /*linesPerFile=*/3000);
  // Workers start before the driver and wait for it.
  startWorkers(/*count=*/3, /*slots=*/2);
  auto& session = startDriver(3);
  EXPECT_EQ(session.state().parallelism(), 6);

  auto rows = test::wordCount(session, (dir_ / "input").string()).collect();
  auto expected = test::shellWordCount(dir_ / "input");
  ASSERT_FALSE(expected.empty());
  EXPECT_EQ(test::toMap(rows), expected);
  for (size_t i = 1; i < rows.size(); ++i) {
    EXPECT_GE(rows[i - 1].getLong(1), rows[i].getLong(1));
  }

  // The session runs further jobs on the same workers.
  EXPECT_EQ(session.read().text((dir_ / "input").string()).count(), 6 * 3000);
}

TEST_F(ClusterTest, workersExitWhenTheDriverStops) {
  startWorkers(/*count=*/2, /*slots=*/1);
  auto& session = startDriver(2);
  test::writeCorpus(dir_ / "input", 2, 100);
  EXPECT_EQ(
      test::toMap(test::wordCount(session, (dir_ / "input").string()).collect()),
      test::shellWordCount(dir_ / "input"));
  session.stop();
  session_.reset();
  for (auto& worker : workers_) {
    EXPECT_EQ(worker->waitForExit(std::chrono::seconds(30)), 0);
  }
  // Each worker removed its shuffle directory.
  EXPECT_TRUE(
      !fs::exists(dir_ / "workers") || fs::is_empty(dir_ / "workers"));
}

TEST_F(ClusterTest, losingAWorkerBetweenJobs) {
  test::writeCorpus(dir_ / "input", 4, 1000);
  const auto expected = test::shellWordCount(dir_ / "input");
  startWorkers(/*count=*/3, /*slots=*/2);
  auto& session = startDriver(3);
  EXPECT_EQ(
      test::toMap(test::wordCount(session, (dir_ / "input").string()).collect()),
      expected);

  // Tasks sent to the dead worker fail and run again on the others.
  workers_[1]->kill();
  EXPECT_EQ(
      test::toMap(test::wordCount(session, (dir_ / "input").string()).collect()),
      expected);
}

TEST_F(ClusterTest, driverFailsWithoutWorkers) {
  VELOX_ASSERT_THROW(
      Session::builder()
          .appName("ClusterTest")
          .master(fmt::format("ember://localhost:{}", port_))
          .config("ember.cluster.registration_timeout_ms", "300")
          .getOrCreate(),
      "0 of 1 workers registered");
}

// Workers in this process, for debugging the protocol without processes.
TEST_F(ClusterTest, inProcessWorkers) {
  std::vector<std::unique_ptr<Worker>> workers;
  std::vector<std::thread> threads;
  for (int i = 0; i < 2; ++i) {
    workers.push_back(std::make_unique<Worker>(WorkerOptions{
        .driver = rpc::HostAndPort{"localhost", port_},
        .advertisedHost = "localhost",
        .slots = 2,
        .localDirectory = (dir_ / "workers").string(),
        .fetchLocalBlocksOverNetwork = true}));
    workers.back()->start();
    threads.emplace_back([worker = workers.back().get()] { worker->run(); });
  }
  auto& session = startDriver(2);
  test::writeCorpus(dir_ / "input", 3, 500);
  EXPECT_EQ(
      test::toMap(test::wordCount(session, (dir_ / "input").string()).collect()),
      test::shellWordCount(dir_ / "input"));
  session.stop();
  session_.reset();
  for (auto& thread : threads) {
    thread.join();
  }
}

} // namespace
} // namespace ember
