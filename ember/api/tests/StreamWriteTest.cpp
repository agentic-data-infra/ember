// write().stream() (DataframeWriter.h), in builds with stoke: a job
// tokenizes and packs text into batches and writes them into a stoke stream
// through Axiom's table write, which stoke's dataloader reads; against an
// in-process Kiln cluster and stoke shards.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <set>

#include <folly/coro/BlockingWait.h>

#include "ember/KilnFiles.h"
#include "ember/Runtime.h"
#include "ember/Session.h"
#include "ember/functions.h"
#include "ember/shuffle/KilnShuffle.h"
#include "kiln/local/LocalCluster.h"
#include "stoke/client/Registry.h"
#include "stoke/client/StreamClient.h"
#include "stoke/loader/Loader.h"
#include "stoke/rpc/Rpc.h"
#include "stoke/shard/Shard.h"
#include "velox/vector/FlatVector.h"

namespace ember {
namespace {

using namespace ember::functions;
using folly::coro::blockingWait;
namespace fs = std::filesystem;
namespace velox = facebook::velox;

constexpr std::string_view kStream = "train";
constexpr int64_t kBatch = 2;
constexpr int64_t kSequence = 7;

class StreamWriteTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    initializeRuntime();
    cluster_ = new kiln::local::LocalCluster(kiln::local::LocalClusterOptions{
        .dramServers = 1, .localFsServers = 0});
  }

  static void TearDownTestSuite() {
    delete cluster_;
    cluster_ = nullptr;
  }

  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_stream_write_test_{}", ::getpid());
    fs::remove_all(dir_);
    fs::create_directories(dir_);
    registry_ =
        std::make_shared<stoke::Registry>(shuffle::kilnClient(metadata()));
    std::vector<std::string> endpoints;
    for (int i = 0; i < 2; ++i) {
      auto shard = std::make_shared<stoke::Shard>(
          registry_,
          stoke::ShardOptions{.refreshInterval = std::chrono::milliseconds(20)});
      servers_.push_back(stoke::rpc::serveShard(shard, "127.0.0.1", 0));
      endpoints.push_back(
          "127.0.0.1:" + std::to_string(servers_.back()->port()));
    }
    blockingWait(registry_->co_create(std::string(kStream), endpoints, 1.0));
  }

  void TearDown() override {
    if (ember_.has_value()) {
      ember_->stop();
    }
    blockingWait(registry_->co_remove(std::string(kStream)));
    servers_.clear();
    fs::remove_all(dir_);
  }

  static std::string metadata() {
    return cluster_->metadataAddress().toString();
  }

  // The session, which reads each input file as one split (each is under
  // 1 KiB), with 'config' added the first time.
  Session& session(std::map<std::string, std::string> config = {}) {
    if (!ember_.has_value()) {
      auto builder = Session::builder();
      builder.appName("StreamWriteTest")
          .master("local[4]")
          .config(std::string(kKilnFilesMetadata), metadata())
          .config("ember.files.max_split_bytes", "1024");
      for (const auto& [key, value] : config) {
        builder.config(key, value);
      }
      ember_ = builder.getOrCreate();
    }
    return *ember_;
  }

  // 'numFiles' text files of 'linesPerFile' lines of 20 letters each, line
  // i of file f holding letter 'a' + (f * linesPerFile + i) % 26.
  std::string writeText(int32_t numFiles, int32_t linesPerFile) {
    const auto input = dir_ / "input";
    fs::create_directories(input);
    for (int32_t f = 0; f < numFiles; ++f) {
      std::ofstream out(input / fmt::format("in-{}", f));
      for (int32_t i = 0; i < linesPerFile; ++i) {
        out << std::string(20, 'a' + (f * linesPerFile + i) % 26) << "\n";
      }
    }
    return input.string();
  }

  // Each line of 'input' as one batch.
  Dataframe batches(const std::string& input) {
    return session().read().text(input).select(
        call(
            "stoke_pack",
            call("stoke_tokenize", col("value")),
            lit(kBatch),
            lit(kSequence))
            .alias("batch"));
  }

  // The batches 'df' holds, as their bytes.
  static std::multiset<std::string> values(const Dataframe& df) {
    std::multiset<std::string> result;
    for (const auto& batch : df.collectBatches()) {
      auto* column = batch->childAt(0)->asFlatVector<velox::StringView>();
      for (velox::vector_size_t i = 0; i < batch->size(); ++i) {
        result.insert(std::string(column->valueAt(i)));
      }
    }
    return result;
  }

  // Chunks put into the stream.
  uint64_t chunksPut() {
    auto stream = blockingWait(
        stoke::StreamClient::co_open(registry_, std::string(kStream)));
    uint64_t put = 0;
    for (const auto& shard : blockingWait(stream->co_stats())) {
      put += shard.put;
    }
    return put;
  }

  // Every batch of the stream until it ends, as its bytes.
  std::multiset<std::string> consume() {
    stoke::Loader loader(
        blockingWait(
            stoke::StreamClient::co_open(registry_, std::string(kStream))),
        shuffle::kilnClient(metadata()),
        stoke::LoaderOptions{
            .epoch = {"rank0", 0}, .wait = std::chrono::milliseconds(20)});
    std::multiset<std::string> result;
    while (auto batch = loader.next()) {
      result.insert(
          stoke::format::encodeBatch(batch->view.header, batch->view.payload));
    }
    return result;
  }

  static kiln::local::LocalCluster* cluster_;
  std::shared_ptr<stoke::Registry> registry_;
  std::vector<std::unique_ptr<stoke::rpc::RpcServer>> servers_;
  std::optional<Session> ember_;
  fs::path dir_;
};

kiln::local::LocalCluster* StreamWriteTest::cluster_ = nullptr;

TEST_F(StreamWriteTest, writesEveryScanTasksChunks) {
  // 4 files, one split and task each; each task's batches fit one chunk.
  auto df = batches(writeText(4, 5));
  EXPECT_EQ(df.write().stream(std::string(kStream), /*seal=*/true), 20);
  // A chunk per scan task: the rows were not gathered into one task.
  EXPECT_EQ(chunksPut(), 4);
  EXPECT_EQ(consume(), values(df));
}

TEST_F(StreamWriteTest, cutsChunksAtTheConfiguredSize) {
  // A batch is 64 + 2 * 8 * 4 bytes, 136 framed; a chunk is put once it
  // reaches 300 bytes, i.e. every 3 batches: 2 chunks for each file's 5.
  session({{std::string(kStokeChunkBytesConfig), "300"}});
  auto df = batches(writeText(2, 5));
  EXPECT_EQ(df.write().stream(std::string(kStream), /*seal=*/true), 10);
  EXPECT_EQ(chunksPut(), 4);
  EXPECT_EQ(consume(), values(df));
}

TEST_F(StreamWriteTest, leavesTheStreamOpenUnlessSealed) {
  auto df = batches(writeText(2, 3));
  EXPECT_EQ(df.write().stream(std::string(kStream)), 6);
  EXPECT_EQ(
      blockingWait(registry_->co_get(std::string(kStream)))->state,
      stoke::StreamState::kOpen);
  // A second job feeds the same stream, then seals it.
  EXPECT_EQ(df.write().stream(std::string(kStream), /*seal=*/true), 6);
  EXPECT_EQ(
      blockingWait(registry_->co_get(std::string(kStream)))->state,
      stoke::StreamState::kSealed);
  // The second job wrote the same chunks, which the stream dropped.
  EXPECT_EQ(consume(), values(df));
}

TEST_F(StreamWriteTest, rejectsOtherColumnsAndMissingStreams) {
  auto input = writeText(1, 2);
  EXPECT_ANY_THROW(session().read().text(input).write().stream(
      std::string(kStream)));
  EXPECT_ANY_THROW(batches(input).write().stream("missing"));
  EXPECT_EQ(chunksPut(), 0);
}

} // namespace
} // namespace ember
