// write().stream() (DataframeWriter.h), in builds with stoke: a job
// tokenizes and packs text into batches and writes them into a stoke stream
// through Axiom's table write, which stoke's dataloader reads; against an
// in-process Kiln cluster and stoke shards.

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <set>
#include <thread>

#include <folly/coro/BlockingWait.h>

#include "ember/KilnFiles.h"
#include "ember/Runtime.h"
#include "ember/Session.h"
#include "ember/functions.h"
#include "ember/shuffle/KilnShuffle.h"
#include "kiln/local/LocalCluster.h"
#include "stoke/client/Registry.h"
#include "stoke/format/Batch.h"
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
    for (int i = 0; i < 2; ++i) {
      auto shard = std::make_shared<stoke::Shard>(
          registry_,
          stoke::ShardOptions{.refreshInterval = std::chrono::milliseconds(20)});
      servers_.push_back(stoke::rpc::serveShard(shard, "127.0.0.1", 0));
      endpoints_.push_back(
          "127.0.0.1:" + std::to_string(servers_.back()->port()));
    }
    blockingWait(registry_->co_create(std::string(kStream), endpoints_, 1.0));
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
  std::vector<std::string> endpoints_;
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

// With a task budget of 1, each scan task puts one chunk and then waits
// until a trainer consumed it: the job waits for its consumer, and the
// stream never holds more than a chunk per task unconsumed.
TEST_F(StreamWriteTest, waitsForTheConsumerAtTheTaskBudget) {
  blockingWait(registry_->co_remove(std::string(kStream)));
  blockingWait(registry_->co_create(
      std::string(kStream), endpoints_, 1.0, /*taskBudget=*/1));
  // 2 files, a task each; 2 chunks a task, as in cutsChunksAtTheConfiguredSize.
  session({{std::string(kStokeChunkBytesConfig), "300"}});
  auto df = batches(writeText(2, 5));
  const auto expected = values(df);

  std::atomic<int64_t> written{-1};
  std::thread job([&] {
    written = df.write().stream(std::string(kStream), /*seal=*/true);
  });
  // Nobody consumes yet: each task put its first chunk and waits.
  std::this_thread::sleep_for(std::chrono::seconds(2));
  EXPECT_EQ(written, -1);
  EXPECT_EQ(chunksPut(), 2);

  EXPECT_EQ(consume(), expected);
  job.join();
  EXPECT_EQ(written, 10);
  EXPECT_EQ(chunksPut(), 4);
}

// End to end with more tasks than slots and a slow trainer: 8 tasks on 4
// slots (local[4]), 3 chunks each, a budget of 2 chunks per task, and a
// loader with a small buffer that takes 20 ms per batch. While the job
// runs the stream never holds more than slots x budget chunks not yet
// consumed, and every batch arrives once.
TEST_F(StreamWriteTest, boundsUnconsumedChunksWhileATrainerReads) {
  constexpr uint64_t kSlots = 4;
  constexpr uint32_t kBudget = 2;
  blockingWait(registry_->co_remove(std::string(kStream)));
  blockingWait(registry_->co_create(
      std::string(kStream), endpoints_, 1.0, kBudget));
  // 9 batches a file, 3 to a chunk of 300 bytes.
  session({{std::string(kStokeChunkBytesConfig), "300"}});
  auto df = batches(writeText(8, 9));
  const auto expected = values(df);

  auto stream = blockingWait(
      stoke::StreamClient::co_open(registry_, std::string(kStream)));
  std::atomic<bool> done{false};
  std::atomic<uint64_t> peak{0};
  std::thread sampler([&] {
    while (!done) {
      uint64_t unconsumed = 0;
      for (const auto& shard : blockingWait(stream->co_stats())) {
        unconsumed += shard.put - shard.acked;
      }
      peak = std::max<uint64_t>(peak, unconsumed);
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  });
  std::atomic<int64_t> written{-1};
  std::thread job([&] {
    written = df.write().stream(std::string(kStream), /*seal=*/true);
  });

  stoke::Loader loader(
      blockingWait(
          stoke::StreamClient::co_open(registry_, std::string(kStream))),
      shuffle::kilnClient(metadata()),
      stoke::LoaderOptions{
          .epoch = {"rank0", 0},
          .minBufferedBatches = 2,
          .maxBufferedBatches = 4,
          .wait = std::chrono::milliseconds(20)});
  std::multiset<std::string> consumed;
  while (auto batch = loader.next()) {
    consumed.insert(
        stoke::format::encodeBatch(batch->view.header, batch->view.payload));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  job.join();
  done = true;
  sampler.join();

  EXPECT_EQ(written, 72);
  EXPECT_EQ(consumed, expected);
  EXPECT_EQ(chunksPut(), 24);
  // The bound held, and the trainer was slow enough to reach it.
  EXPECT_LE(peak, kSlots * kBudget);
  EXPECT_GE(peak, kBudget);
  // The loader's buffer: its maximum, passed by at most part of a chunk.
  EXPECT_LT(loader.stats().peakBuffered, 4 + 3);
}

// With packing, each task concatenates its rows' tokens and cuts them into
// batches of [B, T + 1]: its last batch holds the whole windows left, and
// the tokens after them are dropped.
TEST_F(StreamWriteTest, packsTokensAcrossRows) {
  // 2 files, a task each, of 5 lines of 20 letters: 5 x 21 = 105 tokens a
  // task, 6 batches of 2 x 8 and a last one of 1 x 8, 1 token dropped.
  const auto input = writeText(2, 5);
  auto tokens = session().read().text(input).select(
      call("stoke_tokenize", col("value")).alias("tokens"));
  EXPECT_EQ(
      tokens.write().stream(
          std::string(kStream),
          /*seal=*/true,
          StokePacking{.batch = kBatch, .sequence = kSequence}),
      10);

  std::multiset<std::string> expected;
  for (int32_t f = 0; f < 2; ++f) {
    std::vector<int32_t> all;
    for (int32_t i = 0; i < 5; ++i) {
      all.insert(all.end(), 20, 'a' + (f * 5 + i) % 26);
      all.push_back(256);
    }
    const uint64_t window = kSequence + 1;
    for (size_t begin = 0; begin + window <= all.size();) {
      const auto rows = std::min<uint64_t>(kBatch, (all.size() - begin) / window);
      const stoke::format::BatchHeader header{
          .dtype = stoke::format::DType::kInt32, .shape = {rows, window}};
      expected.insert(stoke::format::encodeBatch(
          header,
          {reinterpret_cast<const uint8_t*>(all.data() + begin),
           rows * window * sizeof(int32_t)}));
      begin += rows * window;
    }
  }
  ASSERT_EQ(expected.size(), 14);
  EXPECT_EQ(consume(), expected);
}

TEST_F(StreamWriteTest, rejectsOtherColumnsAndMissingStreams) {
  auto input = writeText(1, 2);
  EXPECT_ANY_THROW(session().read().text(input).write().stream(
      std::string(kStream)));
  EXPECT_ANY_THROW(batches(input).write().stream("missing"));
  // Batches are not packed, and tokens are only written packed.
  EXPECT_ANY_THROW(batches(input).write().stream(
      std::string(kStream), false, StokePacking{.batch = 1, .sequence = 1}));
  EXPECT_ANY_THROW(session()
                       .read()
                       .text(input)
                       .select(call("stoke_tokenize", col("value")))
                       .write()
                       .stream(std::string(kStream)));
  EXPECT_EQ(chunksPut(), 0);
}

} // namespace
} // namespace ember
