// The Kiln shuffle store against an in-process Kiln cluster: one chunk per
// map output, block reads by byte range, spills, empty outputs, first commit
// wins, and removed chunks.

#include <gtest/gtest.h>

#include <filesystem>

#include <folly/coro/BlockingWait.h>

#include "ember/shuffle/KilnShuffle.h"
#include "kiln/client/Client.h"
#include "kiln/local/LocalCluster.h"

namespace ember::shuffle {
namespace {

namespace fs = std::filesystem;

class KilnShuffleTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    cluster_ = new kiln::local::LocalCluster(kiln::local::LocalClusterOptions{
        .dramServers = 1, .localFsServers = 0});
  }

  static void TearDownTestSuite() {
    delete cluster_;
    cluster_ = nullptr;
  }

  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_kiln_shuffle_{}", ::getpid());
    fs::remove_all(dir_);
    store_ = std::make_shared<ShuffleBlockStore>("test-executor", dir_.string());
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  static std::string metadata() {
    return cluster_->metadataEndpoint().toString();
  }

  std::unique_ptr<ShuffleWriter> writer(
      int32_t shuffleId,
      int32_t mapId,
      int32_t numPartitions,
      const std::string& attemptTag = "attempt0",
      int64_t spillThreshold = ShuffleBlockStore::kDefaultSpillThreshold) {
    return createKilnWriter(
        store_,
        metadata(),
        shuffleId,
        mapId,
        numPartitions,
        attemptTag,
        spillThreshold);
  }

  // Writes pages "<p>-<i>" for i < count[p] in round-robin order over the
  // partitions.
  MapStatus writeMap(
      int32_t shuffleId,
      int32_t mapId,
      const std::vector<int32_t>& counts,
      int64_t spillThreshold = ShuffleBlockStore::kDefaultSpillThreshold) {
    auto mapWriter =
        writer(shuffleId, mapId, counts.size(), "attempt0", spillThreshold);
    const auto max = *std::max_element(counts.begin(), counts.end());
    for (int32_t i = 0; i < max; ++i) {
      for (int32_t p = 0; p < counts.size(); ++p) {
        if (i < counts[p]) {
          mapWriter->collect(
              p, folly::IOBuf::copyBuffer(fmt::format("{}-{}", p, i)));
        }
      }
    }
    return mapWriter->commit();
  }

  // The pages of partition 'reduceId' of 'status', read as a reduce task
  // would: at the byte range the driver puts into the block URL.
  static std::vector<std::string> readBlock(
      const MapStatus& status,
      int32_t reduceId) {
    int64_t offset = 0;
    for (int32_t p = 0; p < reduceId; ++p) {
      offset += status.partitionLengths[p];
    }
    auto url = blockUrl(
        status.location, {0, 0, reduceId}, offset, status.partitionLengths[reduceId]);
    std::vector<std::string> pages;
    for (auto& page : splitFrames(fetchKilnBlock(url).get())) {
      pages.push_back(page->moveToFbString().toStdString());
    }
    return pages;
  }

  static inline kiln::local::LocalCluster* cluster_{nullptr};
  fs::path dir_;
  std::shared_ptr<ShuffleBlockStore> store_;
};

TEST_F(KilnShuffleTest, oneChunkWithPartitionsInOrder) {
  auto status = writeMap(0, 0, {3, 0, 2});
  ASSERT_TRUE(isKilnLocation(status.location));
  EXPECT_EQ(parseKilnLocation(status.location).first, metadata());
  // Each frame is a 4-byte length plus "p-i".
  EXPECT_EQ(status.partitionLengths, (std::vector<int64_t>{21, 0, 14}));
  EXPECT_EQ(readBlock(status, 0), (std::vector<std::string>{"0-0", "0-1", "0-2"}));
  EXPECT_EQ(readBlock(status, 2), (std::vector<std::string>{"2-0", "2-1"}));

  const auto chunk =
      kiln::ChunkId::parse(parseKilnLocation(status.location).second);
  auto info = folly::coro::blockingWait(kilnClient(metadata())->co_stat(chunk));
  EXPECT_EQ(info.size, 35);
  // The store reports the output, and keeps no files for it.
  EXPECT_TRUE(store_->hasMapOutput(0, 0));
  EXPECT_EQ(store_->mapStatus(0, 0).location, status.location);
  EXPECT_TRUE(fs::is_empty(dir_));
}

TEST_F(KilnShuffleTest, spilledRunsMergeInWriteOrder) {
  // A tiny threshold spills after every page.
  auto status = writeMap(1, 0, {5, 4}, /*spillThreshold=*/1);
  EXPECT_EQ(
      readBlock(status, 0),
      (std::vector<std::string>{"0-0", "0-1", "0-2", "0-3", "0-4"}));
  EXPECT_EQ(
      readBlock(status, 1), (std::vector<std::string>{"1-0", "1-1", "1-2", "1-3"}));
  EXPECT_TRUE(fs::is_empty(dir_));
}

TEST_F(KilnShuffleTest, largeOutputSpansAppends) {
  // More than one append buffer and one Kiln block.
  const std::string data(3 << 20, 'x');
  auto mapWriter = writer(2, 0, 2);
  for (int i = 0; i < 3; ++i) {
    mapWriter->collect(i % 2, folly::IOBuf::copyBuffer(data));
  }
  auto status = mapWriter->commit();
  EXPECT_EQ(
      status.partitionLengths,
      (std::vector<int64_t>{2 * (4 + (3 << 20)), 4 + (3 << 20)}));
  EXPECT_EQ(readBlock(status, 0), (std::vector<std::string>{data, data}));
  EXPECT_EQ(readBlock(status, 1), (std::vector<std::string>{data}));
}

TEST_F(KilnShuffleTest, emptyOutputHasNoChunk) {
  auto status = writer(3, 0, 2)->commit();
  EXPECT_TRUE(isKilnLocation(status.location));
  EXPECT_EQ(parseKilnLocation(status.location).second, "");
  EXPECT_EQ(status.partitionLengths, (std::vector<int64_t>{0, 0}));
}

TEST_F(KilnShuffleTest, firstCommitWins) {
  auto first = writer(4, 0, 1, "attempt0");
  auto second = writer(4, 0, 1, "attempt1");
  first->collect(0, folly::IOBuf::copyBuffer("first"));
  second->collect(0, folly::IOBuf::copyBuffer("second, longer"));
  auto secondStatus = second->commit();
  auto status = first->commit();
  EXPECT_EQ(status.location, secondStatus.location);
  EXPECT_EQ(readBlock(status, 0), (std::vector<std::string>{"second, longer"}));
}

TEST_F(KilnShuffleTest, removedChunkFailsTheFetch) {
  auto status = writeMap(5, 0, {2});
  const auto chunk =
      kiln::ChunkId::parse(parseKilnLocation(status.location).second);
  ASSERT_TRUE(folly::coro::blockingWait(kilnClient(metadata())->co_remove(chunk)));
  EXPECT_THROW(readBlock(status, 0), std::exception);
}

TEST(KilnBlockUrlTest, roundTrip) {
  BlockId id{7, 12, 3};
  const auto location = kilnLocation("meta:7000", "abc123");
  EXPECT_EQ(location, "kiln://meta:7000/abc123");
  auto url = blockUrl(location, id, 100, 42);
  EXPECT_EQ(url, "kiln://meta:7000/abc123/100/42/7/12/3");
  EXPECT_TRUE(isBlockUrl(url));
  auto [parsedLocation, parsed] = parseBlockUrl(url);
  EXPECT_EQ(parsedLocation, location);
  EXPECT_EQ(parsed, id);
  EXPECT_EQ(kilnBlockRange(url), (std::pair<int64_t, int64_t>{100, 42}));

  // A fetch failure names the map output's location, as the driver knows it.
  const FetchFailure failure{location, id};
  auto found = FetchFailure::find("Error: " + failure.message("not found"));
  ASSERT_TRUE(found.has_value());
  EXPECT_EQ(found->location, location);
  EXPECT_EQ(found->block, id);
}

} // namespace
} // namespace ember::shuffle
