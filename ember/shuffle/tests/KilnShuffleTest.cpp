// The Kiln shuffle store against an in-process Kiln cluster: one chunk per
// map output, block reads by byte range, spills, empty outputs, first commit
// wins, removed chunks, and the local-FS tier.
//
// With $EMBER_TEST_KILN_METADATA set (Kiln's URL list), the tests use that
// cluster instead; with $EMBER_TEST_KILN_RDMA=1 too, rdmaTransfersWithoutCopies
// checks that the client moves data over RDMA without copies, e.g. against a
// `start_local_cluster.sh -R` cluster that runs as another host.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>

#include <folly/ScopeGuard.h>
#include <folly/coro/BlockingWait.h>

#include "ember/shuffle/KilnShuffle.h"
#include "kiln/client/Client.h"
#include "kiln/local/LocalCluster.h"
#include "velox/common/base/Exceptions.h"

namespace ember::shuffle {
namespace {

namespace fs = std::filesystem;

class KilnShuffleTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    if (std::getenv("EMBER_TEST_KILN_METADATA") == nullptr) {
      cluster_ = new kiln::local::LocalCluster(kiln::local::LocalClusterOptions{
          .dramServers = 1, .localFsServers = 1});
    }
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

  // As a URL list, "tcp://host:port", which block URLs must carry intact.
  static std::string metadata() {
    const char* external = std::getenv("EMBER_TEST_KILN_METADATA");
    return external != nullptr ? std::string(external)
                               : cluster_->metadataAddress().toString();
  }

  std::unique_ptr<ShuffleWriter> writer(
      int32_t shuffleId,
      int32_t mapId,
      int32_t numPartitions,
      const std::string& attemptTag = "attempt0",
      int64_t spillThreshold = ShuffleBlockStore::kDefaultSpillThreshold,
      const std::string& tier = "dram") {
    return createKilnWriter(
        store_,
        metadata(),
        tier,
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
    auto block = folly::coro::blockingWait(co_fetchKilnBlock(url));
    for (auto& page : splitFrames(std::move(block))) {
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

TEST_F(KilnShuffleTest, manyBlocksHeldAtOnce) {
  // More blocks than pooled buffers, all held at once as Velox may: the
  // later ones take unpooled memory. Then again, reusing the pool.
  std::vector<int32_t> counts(40, 0);
  for (size_t p = 0; p < counts.size(); ++p) {
    counts[p] = p + 1;
  }
  auto status = writeMap(6, 0, counts);
  for (int round = 0; round < 2; ++round) {
    std::vector<std::unique_ptr<folly::IOBuf>> blocks;
    int64_t offset = 0;
    for (int32_t p = 0; p < counts.size(); ++p) {
      const auto length = status.partitionLengths[p];
      blocks.push_back(folly::coro::blockingWait(co_fetchKilnBlock(
          blockUrl(status.location, {6, 0, p}, offset, length))));
      offset += length;
    }
    for (int32_t p = 0; p < counts.size(); ++p) {
      auto pages = splitFrames(std::move(blocks[p]));
      ASSERT_EQ(pages.size(), counts[p]);
      EXPECT_EQ(
          pages.back()->moveToFbString().toStdString(),
          fmt::format("{}-{}", p, counts[p] - 1));
    }
  }
}

TEST_F(KilnShuffleTest, rdmaTransfersWithoutCopies) {
  const char* rdma = std::getenv("EMBER_TEST_KILN_RDMA");
  if (rdma == nullptr || std::string(rdma) != "1") {
    GTEST_SKIP() << "Needs EMBER_TEST_KILN_RDMA=1 and an RDMA cluster";
  }
  auto client = kilnClient(metadata());
  const auto before = client->stats();
  const std::string data(1 << 20, 'r');
  auto mapWriter = writer(7, 0, 2);
  for (int i = 0; i < 6; ++i) {
    // Partition 1 is larger than a pooled buffer.
    mapWriter->collect(i == 0 ? 0 : 1, folly::IOBuf::copyBuffer(data));
  }
  auto status = mapWriter->commit();
  EXPECT_EQ(readBlock(status, 0).size(), 1);
  EXPECT_EQ(readBlock(status, 1).size(), 5);

  const auto after = client->stats();
  EXPECT_TRUE(after.rdmaMetadata);
  EXPECT_TRUE(after.rdmaData);
  // Written once and read once, all straight from registered memory.
  const auto bytes = status.partitionLengths[0] + status.partitionLengths[1];
  EXPECT_GE(after.rdmaBytes - before.rdmaBytes, 2 * bytes);
  EXPECT_EQ(after.rdmaCopiedBytes, before.rdmaCopiedBytes);
}

TEST_F(KilnShuffleTest, removedChunkFailsTheFetch) {
  auto status = writeMap(5, 0, {2});
  const auto chunk =
      kiln::ChunkId::parse(parseKilnLocation(status.location).second);
  ASSERT_TRUE(folly::coro::blockingWait(kilnClient(metadata())->co_remove(chunk)));
  EXPECT_THROW(readBlock(status, 0), std::exception);
}

TEST_F(KilnShuffleTest, localFsTier) {
  auto mapWriter = writer(8, 0, 2, "attempt0",
      ShuffleBlockStore::kDefaultSpillThreshold, "localfs");
  mapWriter->collect(0, folly::IOBuf::copyBuffer("on disk"));
  mapWriter->collect(1, folly::IOBuf::copyBuffer("also on disk"));
  auto status = mapWriter->commit();
  EXPECT_EQ(readBlock(status, 0), (std::vector<std::string>{"on disk"}));
  EXPECT_EQ(readBlock(status, 1), (std::vector<std::string>{"also on disk"}));

  const auto chunk =
      kiln::ChunkId::parse(parseKilnLocation(status.location).second);
  auto info = folly::coro::blockingWait(kilnClient(metadata())->co_stat(chunk));
  ASSERT_EQ(info.replicas.size(), 1);
  EXPECT_EQ(info.replicas[0].tier, kiln::Tier::kLocalFs);
}

TEST_F(KilnShuffleTest, unknownTierFails) {
  EXPECT_THROW(
      writer(9, 0, 1, "attempt0", ShuffleBlockStore::kDefaultSpillThreshold,
          "objectstore"),
      facebook::velox::VeloxUserError);
}

// A shuffle bigger than Kiln's DRAM loses its first map outputs to eviction
// in the DRAM tier, and keeps them all in the larger local-FS tier.
TEST(KilnTierTest, localFsHoldsMoreThanDram) {
  // 8 MiB of DRAM, 16 MiB of local FS. Evicted blocks are reused at once,
  // so reading an evicted output fails or finds another output's bytes.
  kiln::local::LocalCluster cluster(kiln::local::LocalClusterOptions{
      .dramServers = 1,
      .localFsServers = 2,
      .regionSize = 8 << 20,
      .metadata = {.gracePeriod = std::chrono::milliseconds(0)}});
  const auto metadata = cluster.metadataAddress().toString();
  const auto dir = fs::temp_directory_path() /
      fmt::format("ember_kiln_tier_{}", ::getpid());
  auto store = std::make_shared<ShuffleBlockStore>("test-executor", dir.string());
  SCOPE_EXIT {
    fs::remove_all(dir);
  };

  // Six map outputs of two 1 MiB Kiln blocks each, 12 MiB in all, each of
  // its own letter.
  auto data = [](int32_t mapId) {
    return std::string((2 << 20) - 4, 'a' + mapId);
  };
  auto readable = [&](const std::string& tier, int32_t shuffleId) {
    std::vector<MapStatus> outputs;
    for (int32_t mapId = 0; mapId < 6; ++mapId) {
      auto mapWriter = createKilnWriter(
          store, metadata, tier, shuffleId, mapId, 1, "attempt0",
          ShuffleBlockStore::kDefaultSpillThreshold);
      mapWriter->collect(0, folly::IOBuf::copyBuffer(data(mapId)));
      outputs.push_back(mapWriter->commit());
    }
    int32_t count = 0;
    for (int32_t mapId = 0; mapId < 6; ++mapId) {
      const auto& status = outputs[mapId];
      try {
        auto block = folly::coro::blockingWait(co_fetchKilnBlock(blockUrl(
            status.location, {shuffleId, mapId, 0}, 0,
            status.partitionLengths[0])));
        auto pages = splitFrames(std::move(block));
        count += pages.size() == 1 &&
            pages[0]->moveToFbString().toStdString() == data(mapId);
      } catch (const std::exception&) {
      }
    }
    return count;
  };
  EXPECT_LT(readable("dram", 0), 6);
  EXPECT_EQ(readable("localfs", 1), 6);
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

  // Kiln's URL lists keep their '/'s escaped inside the location.
  const auto urls = "tcp://meta:7470,rdma://10.0.0.1:7471";
  const auto listed = kilnLocation(urls, "abc123");
  EXPECT_EQ(
      listed, "kiln://tcp:%2F%2Fmeta:7470,rdma:%2F%2F10.0.0.1:7471/abc123");
  EXPECT_EQ(
      parseKilnLocation(listed),
      (std::pair<std::string, std::string>{urls, "abc123"}));
  const auto listedUrl = blockUrl(listed, id, 100, 42);
  EXPECT_EQ(parseBlockUrl(listedUrl).first, listed);
  EXPECT_EQ(kilnBlockRange(listedUrl), (std::pair<int64_t, int64_t>{100, 42}));
  EXPECT_EQ(
      parseKilnLocation(kilnLocation("odd%name/x:1", "")).first,
      "odd%name/x:1");

  // A fetch failure names the map output's location, as the driver knows it.
  const FetchFailure failure{location, id};
  auto found = FetchFailure::find("Error: " + failure.message("not found"));
  ASSERT_TRUE(found.has_value());
  EXPECT_EQ(found->location, location);
  EXPECT_EQ(found->block, id);
}

} // namespace
} // namespace ember::shuffle
