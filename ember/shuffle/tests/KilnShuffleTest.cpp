// The Kiln shuffle store against an in-process Kiln cluster: one chunk per
// map output, block reads by byte range, spills, appends without spills,
// empty outputs, first commit wins, removed chunks, and the local-FS tier.
//
// With $EMBER_TEST_KILN_METADATA set (Kiln's URL list), the tests use that
// cluster instead. rdmaTransfersWithoutCopies checks that the client moves
// data over RDMA without copies, against a cluster of its own; it needs a
// -c kiln.rdma=true build and an RDMA device (e.g. Soft-RoCE's rxe0, from
// rlib's scripts/setup-rxe.sh), else it skips.

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>

#include <folly/ScopeGuard.h>
#include <folly/coro/BlockingWait.h>
#include <folly/executors/GlobalExecutor.h>

#include "ember/shuffle/KilnShuffle.h"
#include "ember/shuffle/ShuffleBlockSource.h"
#include "kiln/client/BufferPool.h"
#include "kiln/client/Client.h"
#include "kiln/local/LocalCluster.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/ExchangeQueue.h"
#include "velox/exec/ExchangeSource.h"

namespace ember::shuffle {
namespace {

namespace fs = std::filesystem;

// The pages of a whole block.
std::vector<std::unique_ptr<folly::IOBuf>> splitFrames(
    std::unique_ptr<folly::IOBuf> block) {
  folly::IOBufQueue queue(folly::IOBufQueue::cacheChainLength());
  queue.append(std::move(block));
  return takeFrames(queue);
}

class KilnShuffleTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    facebook::velox::memory::MemoryManager::testingSetInstance(
        facebook::velox::memory::MemoryManager::Options{});
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
      const std::string& tier = "dram",
      uint64_t bucket = kiln::kDefaultBucket,
      bool spill = true) {
    return createKilnWriter(
        store_,
        metadata(),
        tier,
        bucket,
        shuffleId,
        mapId,
        numPartitions,
        attemptTag,
        spill);
  }

  // Writes pages "<p>-<i>" for i < count[p] in round-robin order over the
  // partitions.
  MapStatus writeMap(
      int32_t shuffleId,
      int32_t mapId,
      const std::vector<int32_t>& counts,
      bool spillEachPage = false,
      bool spill = true) {
    auto mapWriter = writer(
        shuffleId,
        mapId,
        counts.size(),
        "attempt0",
        "dram",
        kiln::kDefaultBucket,
        spill);
    const auto max = *std::max_element(counts.begin(), counts.end());
    for (int32_t i = 0; i < max; ++i) {
      for (int32_t p = 0; p < counts.size(); ++p) {
        if (i < counts[p]) {
          mapWriter->collect(
              p, folly::IOBuf::copyBuffer(fmt::format("{}-{}", p, i)));
          if (spillEachPage) {
            mapWriter->spill();
          }
        }
      }
    }
    return mapWriter->commit();
  }

  // The pages of partition 'reduceId' of 'status', read as a reduce task
  // would: at the byte ranges the driver puts into the block URL.
  static std::vector<std::string> readBlock(
      const MapStatus& status,
      int32_t reduceId) {
    auto url = blockUrl(
        status.location, {0, 0, reduceId}, status.blockRanges(reduceId));
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
  EXPECT_EQ(
      status.segments, (std::vector<std::vector<int64_t>>{{21, 0, 14}}));
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
  auto status = writeMap(1, 0, {5, 4}, /*spillEachPage=*/true);
  EXPECT_EQ(
      readBlock(status, 0),
      (std::vector<std::string>{"0-0", "0-1", "0-2", "0-3", "0-4"}));
  EXPECT_EQ(
      readBlock(status, 1), (std::vector<std::string>{"1-0", "1-1", "1-2", "1-3"}));
  EXPECT_TRUE(fs::is_empty(dir_));
}

// Without spills, every time the buffer passes the threshold, here after
// every page, its pages go to the chunk as a segment of their own; a block
// is then one range per segment that has bytes of it.
TEST_F(KilnShuffleTest, appendsSegmentsWithoutSpillFiles) {
  auto status = writeMap(10, 0, {5, 4}, /*spillEachPage=*/true, /*spill=*/false);
  ASSERT_EQ(status.segments.size(), 9);
  EXPECT_EQ(status.segments[0], (std::vector<int64_t>{7, 0}));
  EXPECT_EQ(status.segments[1], (std::vector<int64_t>{0, 7}));
  EXPECT_EQ(status.partitionLengths(), (std::vector<int64_t>{35, 28}));
  EXPECT_EQ(
      status.blockRanges(1),
      (std::vector<std::pair<int64_t, int64_t>>{
          {7, 7}, {21, 7}, {35, 7}, {49, 7}}));
  EXPECT_EQ(
      readBlock(status, 0),
      (std::vector<std::string>{"0-0", "0-1", "0-2", "0-3", "0-4"}));
  EXPECT_EQ(
      readBlock(status, 1), (std::vector<std::string>{"1-0", "1-1", "1-2", "1-3"}));
  EXPECT_TRUE(fs::is_empty(dir_));

  const auto chunk =
      kiln::ChunkId::parse(parseKilnLocation(status.location).second);
  auto info = folly::coro::blockingWait(kilnClient(metadata())->co_stat(chunk));
  EXPECT_EQ(info.size, 63);
}

// Without spills, released pages go to the chunk as a segment of their own.
TEST_F(KilnShuffleTest, spillAppendsSegment) {
  auto mapWriter = writer(
      15,
      0,
      1,
      "attempt0",
      "dram",
      kiln::kDefaultBucket,
      /*spill=*/false);
  mapWriter->collect(0, folly::IOBuf::copyBuffer("first"));
  mapWriter->spill();
  mapWriter->spill();
  mapWriter->collect(0, folly::IOBuf::copyBuffer("second"));
  auto status = mapWriter->commit();
  EXPECT_EQ(status.segments.size(), 2);
  EXPECT_EQ(readBlock(status, 0), (std::vector<std::string>{"first", "second"}));
  EXPECT_TRUE(fs::is_empty(dir_));
}

// Appends of several Kiln blocks each, with pages left in memory at commit:
// those go last, as one more segment.
TEST_F(KilnShuffleTest, largeAppendsWithoutSpills) {
  const std::string data(3 << 20, 'y');
  auto mapWriter = writer(
      11,
      0,
      2,
      "attempt0",
      "dram",
      kiln::kDefaultBucket,
      /*spill=*/false);
  for (int i = 0; i < 5; ++i) {
    mapWriter->collect(i % 2, folly::IOBuf::copyBuffer(data + std::to_string(i)));
    if (i % 2 == 1) {
      mapWriter->spill();
    }
  }
  auto status = mapWriter->commit();
  const int64_t frame = 4 + (3 << 20) + 1;
  EXPECT_EQ(
      status.segments,
      (std::vector<std::vector<int64_t>>{
          {frame, frame}, {frame, frame}, {frame, 0}}));
  EXPECT_EQ(
      readBlock(status, 0),
      (std::vector<std::string>{data + "0", data + "2", data + "4"}));
  EXPECT_EQ(
      readBlock(status, 1), (std::vector<std::string>{data + "1", data + "3"}));
}

// Never past the threshold, the output is written at commit as one segment.
TEST_F(KilnShuffleTest, noSpillsNeededIsOneSegment) {
  auto status = writeMap(
      12, 0, {2, 1}, /*spillEachPage=*/false, /*spill=*/false);
  EXPECT_EQ(status.segments, (std::vector<std::vector<int64_t>>{{14, 7}}));
  EXPECT_EQ(readBlock(status, 0), (std::vector<std::string>{"0-0", "0-1"}));
}

// An aborted writer drops its chunk; another attempt then commits.
TEST_F(KilnShuffleTest, abortDropsAppendedSegments) {
  auto aborted = writer(
      13, 0, 1, "attempt0", "dram", kiln::kDefaultBucket, /*spill=*/false);
  aborted->collect(0, folly::IOBuf::copyBuffer("lost"));
  aborted->spill();
  aborted->collect(0, folly::IOBuf::copyBuffer("also lost"));
  aborted->spill();
  aborted->abort();
  EXPECT_FALSE(store_->hasMapOutput(13, 0));

  auto retry = writer(
      13, 0, 1, "attempt1", "dram", kiln::kDefaultBucket, /*spill=*/false);
  retry->collect(0, folly::IOBuf::copyBuffer("kept"));
  retry->spill();
  auto status = retry->commit();
  EXPECT_EQ(readBlock(status, 0), (std::vector<std::string>{"kept"}));
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
      status.partitionLengths(),
      (std::vector<int64_t>{2 * (4 + (3 << 20)), 4 + (3 << 20)}));
  EXPECT_EQ(readBlock(status, 0), (std::vector<std::string>{data, data}));
  EXPECT_EQ(readBlock(status, 1), (std::vector<std::string>{data}));
}

// Spilled runs and buffered pages, regrouped at commit into many appends:
// each buffer is filled while the previous one is appended.
TEST_F(KilnShuffleTest, largeSpilledOutputSpansAppends) {
  const std::string data(3 << 20, 'z');
  auto mapWriter = writer(14, 0, 2);
  for (int i = 0; i < 7; ++i) {
    mapWriter->collect(i % 2, folly::IOBuf::copyBuffer(data + std::to_string(i)));
    if (i % 2 == 1) {
      mapWriter->spill();
    }
  }
  auto status = mapWriter->commit();
  ASSERT_EQ(status.segments.size(), 1);
  EXPECT_EQ(
      readBlock(status, 0),
      (std::vector<std::string>{data + "0", data + "2", data + "4", data + "6"}));
  EXPECT_EQ(
      readBlock(status, 1),
      (std::vector<std::string>{data + "1", data + "3", data + "5"}));
  EXPECT_TRUE(fs::is_empty(dir_));
}

TEST_F(KilnShuffleTest, emptyOutputHasNoChunk) {
  auto status = writer(3, 0, 2)->commit();
  EXPECT_TRUE(isKilnLocation(status.location));
  EXPECT_EQ(parseKilnLocation(status.location).second, "");
  EXPECT_EQ(status.partitionLengths(), (std::vector<int64_t>{0, 0}));
  EXPECT_TRUE(status.blockRanges(0).empty());
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

TEST(TakeFramesTest, leavesCutPages) {
  folly::IOBufQueue queue{folly::IOBufQueue::cacheChainLength()};
  auto frame = [](const std::string& page) {
    std::string bytes(sizeof(uint32_t), '\0');
    const uint32_t length = page.size();
    std::memcpy(bytes.data(), &length, sizeof(length));
    return bytes + page;
  };
  const auto data = frame("first") + frame("") + frame("second");
  // Cut inside the second length and inside "second".
  queue.append(folly::IOBuf::copyBuffer(data.substr(0, 11)));
  auto pages = takeFrames(queue);
  ASSERT_EQ(pages.size(), 1);
  EXPECT_EQ(pages[0]->moveToFbString().toStdString(), "first");
  queue.append(folly::IOBuf::copyBuffer(data.substr(11, 10)));
  pages = takeFrames(queue);
  ASSERT_EQ(pages.size(), 1);
  EXPECT_EQ(pages[0]->computeChainDataLength(), 0);
  queue.append(folly::IOBuf::copyBuffer(data.substr(21)));
  pages = takeFrames(queue);
  ASSERT_EQ(pages.size(), 1);
  EXPECT_EQ(pages[0]->moveToFbString().toStdString(), "second");
  EXPECT_TRUE(queue.empty());
}

TEST_F(KilnShuffleTest, sourceStreamsBlockInPieces) {
  // Pages of 700 KiB, so that most are cut between two pieces of 1 MiB.
  // With spill=false, each page is a segment of its own, read as one piece.
  for (const bool spill : {true, false}) {
    SCOPED_TRACE(spill);
    auto mapWriter = writer(
        12, spill ? 0 : 1, 1, "attempt0", "dram", kiln::kDefaultBucket, spill);
    std::vector<std::string> data;
    for (int i = 0; i < 5; ++i) {
      data.push_back(std::string(700 << 10, 'a' + i));
      mapWriter->collect(0, folly::IOBuf::copyBuffer(data.back()));
      if (!spill) {
        mapWriter->spill();
      }
    }
    auto status = mapWriter->commit();
    const auto url = blockUrl(
        status.location, {12, spill ? 0 : 1, 0}, status.blockRanges(0));

    auto pool = facebook::velox::memory::memoryManager()->addLeafPool();
    auto queue = std::make_shared<facebook::velox::exec::ExchangeQueue>(1, 0);
    {
      std::lock_guard<std::mutex> l(queue->mutex());
      queue->addSourceLocked();
    }
    queue->noMoreSources();
    const ShuffleContextScope scope(*pool, nullptr, nullptr, nullptr);
    auto source = createShuffleBlockSource(url, 0, queue, pool.get());
    auto request = [&](auto future) {
      {
        std::lock_guard<std::mutex> l(queue->mutex());
        EXPECT_TRUE(source->shouldRequestLocked());
      }
      return std::move(future).via(folly::getGlobalCPUExecutor()).get();
    };
    // Sizes come without a read: one piece at a time.
    auto sizes = request(folly::makeSemiFuture().deferValue([&](auto&&) {
      return source->requestDataSizes(std::chrono::seconds(1));
    }));
    EXPECT_EQ(sizes.bytes, 0);
    EXPECT_EQ(
        sizes.remainingBytes,
        std::vector<int64_t>{std::min<int64_t>(
            status.blockRanges(0)[0].second, 1 << 20)});
    // Each request of a piece returns whole pages, about that many bytes.
    int requests = 0;
    for (bool atEnd = false; !atEnd; ++requests) {
      auto response = request(folly::makeSemiFuture().deferValue([&](auto&&) {
        return source->request(1 << 20, std::chrono::seconds(1));
      }));
      EXPECT_GT(response.bytes, 0);
      atEnd = response.atEnd;
      EXPECT_EQ(response.remainingBytes.empty(), atEnd);
    }
    EXPECT_GT(requests, 2);

    std::vector<std::string> pages;
    bool atEnd = false;
    facebook::velox::ContinueFuture future;
    facebook::velox::ContinuePromise stale;
    std::lock_guard<std::mutex> l(queue->mutex());
    while (!atEnd) {
      auto dequeued =
          queue->dequeueLocked(0, 1 << 30, &atEnd, &future, &stale);
      ASSERT_TRUE(!dequeued.empty() || atEnd);
      for (auto& page : dequeued) {
        pages.push_back(page->getIOBuf()->moveToFbString().toStdString());
      }
    }
    EXPECT_EQ(pages, data);
    EXPECT_EQ(source->stats().at("kilnCopiedBytes"), 0);
  }
}

TEST_F(KilnShuffleTest, manyBlocksHeldAtOnce) {
  // Many blocks, all held at once as Velox may: the first in buffers of the
  // read pool, the others copied once it has none free. Then again.
  std::vector<int32_t> counts(40, 0);
  for (size_t p = 0; p < counts.size(); ++p) {
    counts[p] = p + 1;
  }
  auto status = writeMap(6, 0, counts);
  for (int round = 0; round < 2; ++round) {
    std::vector<std::unique_ptr<folly::IOBuf>> blocks;
    for (int32_t p = 0; p < counts.size(); ++p) {
      blocks.push_back(folly::coro::blockingWait(co_fetchKilnBlock(
          blockUrl(status.location, {6, 0, p}, status.blockRanges(p)))));
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

// Against its own cluster, with pools of two buffers: blocks read keep read
// buffers while the read pool has them free and are read through the
// staging pool, in batches if larger, when not, without waiting; writes
// take theirs from the write pool and give them back.
TEST(KilnBufferPoolTest, readsNeverWaitForHeldBuffers) {
  kiln::local::LocalCluster cluster{kiln::local::LocalClusterOptions{
      .dramServers = 1, .localFsServers = 0}};
  const auto metadata = cluster.metadataAddress().toString();
  setKilnBufferPools(2, 2, 2);
  SCOPE_EXIT {
    setKilnBufferPools(kDefaultKilnReadBuffers, kDefaultKilnWriteBuffers);
  };
  auto reads = kilnReadPool(metadata);
  auto staging = kilnStagingPool(metadata);
  auto writes = kilnWritePool(metadata);
  ASSERT_EQ(reads->count(), 2);
  ASSERT_EQ(staging->count(), 2);
  ASSERT_EQ(writes->count(), 2);

  const auto dir = fs::temp_directory_path() /
      fmt::format("ember_kiln_pool_{}", ::getpid());
  auto store = std::make_shared<ShuffleBlockStore>("test-executor", dir.string());
  SCOPE_EXIT {
    fs::remove_all(dir);
  };
  // Partition 0 is three pages of 3 MiB, more than either read pool's
  // 8 MiB; spill=false appends each page as a segment, from the write pool
  // too. Partitions 1 to 3 are one small page each.
  const std::string data(3 << 20, 'p');
  for (const bool spill : {true, false}) {
    SCOPED_TRACE(spill);
    auto mapWriter = createKilnWriter(
        store, metadata, "dram", kiln::kDefaultBucket, spill ? 0 : 1, 0, 4,
        "attempt0", spill);
    for (int i = 0; i < 3; ++i) {
      mapWriter->collect(0, folly::IOBuf::copyBuffer(data));
      if (!spill) {
        mapWriter->spill();
      }
    }
    for (int32_t p = 1; p < 4; ++p) {
      mapWriter->collect(p, folly::IOBuf::copyBuffer(fmt::format("page {}", p)));
      if (!spill) {
        mapWriter->spill();
      }
    }
    auto status = mapWriter->commit();
    EXPECT_EQ(writes->available(), 2);

    auto fetch = [&](int32_t p) {
      return folly::coro::blockingWait(co_fetchKilnBlock(
          blockUrl(status.location, {0, 0, p}, status.blockRanges(p))));
    };
    auto text = [](std::unique_ptr<folly::IOBuf> block) {
      return splitFrames(std::move(block))[0]->moveToFbString().toStdString();
    };
    // Blocks 1 and 2 keep a read buffer each, and block 3 finds none.
    std::vector<std::unique_ptr<folly::IOBuf>> held;
    held.push_back(fetch(1));
    EXPECT_EQ(reads->available(), 1);
    held.push_back(fetch(2));
    EXPECT_EQ(reads->available(), 0);
    EXPECT_EQ(text(fetch(3)), "page 3");
    // Too large for the read pool, so read through staging in two batches.
    held.clear();
    auto pages = splitFrames(fetch(0));
    ASSERT_EQ(pages.size(), 3);
    for (auto& page : pages) {
      EXPECT_EQ(page->moveToFbString().toStdString(), data);
    }
    EXPECT_EQ(reads->available(), 2);
    EXPECT_EQ(staging->available(), 2);
  }
}

// Against its own cluster, whose metadata and DRAM servers serve RDMA, with
// shared memory off so that RDMA is used on this one host.
TEST_F(KilnShuffleTest, rdmaTransfersWithoutCopies) {
  kiln::local::LocalCluster cluster{kiln::local::LocalClusterOptions{
      .dramServers = 1, .localFsServers = 0, .rdma = true}};
  if (!cluster.metadataAddress().rdma.has_value()) {
    GTEST_SKIP() << "Needs a -c kiln.rdma=true build and an RDMA device";
  }
  setKilnUseShm(false);
  SCOPE_EXIT {
    setKilnUseShm(true);
  };
  const auto metadata = cluster.metadataAddress().toString();
  auto client = kilnClient(metadata);
  const std::string data(1 << 20, 'r');
  // Written at commit, and appended as the map runs (two segments).
  for (const bool spill : {true, false}) {
    SCOPED_TRACE(spill);
    const auto before = client->stats();
    auto mapWriter = createKilnWriter(
        store_,
        metadata,
        "dram",
        kiln::kDefaultBucket,
        7,
        spill ? 0 : 1,
        2,
        "attempt0",
        spill);
    for (int i = 0; i < 6; ++i) {
      // Partition 1 is larger than a pooled buffer.
      mapWriter->collect(i == 0 ? 0 : 1, folly::IOBuf::copyBuffer(data));
      if (i == 2 && !spill) {
        mapWriter->spill();
      }
    }
    auto status = mapWriter->commit();
    EXPECT_EQ(status.segments.size(), spill ? 1 : 2);
    EXPECT_EQ(readBlock(status, 0).size(), 1);
    EXPECT_EQ(readBlock(status, 1).size(), 5);

    const auto after = client->stats();
    EXPECT_TRUE(after.rdmaMetadata);
    EXPECT_TRUE(after.rdmaData);
    // Written once and read once, all straight from registered memory.
    const auto lengths = status.partitionLengths();
    const auto bytes = lengths[0] + lengths[1];
    EXPECT_GE(after.rdmaBytes - before.rdmaBytes, 2 * bytes);
    EXPECT_EQ(after.rdmaCopiedBytes, before.rdmaCopiedBytes);
  }
}

TEST_F(KilnShuffleTest, removedChunkFailsTheFetch) {
  auto status = writeMap(5, 0, {2});
  const auto chunk =
      kiln::ChunkId::parse(parseKilnLocation(status.location).second);
  ASSERT_TRUE(folly::coro::blockingWait(kilnClient(metadata())->co_remove(chunk)));
  EXPECT_THROW(readBlock(status, 0), std::exception);
}

TEST_F(KilnShuffleTest, localFsTier) {
  auto mapWriter = writer(8, 0, 2, "attempt0", "localfs");
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
      writer(9, 0, 1, "attempt0", "objectstore"),
      facebook::velox::VeloxUserError);
}

// Kiln with 8 MiB of DRAM and 16 MiB of local FS. Evicted blocks are reused
// at once, so reading an evicted output fails or finds another output's
// bytes.
class KilnTierTest : public testing::Test {
 protected:
  KilnTierTest()
      : cluster_{kiln::local::LocalClusterOptions{
            .dramServers = 1,
            .localFsServers = 2,
            .regionSize = 8 << 20,
            .metadata = {.gracePeriod = std::chrono::milliseconds(0)}}},
        dir_{fs::temp_directory_path() /
             fmt::format("ember_kiln_tier_{}", ::getpid())},
        store_{std::make_shared<ShuffleBlockStore>(
            "test-executor", dir_.string())} {}

  ~KilnTierTest() override {
    fs::remove_all(dir_);
  }

  // Map output 'mapId': two 1 MiB Kiln blocks of its own letter.
  static std::string data(int32_t mapId) {
    return std::string((2 << 20) - 4, 'a' + mapId);
  }

  // Writes six map outputs, 12 MiB in all, into 'tier', and returns how
  // many read back intact. 'tiers' gets the tier of each output's chunk.
  int32_t writeAndRead(
      const std::string& tier,
      int32_t shuffleId,
      std::vector<kiln::Tier>* tiers = nullptr) {
    const auto metadata = cluster_.metadataAddress().toString();
    std::vector<MapStatus> outputs;
    for (int32_t mapId = 0; mapId < 6; ++mapId) {
      auto mapWriter = createKilnWriter(
          store_, metadata, tier, kiln::kDefaultBucket, shuffleId, mapId, 1,
          "attempt0");
      mapWriter->collect(0, folly::IOBuf::copyBuffer(data(mapId)));
      outputs.push_back(mapWriter->commit());
      if (tiers != nullptr) {
        const auto chunk = kiln::ChunkId::parse(
            parseKilnLocation(outputs.back().location).second);
        auto info =
            folly::coro::blockingWait(kilnClient(metadata)->co_stat(chunk));
        tiers->push_back(info.replicas.at(0).tier);
      }
    }
    int32_t count = 0;
    for (int32_t mapId = 0; mapId < 6; ++mapId) {
      const auto& status = outputs[mapId];
      try {
        auto block = folly::coro::blockingWait(co_fetchKilnBlock(blockUrl(
            status.location, {shuffleId, mapId, 0}, status.blockRanges(0))));
        auto pages = splitFrames(std::move(block));
        count += pages.size() == 1 &&
            pages[0]->moveToFbString().toStdString() == data(mapId);
      } catch (const std::exception&) {
      }
    }
    return count;
  }

  kiln::local::LocalCluster cluster_;
  fs::path dir_;
  std::shared_ptr<ShuffleBlockStore> store_;
};

// A shuffle bigger than Kiln's DRAM loses its first map outputs to eviction
// in DRAM alone.
TEST_F(KilnTierTest, dramEvicts) {
  EXPECT_LT(writeAndRead("dram", 0), 6);
}

TEST_F(KilnTierTest, localFsHoldsAll) {
  EXPECT_EQ(writeAndRead("localfs", 0), 6);
}

// Outputs go to DRAM while they fit, then to local FS, without evicting.
TEST_F(KilnTierTest, dramThenLocalFs) {
  std::vector<kiln::Tier> tiers;
  EXPECT_EQ(writeAndRead("dram+localfs", 0, &tiers), 6);
  EXPECT_EQ(
      tiers,
      (std::vector<kiln::Tier>{
          kiln::Tier::kDram,
          kiln::Tier::kDram,
          kiln::Tier::kDram,
          kiln::Tier::kDram,
          kiln::Tier::kLocalFs,
          kiln::Tier::kLocalFs}));
  EXPECT_EQ(cluster_.metadata().evictions(), 0);
}

TEST(KilnBlockUrlTest, roundTrip) {
  BlockId id{7, 12, 3};
  const auto location = kilnLocation("meta:7000", "abc123");
  EXPECT_EQ(location, "kiln://meta:7000/abc123");
  auto url = blockUrl(location, id, {{100, 42}});
  EXPECT_EQ(url, "kiln://meta:7000/abc123/100/42/7/12/3");
  EXPECT_TRUE(isBlockUrl(url));
  auto [parsedLocation, parsed] = parseBlockUrl(url);
  EXPECT_EQ(parsedLocation, location);
  EXPECT_EQ(parsed, id);
  EXPECT_EQ(
      blockUrlRanges(url),
      (std::vector<std::pair<int64_t, int64_t>>{{100, 42}}));

  // A block of several segments has one range in each.
  auto ranged = blockUrl(location, id, {{0, 10}, {50, 20}});
  EXPECT_EQ(ranged, "kiln://meta:7000/abc123/0,50/10,20/7/12/3");
  EXPECT_EQ(parseBlockUrl(ranged).second, id);
  EXPECT_EQ(
      blockUrlRanges(ranged),
      (std::vector<std::pair<int64_t, int64_t>>{{0, 10}, {50, 20}}));

  // Kiln's URL lists keep their '/'s escaped inside the location.
  const auto urls = "tcp://meta:7470,rdma://10.0.0.1:7471";
  const auto listed = kilnLocation(urls, "abc123");
  EXPECT_EQ(
      listed, "kiln://tcp:%2F%2Fmeta:7470,rdma:%2F%2F10.0.0.1:7471/abc123");
  EXPECT_EQ(
      parseKilnLocation(listed),
      (std::pair<std::string, std::string>{urls, "abc123"}));
  const auto listedUrl = blockUrl(listed, id, {{100, 42}, {200, 1}});
  EXPECT_EQ(parseBlockUrl(listedUrl).first, listed);
  EXPECT_EQ(
      blockUrlRanges(listedUrl),
      (std::vector<std::pair<int64_t, int64_t>>{{100, 42}, {200, 1}}));
  EXPECT_EQ(
      parseKilnLocation(kilnLocation("odd%name/x:1", "")).first,
      "odd%name/x:1");

}

// A reader's failure reaches the task it belongs to, and only that task.
TEST_F(KilnShuffleTest, fetchFailureReachesItsTask) {
  auto task = facebook::velox::memory::memoryManager()->addRootPool("task");
  auto reader = task->addLeafChild("reader");
  auto other = facebook::velox::memory::memoryManager()->addRootPool("other");
  const BlockId first{1, 2, 3};
  const BlockId second{4, 5, 6};
  {
    const ShuffleContextScope scope(*task, nullptr, nullptr, nullptr);
    auto context = ShuffleContext::of(*reader);
    EXPECT_FALSE(scope.context().fetchFailures.failure().has_value());
    EXPECT_THROW(context->fetchFailures.fail({"a", first}, "gone"), std::exception);
    EXPECT_THROW(context->fetchFailures.fail({"b", second}, "gone"), std::exception);
    ASSERT_TRUE(scope.context().fetchFailures.failure().has_value());
    EXPECT_EQ(scope.context().fetchFailures.failure()->location, "a");
    EXPECT_EQ(scope.context().fetchFailures.failure()->block, first);
    EXPECT_THROW(ShuffleContext::of(*other), std::exception);
  }
  EXPECT_THROW(ShuffleContext::of(*reader), std::exception);
}

} // namespace
} // namespace ember::shuffle
