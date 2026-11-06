// The file shuffle's on-disk format: commit, spill and merge, first commit
// wins, block reads, and block URLs.

#include <gtest/gtest.h>

#include <filesystem>

#include <folly/Synchronized.h>
#include <folly/coro/BlockingWait.h>
#include <folly/executors/CPUThreadPoolExecutor.h>

#include "ember/shuffle/BackgroundWork.h"
#include "ember/shuffle/FileShuffle.h"
#include "ember/shuffle/ShuffleBlockSource.h"
#include "velox/common/memory/Memory.h"
#include "velox/exec/ExchangeQueue.h"

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

// Fetches through 'inner' and records the size of each fetch.
class RecordingTransport : public BlockTransport {
 public:
  explicit RecordingTransport(std::shared_ptr<BlockTransport> inner)
      : inner_{std::move(inner)} {}

  folly::coro::Task<std::unique_ptr<folly::IOBuf>> co_fetchBlock(
      std::string location,
      BlockId id,
      int64_t offset,
      int64_t length) override {
    {
      auto locked = lengths_.wlock();
      locked->push_back(length);
    }
    return inner_->co_fetchBlock(std::move(location), id, offset, length);
  }

  std::vector<int64_t> lengths() const {
    return lengths_.copy();
  }

 private:
  const std::shared_ptr<BlockTransport> inner_;
  folly::Synchronized<std::vector<int64_t>> lengths_;
};

class FileShuffleTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    facebook::velox::memory::MemoryManager::testingSetInstance(
        facebook::velox::memory::MemoryManager::Options{});
  }

  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_file_shuffle_{}", ::getpid());
    fs::remove_all(dir_);
    store_ = std::make_shared<ShuffleBlockStore>("test-executor", dir_.string());
    transport_ = std::make_shared<LocalBlockTransport>(store_);
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  static std::unique_ptr<folly::IOBuf> page(const std::string& data) {
    return folly::IOBuf::copyBuffer(data);
  }

  static std::vector<std::string> pages(std::unique_ptr<folly::IOBuf> block) {
    std::vector<std::string> result;
    for (auto& page : splitFrames(std::move(block))) {
      result.push_back(page->moveToFbString().toStdString());
    }
    return result;
  }

  std::vector<std::string> readBlock(int32_t shuffleId, int32_t mapId, int32_t reduceId) {
    const auto ranges =
        store_->mapStatus(shuffleId, mapId).blockRanges(reduceId);
    if (ranges.empty()) {
      return {};
    }
    return pages(folly::coro::blockingWait(transport_->co_fetchBlock(
        "test-executor",
        {shuffleId, mapId, reduceId},
        ranges[0].first,
        ranges[0].second)));
  }

  // Writes pages "<p>-<i>" for i < count[p] in round-robin order over the
  // partitions.
  MapStatus writeMap(
      int32_t shuffleId,
      int32_t mapId,
      const std::vector<int32_t>& counts,
      bool spillEachPage = false) {
    auto writer =
        store_->createWriter(shuffleId, mapId, counts.size(), "attempt0");
    const auto max = *std::max_element(counts.begin(), counts.end());
    for (int32_t i = 0; i < max; ++i) {
      for (int32_t p = 0; p < counts.size(); ++p) {
        if (i < counts[p]) {
          writer->collect(p, page(fmt::format("{}-{}", p, i)));
          if (spillEachPage) {
            writer->spill();
          }
        }
      }
    }
    return writer->commit();
  }

  // Temporary files of the writers in the store's directory.
  int64_t tmpFiles() const {
    int64_t count = 0;
    for (const auto& entry : fs::directory_iterator(dir_)) {
      count += entry.path().extension() == ".tmp";
    }
    return count;
  }

  fs::path dir_;
  std::shared_ptr<ShuffleBlockStore> store_;
  std::shared_ptr<LocalBlockTransport> transport_;
};

TEST_F(FileShuffleTest, partitionsAreContiguousAndInOrder) {
  auto status = writeMap(0, 0, {3, 0, 2});
  EXPECT_EQ(status.location, "test-executor");
  ASSERT_EQ(status.segments.size(), 1);
  ASSERT_EQ(status.partitionLengths().size(), 3);
  // Each frame is a 4-byte length plus "p-i".
  EXPECT_EQ(status.partitionLengths(), (std::vector<int64_t>{21, 0, 14}));
  EXPECT_EQ(readBlock(0, 0, 0), (std::vector<std::string>{"0-0", "0-1", "0-2"}));
  EXPECT_TRUE(readBlock(0, 0, 1).empty());
  EXPECT_EQ(readBlock(0, 0, 2), (std::vector<std::string>{"2-0", "2-1"}));
  EXPECT_EQ(fs::file_size(store_->indexFile(0, 0)), 4 * sizeof(int64_t));
}

TEST_F(FileShuffleTest, spilledRunsMergeInWriteOrder) {
  writeMap(1, 0, {5, 4}, /*spillEachPage=*/true);
  EXPECT_EQ(
      readBlock(1, 0, 0),
      (std::vector<std::string>{"0-0", "0-1", "0-2", "0-3", "0-4"}));
  EXPECT_EQ(
      readBlock(1, 0, 1), (std::vector<std::string>{"1-0", "1-1", "1-2", "1-3"}));

  // Spill files are gone, only the data and index files remain.
  std::vector<std::string> files;
  for (const auto& entry : fs::directory_iterator(dir_)) {
    files.push_back(entry.path().filename().string());
  }
  std::sort(files.begin(), files.end());
  EXPECT_EQ(
      files,
      (std::vector<std::string>{"shuffle_1_0.data", "shuffle_1_0.index"}));
}

// A block is read as Velox asks for it, not all at once.
TEST_F(FileShuffleTest, sourceFollowsVeloxFlowControl) {
  // Pages of 700 KiB, so that most are cut between two pieces of 1 MiB.
  auto writer = store_->createWriter(14, 0, 1, "attempt0");
  std::vector<std::string> data;
  for (int i = 0; i < 5; ++i) {
    data.push_back(std::string(700 << 10, 'a' + i));
    writer->collect(0, page(data.back()));
  }
  const auto status = writer->commit();
  const auto ranges = status.blockRanges(0);
  const auto url = blockUrl(status.location, {14, 0, 0}, ranges);

  auto pool = facebook::velox::memory::memoryManager()->addLeafPool();
  auto queue = std::make_shared<facebook::velox::exec::ExchangeQueue>(1, 0);
  {
    std::lock_guard<std::mutex> l(queue->mutex());
    queue->addSourceLocked();
  }
  queue->noMoreSources();
  auto recording = std::make_shared<RecordingTransport>(transport_);
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(1);
  const ShuffleRuntimeScope scope(store_, recording, executor);
  auto source = createShuffleBlockSource(url, 0, queue, pool.get());
  auto request = [&](auto future) {
    {
      std::lock_guard<std::mutex> l(queue->mutex());
      EXPECT_TRUE(source->shouldRequestLocked());
    }
    return std::move(future).via(executor.get()).get();
  };

  // The size comes from the URL: one piece at a time, no bytes.
  auto sizes = request(folly::makeSemiFuture().deferValue([&](auto&&) {
    return source->requestDataSizes(std::chrono::seconds(1));
  }));
  EXPECT_EQ(sizes.bytes, 0);
  EXPECT_EQ(sizes.remainingBytes, std::vector<int64_t>{kBlockPieceBytes});

  // Each request of a piece returns whole pages, about that many bytes.
  int requests = 0;
  for (bool atEnd = false; !atEnd; ++requests) {
    auto response = request(folly::makeSemiFuture().deferValue([&](auto&&) {
      return source->request(kBlockPieceBytes, std::chrono::seconds(1));
    }));
    EXPECT_GT(response.bytes, 0);
    EXPECT_LT(response.bytes, 2 * kBlockPieceBytes);
    atEnd = response.atEnd;
    EXPECT_EQ(response.remainingBytes.empty(), atEnd);
  }
  EXPECT_GT(requests, 2);
  const auto lengths = recording->lengths();
  EXPECT_GT(lengths.size(), 2);
  EXPECT_EQ(
      std::accumulate(lengths.begin(), lengths.end(), int64_t{0}),
      ranges[0].second);
  for (const auto length : lengths) {
    EXPECT_LE(length, kBlockPieceBytes);
  }

  std::vector<std::string> pages;
  bool atEnd = false;
  facebook::velox::ContinueFuture future;
  facebook::velox::ContinuePromise stale;
  std::lock_guard<std::mutex> l(queue->mutex());
  while (!atEnd) {
    auto dequeued = queue->dequeueLocked(0, 1 << 30, &atEnd, &future, &stale);
    ASSERT_TRUE(!dequeued.empty() || atEnd);
    for (auto& dequeuedPage : dequeued) {
      pages.push_back(dequeuedPage->getIOBuf()->moveToFbString().toStdString());
    }
  }
  EXPECT_EQ(pages, data);
}

TEST_F(FileShuffleTest, spillWritesARunOfCollectedPages) {
  auto writer = store_->createWriter(12, 0, 1, "attempt0");
  writer->spill();
  EXPECT_EQ(tmpFiles(), 0);
  writer->collect(0, page("first"));
  writer->spill();
  writer->spill();
  EXPECT_EQ(tmpFiles(), 1);
  writer->collect(0, page("second"));
  writer->commit();
  writer->spill();
  EXPECT_EQ(tmpFiles(), 0);
  EXPECT_EQ(readBlock(12, 0, 0), (std::vector<std::string>{"first", "second"}));
}

TEST_F(FileShuffleTest, firstCommitWins) {
  auto first = store_->createWriter(2, 0, 1, "attempt0");
  auto second = store_->createWriter(2, 0, 1, "attempt1");
  first->collect(0, page("first"));
  second->collect(0, page("second, longer"));
  second->commit();
  auto status = first->commit();
  EXPECT_EQ(readBlock(2, 0, 0), (std::vector<std::string>{"second, longer"}));
  EXPECT_EQ(status.partitionLengths()[0], 4 + 14);
}

TEST_F(FileShuffleTest, abortLeavesNoOutput) {
  {
    auto writer = store_->createWriter(3, 0, 2, "attempt0");
    writer->collect(0, page("x"));
    writer->spill();
    writer->collect(1, page("y"));
    writer->spill();
    writer->abort();
  }
  {
    // Destroyed without commit.
    auto writer = store_->createWriter(3, 1, 2, "attempt0");
    writer->collect(0, page("x"));
  }
  EXPECT_FALSE(store_->hasMapOutput(3, 0));
  EXPECT_FALSE(store_->hasMapOutput(3, 1));
  EXPECT_TRUE(fs::is_empty(dir_));
}

TEST_F(FileShuffleTest, removeShuffle) {
  writeMap(4, 0, {1});
  writeMap(5, 0, {1});
  store_->removeShuffle(4);
  EXPECT_FALSE(store_->hasMapOutput(4, 0));
  EXPECT_TRUE(store_->hasMapOutput(5, 0));
}

TEST_F(FileShuffleTest, backgroundWorkBlocksUntilDone) {
  auto executor = std::make_shared<folly::CPUThreadPoolExecutor>(1);
  BackgroundWork work(executor);
  auto future = facebook::velox::ContinueFuture::makeEmpty();
  EXPECT_FALSE(work.blocked(&future));

  folly::Baton<> release;
  bool ran = false;
  work.start([&] {
    release.wait();
    ran = true;
  });
  EXPECT_TRUE(work.blocked(&future));
  EXPECT_FALSE(ran);
  release.post();
  std::move(future).via(executor.get()).get();
  EXPECT_FALSE(work.blocked(&future));
  EXPECT_TRUE(ran);

  // A failure comes out where the owner looks, once.
  folly::Baton<> fail;
  work.start([&] {
    fail.wait();
    throw std::runtime_error("failed");
  });
  EXPECT_TRUE(work.blocked(&future));
  fail.post();
  std::move(future).via(executor.get()).get();
  EXPECT_THROW(work.blocked(&future), std::runtime_error);
  EXPECT_FALSE(work.blocked(&future));

  // finish() leaves the failure for the owner.
  EXPECT_TRUE(work.finish());
  work.start([] { throw std::runtime_error("failed"); });
  EXPECT_FALSE(work.finish());
  EXPECT_THROW(work.blocked(&future), std::runtime_error);
}

TEST_F(FileShuffleTest, missingBlockFailsTheFetch) {
  EXPECT_THROW(
      folly::coro::blockingWait(
          transport_->co_fetchBlock("test-executor", {9, 0, 0}, 0, 1)),
      std::exception);
  EXPECT_THROW(
      folly::coro::blockingWait(
          transport_->co_fetchBlock("nowhere", {0, 0, 0}, 0, 1)),
      std::exception);
}

TEST(BlockUrlTest, roundTrip) {
  BlockId id{7, 12, 3};
  auto url = blockUrl("host:4040", id, {{100, 42}});
  EXPECT_EQ(url, "ember-shuffle://host:4040/100/42/7/12/3");
  EXPECT_TRUE(isBlockUrl(url));
  EXPECT_FALSE(isBlockUrl("local://task"));
  auto [location, parsed] = parseBlockUrl(url);
  EXPECT_EQ(location, "host:4040");
  EXPECT_EQ(parsed, id);
  EXPECT_EQ(parsed.toString(), "shuffle_7_12_3");
  EXPECT_EQ(
      blockUrlRanges(url),
      (std::vector<std::pair<int64_t, int64_t>>{{100, 42}}));

  // A fetch failure names the block without its ranges.
  EXPECT_EQ(parseBlockUrl(blockUrl("host:4040", id)).second, id);
  EXPECT_TRUE(blockUrlRanges(blockUrl("host:4040", id)).empty());
}

} // namespace
} // namespace ember::shuffle
