// The file shuffle's on-disk format: commit, spill and merge, first commit
// wins, block reads, and block URLs.

#include <gtest/gtest.h>

#include <filesystem>

#include "ember/shuffle/FileShuffle.h"

namespace ember::shuffle {
namespace {

namespace fs = std::filesystem;

class FileShuffleTest : public testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_file_shuffle_{}", ::getpid());
    fs::remove_all(dir_);
    store_ = std::make_shared<ShuffleBlockStore>("test-executor", dir_.string());
    ShuffleBlockStore::registerStore(store_);
  }

  void TearDown() override {
    ShuffleBlockStore::unregisterStore("test-executor");
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
    return pages(BlockTransport::instance()
                     ->fetchBlock("test-executor", {shuffleId, mapId, reduceId})
                     .get());
  }

  // Writes pages "<p>-<i>" for i < count[p] in round-robin order over the
  // partitions.
  MapStatus writeMap(
      int32_t shuffleId,
      int32_t mapId,
      const std::vector<int32_t>& counts,
      int64_t spillThreshold,
      int64_t* spills = nullptr) {
    auto writer = store_->createWriter(
        shuffleId, mapId, counts.size(), "attempt0", spillThreshold);
    const auto max = *std::max_element(counts.begin(), counts.end());
    for (int32_t i = 0; i < max; ++i) {
      for (int32_t p = 0; p < counts.size(); ++p) {
        if (i < counts[p]) {
          writer->collect(p, page(fmt::format("{}-{}", p, i)));
        }
      }
    }
    auto status = writer->commit();
    if (spills != nullptr) {
      *spills = writer->spillCount();
    }
    return status;
  }

  fs::path dir_;
  std::shared_ptr<ShuffleBlockStore> store_;
};

TEST_F(FileShuffleTest, partitionsAreContiguousAndInOrder) {
  auto status = writeMap(0, 0, {3, 0, 2}, ShuffleBlockStore::kDefaultSpillThreshold);
  EXPECT_EQ(status.location, "test-executor");
  ASSERT_EQ(status.partitionLengths.size(), 3);
  // Each frame is a 4-byte length plus "p-i".
  EXPECT_EQ(status.partitionLengths, (std::vector<int64_t>{21, 0, 14}));
  EXPECT_EQ(readBlock(0, 0, 0), (std::vector<std::string>{"0-0", "0-1", "0-2"}));
  EXPECT_TRUE(readBlock(0, 0, 1).empty());
  EXPECT_EQ(readBlock(0, 0, 2), (std::vector<std::string>{"2-0", "2-1"}));
  EXPECT_EQ(fs::file_size(store_->indexFile(0, 0)), 4 * sizeof(int64_t));
}

TEST_F(FileShuffleTest, spilledRunsMergeInWriteOrder) {
  int64_t spills = 0;
  // A tiny threshold spills after every page.
  auto status = writeMap(1, 0, {5, 4}, /*spillThreshold=*/1, &spills);
  EXPECT_EQ(spills, 9);
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

TEST_F(FileShuffleTest, firstCommitWins) {
  auto first = store_->createWriter(2, 0, 1, "attempt0");
  auto second = store_->createWriter(2, 0, 1, "attempt1");
  first->collect(0, page("first"));
  second->collect(0, page("second, longer"));
  second->commit();
  auto status = first->commit();
  EXPECT_EQ(readBlock(2, 0, 0), (std::vector<std::string>{"second, longer"}));
  EXPECT_EQ(status.partitionLengths[0], 4 + 14);
}

TEST_F(FileShuffleTest, abortLeavesNoOutput) {
  {
    auto writer = store_->createWriter(3, 0, 2, "attempt0", 1);
    writer->collect(0, page("x"));
    writer->collect(1, page("y"));
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
  writeMap(4, 0, {1}, ShuffleBlockStore::kDefaultSpillThreshold);
  writeMap(5, 0, {1}, ShuffleBlockStore::kDefaultSpillThreshold);
  store_->removeShuffle(4);
  EXPECT_FALSE(store_->hasMapOutput(4, 0));
  EXPECT_TRUE(store_->hasMapOutput(5, 0));
}

TEST_F(FileShuffleTest, missingBlockFailsTheFetch) {
  EXPECT_THROW(
      BlockTransport::instance()->fetchBlock("test-executor", {9, 0, 0}).get(),
      std::exception);
  EXPECT_THROW(
      BlockTransport::instance()->fetchBlock("nowhere", {0, 0, 0}).get(),
      std::exception);
}

TEST(BlockUrlTest, roundTrip) {
  BlockId id{7, 12, 3};
  auto url = blockUrl("host:4040", id);
  EXPECT_EQ(url, "ember-shuffle://host:4040/7/12/3");
  EXPECT_TRUE(isBlockUrl(url));
  EXPECT_FALSE(isBlockUrl("local://task"));
  auto [location, parsed] = parseBlockUrl(url);
  EXPECT_EQ(location, "host:4040");
  EXPECT_EQ(parsed, id);
  EXPECT_EQ(parsed.toString(), "shuffle_7_12_3");
}

} // namespace
} // namespace ember::shuffle
