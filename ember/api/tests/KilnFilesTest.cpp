// Datasets in Kiln as files (KilnFiles.h), against an in-process Kiln
// cluster: kiln:// paths, the "kiln" file system, and sorts that read their
// input from a dataset and write their output as a new one.

#include <gtest/gtest.h>

#include <random>

#include <folly/coro/BlockingWait.h>

#include "ember/KilnFiles.h"
#include "ember/Runtime.h"
#include "ember/Session.h"
#include "ember/functions.h"
#include "ember/shuffle/KilnShuffle.h"
#include "kiln/catalog/Catalog.h"
#include "kiln/client/Client.h"
#include "kiln/local/LocalCluster.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace ember {
namespace {

using namespace ember::functions;
using folly::coro::blockingWait;
namespace velox = facebook::velox;

class KilnFilesTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    initializeRuntime();
    cluster_ = new kiln::local::LocalCluster(kiln::local::LocalClusterOptions{
        .dramServers = 1, .localFsServers = 1});
  }

  static void TearDownTestSuite() {
    delete cluster_;
    cluster_ = nullptr;
  }

  void TearDown() override {
    if (ember_.has_value()) {
      ember_->stop();
    }
  }

  static std::string metadata() {
    return cluster_->metadataAddress().toString();
  }

  static kiln::client::Client& client() {
    return *shuffle::kilnClient(metadata());
  }

  // A catalog that keeps manifests in DRAM, as the parts written.
  static kiln::catalog::Catalog catalog() {
    return kiln::catalog::Catalog(
        client(),
        kiln::catalog::CatalogOptions{
            .placement = kiln::Placement{.tier = kiln::Tier::kDram}});
  }

  Session& session(std::map<std::string, std::string> config = {}) {
    auto builder = Session::builder();
    builder.appName("KilnFilesTest").master("local[4]");
    builder.config(std::string(kKilnFilesMetadata), metadata());
    for (const auto& [key, value] : config) {
      builder.config(key, value);
    }
    ember_ = builder.getOrCreate();
    return *ember_;
  }

  static kiln::ChunkId putChunk(const std::string& bytes) {
    auto writer = blockingWait(client().co_create(kiln::kDefaultBucket));
    auto buffer = client().allocateBuffer(bytes.size());
    std::memcpy(buffer.data(), bytes.data(), bytes.size());
    buffer.setSize(bytes.size());
    blockingWait(writer->co_append(std::move(buffer)));
    return blockingWait(writer->co_commit());
  }

  // Publishes dataset 'name' with one file per entry of 'files'.
  static void publish(
      const std::string& name,
      const std::vector<std::pair<std::string, std::string>>& files) {
    kiln::catalog::Manifest manifest;
    for (const auto& [file, bytes] : files) {
      manifest.chunks.push_back(kiln::catalog::ChunkRange{
          .id = putChunk(bytes), .length = bytes.size(), .name = file});
    }
    ASSERT_TRUE(
        blockingWait(catalog().co_publishDataset(name, std::move(manifest)))
            .has_value());
  }

  // Publishes dataset 'name' with 'numFiles' files of 'recordsPerFile'
  // random gensort records; returns the records, sorted.
  static std::vector<std::string> publishRecords(
      const std::string& name,
      int32_t numFiles,
      int32_t recordsPerFile) {
    std::mt19937 random(42);
    std::vector<std::string> records;
    std::vector<std::pair<std::string, std::string>> files;
    for (int32_t f = 0; f < numFiles; ++f) {
      std::string bytes;
      for (int32_t r = 0; r < recordsPerFile; ++r) {
        std::string record(100, '\0');
        for (auto& c : record) {
          c = static_cast<char>(random());
        }
        bytes += record;
        records.push_back(std::move(record));
      }
      files.emplace_back(fmt::format("in-{}", f), std::move(bytes));
    }
    publish(name, files);
    std::sort(records.begin(), records.end());
    return records;
  }

  // The dataset's file names, and its records in manifest order.
  static std::pair<std::vector<std::string>, std::vector<std::string>>
  readDataset(const std::string& name) {
    auto dataset = blockingWait(catalog().co_getDataset(name));
    EXPECT_TRUE(dataset.has_value());
    std::vector<std::string> names;
    std::vector<std::string> records;
    auto fileSystem = velox::filesystems::getFileSystem(kilnPath(metadata(), name), nullptr);
    for (const auto& entry : dataset->manifest.chunks) {
      names.push_back(entry.name);
      auto file =
          fileSystem->openFileForRead(kilnPath(metadata(), name, entry.name));
      const auto bytes = file->pread(0, file->size());
      for (size_t i = 0; i < bytes.size(); i += 100) {
        records.push_back(bytes.substr(i, 100));
      }
    }
    return {names, records};
  }

  static inline kiln::local::LocalCluster* cluster_{nullptr};
  std::optional<Session> ember_;
};

TEST_F(KilnFilesTest, paths) {
  EXPECT_TRUE(isKilnPath("kiln://meta:7470/data/ds/part-00000"));
  EXPECT_TRUE(isKilnPath("kiln:///data/ds/"));
  EXPECT_TRUE(isKilnPath("kiln://meta:7470/data"));
  // Shuffle locations and other schemes are not.
  EXPECT_FALSE(isKilnPath("kiln://meta:7470/0123abcd"));
  EXPECT_FALSE(isKilnPath("kiln://meta:7470/database"));
  EXPECT_FALSE(isKilnPath("/data/ds"));

  const auto metadata = "tcp://meta:7470,rdma://10.0.0.1:7471";
  const auto path = kilnPath(metadata, "ds", "part-00000");
  EXPECT_EQ(path, "kiln://tcp:%2F%2Fmeta:7470,rdma:%2F%2F10.0.0.1:7471/data/ds/part-00000");
  const auto parsed = parseKilnPath(path);
  EXPECT_EQ(parsed.metadata, metadata);
  EXPECT_EQ(parsed.dataset, "ds");
  EXPECT_EQ(parsed.file, "part-00000");

  EXPECT_EQ(kilnPath("m", "ds"), "kiln://m/data/ds/");
  EXPECT_EQ(parseKilnPath("kiln://m/data/ds/").file, "");
  EXPECT_EQ(parseKilnPath("kiln://m/data/ds").dataset, "ds");
  EXPECT_EQ(parseKilnPath("kiln://m/data/").dataset, "");
  EXPECT_THROW(parseKilnPath("kiln://m/abc"), velox::VeloxUserError);

  EXPECT_EQ(
      resolveKilnPath("kiln:///data/ds/f", {{"ember.kiln.metadata", "m:1"}}),
      "kiln://m:1/data/ds/f");
  EXPECT_EQ(
      resolveKilnPath(
          "kiln:///data/ds/", {{"ember.shuffle.kiln.metadata", "m:2"}}),
      "kiln://m:2/data/ds/");
  EXPECT_EQ(resolveKilnPath("kiln://m/data/ds/", {}), "kiln://m/data/ds/");
  EXPECT_EQ(resolveKilnPath("/local/dir", {}), "/local/dir");
  EXPECT_THROW(resolveKilnPath("kiln:///data/ds/", {}), velox::VeloxUserError);
}

TEST_F(KilnFilesTest, fileSystem) {
  publish("fs", {{"a", "hello"}, {"b", "kiln files"}});
  const auto directory = kilnPath(metadata(), "fs");
  auto fileSystem = velox::filesystems::getFileSystem(directory, nullptr);

  EXPECT_EQ(
      fileSystem->list(directory),
      (std::vector<std::string>{
          kilnPath(metadata(), "fs", "a"), kilnPath(metadata(), "fs", "b")}));
  const auto datasets = fileSystem->list(kilnPath(metadata(), ""));
  EXPECT_NE(
      std::find(datasets.begin(), datasets.end(), directory), datasets.end());

  EXPECT_TRUE(fileSystem->isDirectory(directory));
  EXPECT_FALSE(fileSystem->isDirectory(kilnPath(metadata(), "fs", "a")));
  EXPECT_FALSE(fileSystem->isDirectory(kilnPath(metadata(), "none")));
  EXPECT_TRUE(fileSystem->exists(kilnPath(metadata(), "fs", "b")));
  EXPECT_FALSE(fileSystem->exists(kilnPath(metadata(), "fs", "c")));
  EXPECT_FALSE(fileSystem->exists(kilnPath(metadata(), "none")));

  auto file = fileSystem->openFileForRead(kilnPath(metadata(), "fs", "b"));
  EXPECT_EQ(file->size(), 10);
  EXPECT_EQ(file->pread(0, 10), "kiln files");
  EXPECT_EQ(file->pread(5, 5), "files");
  EXPECT_ANY_THROW(file->pread(5, 6));
  EXPECT_THROW(
      fileSystem->openFileForRead(kilnPath(metadata(), "fs", "c")),
      velox::VeloxUserError);
  EXPECT_THROW(
      fileSystem->openFileForWrite(kilnPath(metadata(), "fs", "c")),
      velox::VeloxUserError);
}

TEST_F(KilnFilesTest, sortsDatasetIntoDataset) {
  auto expected = publishRecords("sort-in", 4, 2000);
  auto records = session({{"ember.sort.range_partitions", "6"}})
                     .read()
                     .gensort("kiln:///data/sort-in/")
                     .orderBy(col("key"))
                     .write()
                     .gensort("kiln:///data/sort-out");
  EXPECT_EQ(records, 8000);

  auto dataset = blockingWait(catalog().co_getDataset("sort-out"));
  ASSERT_TRUE(dataset.has_value());
  EXPECT_EQ(dataset->manifest.rows, 8000);
  EXPECT_EQ(dataset->manifest.bytes, 800000);
  EXPECT_EQ(dataset->manifest.schema, "gensort");
  EXPECT_NE(dataset->manifest.bucket, kiln::kDefaultBucket);
  uint64_t rows = 0;
  for (const auto& entry : dataset->manifest.chunks) {
    EXPECT_GT(*entry.rows, 0);
    rows += *entry.rows;
  }
  EXPECT_EQ(rows, 8000);

  auto [names, sorted] = readDataset("sort-out");
  EXPECT_FALSE(names.empty());
  EXPECT_EQ(names.front(), "part-00000");
  EXPECT_TRUE(std::is_sorted(names.begin(), names.end()));
  EXPECT_EQ(sorted, expected);

  // The output reads back as input, and is not written over.
  EXPECT_EQ(session().read().gensort("kiln:///data/sort-out/").count(), 8000);
  EXPECT_THROW(
      session()
          .read()
          .gensort("kiln:///data/sort-in/")
          .write()
          .gensort("kiln:///data/sort-out"),
      velox::VeloxUserError);

  // Deleting the dataset deletes the parts with their bucket.
  EXPECT_TRUE(blockingWait(catalog().co_deleteDataset("sort-out")));
  EXPECT_ANY_THROW(blockingWait(client().co_stat(dataset->manifest.chunks[0].id)));
}

TEST_F(KilnFilesTest, writesToLocalFs) {
  auto expected = publishRecords("tier-in", 2, 500);
  session({{std::string(kKilnFilesTier), "localfs"}})
      .read()
      .gensort("kiln:///data/tier-in/")
      .orderBy(col("key"))
      .write()
      .gensort("kiln:///data/tier-out");
  auto dataset = blockingWait(catalog().co_getDataset("tier-out"));
  ASSERT_TRUE(dataset.has_value());
  for (const auto& entry : dataset->manifest.chunks) {
    const auto info = blockingWait(client().co_stat(entry.id));
    EXPECT_EQ(info.placement.tier, kiln::Tier::kLocalFs);
  }
  EXPECT_EQ(readDataset("tier-out").second, expected);
}

TEST_F(KilnFilesTest, failedJobPublishesNothing) {
  // Input files that are not whole gensort records fail the job.
  publish("bad-in", {{"a", std::string(150, 'x')}});
  EXPECT_ANY_THROW(session()
                       .read()
                       .gensort("kiln:///data/bad-in/")
                       .write()
                       .gensort("kiln:///data/bad-out"));
  EXPECT_FALSE(
      blockingWait(catalog().co_getDataset("bad-out")).has_value());
}

} // namespace
} // namespace ember
