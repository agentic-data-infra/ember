// dataframe.write(): result tasks write part files, which hold the sorted
// records in name order, and sorts spill when a task's memory runs out.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <random>

#include "ember/Session.h"
#include "ember/functions.h"
#include "velox/exec/SpillStats.h"

namespace ember {
namespace {

using namespace ember::functions;
namespace fs = std::filesystem;

class WriteTest : public testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_write_test_{}", ::getpid());
    fs::remove_all(dir_);
    fs::create_directories(input());
  }

  void TearDown() override {
    if (ember_.has_value()) {
      ember_->stop();
    }
    fs::remove_all(dir_);
  }

  Session& session(std::map<std::string, std::string> config = {}) {
    auto builder = Session::builder();
    builder.appName("WriteTest").master("local[4]");
    for (const auto& [key, value] : config) {
      builder.config(key, value);
    }
    ember_ = builder.getOrCreate();
    return *ember_;
  }

  fs::path input() const {
    return dir_ / "input";
  }

  fs::path output() const {
    return dir_ / "output";
  }

  // Writes 'numFiles' files of 'recordsPerFile' random records; returns the
  // records, sorted.
  std::vector<std::string> writeInput(
      int32_t numFiles,
      int32_t recordsPerFile) {
    std::mt19937 random(42);
    std::vector<std::string> records;
    for (int32_t f = 0; f < numFiles; ++f) {
      std::ofstream out(input() / fmt::format("in-{}", f), std::ios::binary);
      for (int32_t r = 0; r < recordsPerFile; ++r) {
        std::string record(100, '\0');
        for (auto& c : record) {
          c = static_cast<char>(random());
        }
        out.write(record.data(), record.size());
        records.push_back(std::move(record));
      }
    }
    std::sort(records.begin(), records.end());
    return records;
  }

  // The output directory's file names, and its records in name order.
  std::pair<std::vector<std::string>, std::vector<std::string>> readOutput()
      const {
    std::vector<std::string> names;
    for (const auto& entry : fs::directory_iterator(output())) {
      names.push_back(entry.path().filename().string());
    }
    std::sort(names.begin(), names.end());
    std::vector<std::string> records;
    for (const auto& name : names) {
      std::ifstream in(output() / name, std::ios::binary);
      std::string record(100, '\0');
      while (in.read(record.data(), record.size())) {
        records.push_back(record);
      }
    }
    return {names, records};
  }

  std::optional<Session> ember_;
  fs::path dir_;
};

TEST_F(WriteTest, writesOnePartFile) {
  auto expected = writeInput(3, 1000);
  auto records = session()
                     .read()
                     .gensort(input().string())
                     .orderBy(col("key"))
                     .write()
                     .gensort(output().string());
  EXPECT_EQ(records, 3000);
  auto [names, rows] = readOutput();
  EXPECT_EQ(names, std::vector<std::string>{"part-00000"});
  EXPECT_EQ(rows, expected);
}

TEST_F(WriteTest, writesRangePartitionsInOrder) {
  auto expected = writeInput(4, 2000);
  auto records = session({{"ember.sort.range_partitions", "6"}})
                     .read()
                     .gensort(input().string())
                     .orderBy(col("key"))
                     .write()
                     .gensort(output().string());
  EXPECT_EQ(records, 8000);
  auto [names, rows] = readOutput();
  EXPECT_EQ(
      names,
      (std::vector<std::string>{
          "part-00000",
          "part-00001",
          "part-00002",
          "part-00003",
          "part-00004",
          "part-00005"}));
  EXPECT_EQ(rows, expected);
}

TEST_F(WriteTest, spillsSortOverMemoryLimit) {
  // Two map tasks sort 50MB each in 32MB of memory.
  auto expected = writeInput(2, 500'000);
  const auto spilledBefore =
      facebook::velox::exec::globalSpillStats().spilledBytes.load();
  auto records = session({{"ember.default.parallelism", "2"},
                          {"ember.sort.range_partitions", "2"},
                          {"spill_enabled", "true"},
                          {"query_max_memory_per_node", "32MB"},
                          {"ember.shuffle.spill_threshold_bytes", "1048576"}})
                     .read()
                     .gensort(input().string())
                     .orderBy(col("key"))
                     .write()
                     .gensort(output().string());
  EXPECT_EQ(records, 1'000'000);
  EXPECT_GT(
      facebook::velox::exec::globalSpillStats().spilledBytes.load(),
      spilledBefore);
  EXPECT_EQ(readOutput().second, expected);
}

TEST_F(WriteTest, rejectsDirectoryWithPartFiles) {
  writeInput(1, 10);
  fs::create_directories(output());
  std::ofstream(output() / "part-00000") << "old";
  try {
    (void)session()
        .read()
        .gensort(input().string())
        .write()
        .gensort(output().string());
    FAIL() << "Expected an error";
  } catch (const std::exception& e) {
    EXPECT_NE(
        std::string(e.what()).find("holds part files already"),
        std::string::npos)
        << e.what();
  }
}

} // namespace
} // namespace ember
