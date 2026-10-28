// Reading gensort files: 100-byte records split across tasks on record
// boundaries, sorted by their binary key.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <random>

#include "ember/Session.h"
#include "ember/functions.h"
#include "velox/vector/FlatVector.h"

namespace ember {
namespace {

using namespace ember::functions;
namespace velox = facebook::velox;
namespace fs = std::filesystem;

class GensortTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    // Ranges of 1234 bytes start and end inside records.
    ember_ = Session::builder()
                 .appName("GensortTest")
                 .master("local[4]")
                 .config("ember.files.max_split_bytes", "1234")
                 .getOrCreate();
  }

  static void TearDownTestCase() {
    ember_->stop();
    ember_.reset();
  }

  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_gensort_test_{}", ::getpid());
    fs::remove_all(dir_);
    fs::create_directories(dir_);
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  // Writes 'numFiles' files of 'recordsPerFile' records with random keys and
  // payloads; returns the records.
  std::vector<std::string> writeRecords(
      int32_t numFiles,
      int32_t recordsPerFile) {
    std::mt19937 random(42);
    std::vector<std::string> records;
    for (int32_t f = 0; f < numFiles; ++f) {
      std::ofstream out(dir_ / fmt::format("part-{}", f), std::ios::binary);
      for (int32_t r = 0; r < recordsPerFile; ++r) {
        std::string record(100, '\0');
        for (auto& c : record) {
          c = static_cast<char>(random());
        }
        out.write(record.data(), record.size());
        records.push_back(std::move(record));
      }
    }
    return records;
  }

  std::string dir() const {
    return dir_.string() + "/";
  }

  // The rows of 'df', whose columns are binary, each row's columns
  // concatenated.
  static std::vector<std::string> collectBinary(const Dataframe& df) {
    std::vector<std::string> rows;
    for (const auto& batch : df.collectBatches()) {
      for (velox::vector_size_t i = 0; i < batch->size(); ++i) {
        std::string row;
        for (const auto& child : batch->children()) {
          row += std::string_view(
              child->asFlatVector<velox::StringView>()->valueAt(i));
        }
        rows.push_back(std::move(row));
      }
    }
    return rows;
  }

  static inline std::optional<Session> ember_;
  fs::path dir_;
};

TEST_F(GensortTest, schema) {
  writeRecords(1, 1);
  auto df = ember_->read().gensort(dir());
  EXPECT_EQ(
      df.schema()->toString(), "ROW<key:VARBINARY,payload:VARBINARY>");
}

TEST_F(GensortTest, readsEveryRecordOnce) {
  auto expected = writeRecords(3, 1000);
  auto df = ember_->read().gensort(dir());
  EXPECT_EQ(df.count(), 3000);

  auto rows = collectBinary(df);
  std::sort(rows.begin(), rows.end());
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(rows, expected);
}

TEST_F(GensortTest, sortsByUnsignedKeyBytes) {
  auto expected = writeRecords(2, 500);
  auto rows =
      collectBinary(ember_->read().gensort(dir()).orderBy(col("key")));
  // std::string compares chars as unsigned, as memcmp does.
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(rows, expected);
}

TEST_F(GensortTest, readsOneColumn) {
  auto records = writeRecords(1, 100);
  auto payloads =
      collectBinary(ember_->read().gensort(dir()).select(col("payload")));
  std::vector<std::string> expected;
  for (const auto& record : records) {
    expected.push_back(record.substr(10));
  }
  std::sort(payloads.begin(), payloads.end());
  std::sort(expected.begin(), expected.end());
  EXPECT_EQ(payloads, expected);
}

TEST_F(GensortTest, rejectsPartialRecords) {
  std::ofstream(dir_ / "bad", std::ios::binary) << std::string(150, 'x');
  try {
    (void)ember_->read().gensort(dir()).count();
    FAIL() << "Expected an error";
  } catch (const std::exception& e) {
    EXPECT_NE(
        std::string(e.what()).find("Not a gensort file"), std::string::npos)
        << e.what();
  }
}

} // namespace
} // namespace ember
