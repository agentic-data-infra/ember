// stoke_tokenize and stoke_pack (Stoke.h), in builds with stoke: text to
// training batches in stoke's format.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>

#include "ember/Session.h"
#include "ember/functions.h"
#include "stoke/format/Batch.h"
#include "velox/vector/FlatVector.h"

namespace ember {
namespace {

using namespace ember::functions;
namespace fs = std::filesystem;
namespace velox = facebook::velox;

constexpr int64_t kBatch = 2;
constexpr int64_t kSequence = 7;

class StokeFunctionsTest : public testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("ember_stoke_functions_test_{}", ::getpid());
    fs::remove_all(dir_);
    fs::create_directories(dir_);
    ember_ = Session::builder()
                 .appName("StokeFunctionsTest")
                 .master("local[2]")
                 .getOrCreate();
  }

  void TearDown() override {
    ember_->stop();
    fs::remove_all(dir_);
  }

  // A text file of 'lines'.
  std::string writeText(const std::vector<std::string>& lines) {
    std::ofstream out(dir_ / "in");
    for (const auto& line : lines) {
      out << line << "\n";
    }
    return dir_.string();
  }

  // The values of the one column of 'df', in its order.
  static std::vector<std::optional<std::string>> values(const Dataframe& df) {
    std::vector<std::optional<std::string>> result;
    for (const auto& batch : df.collectBatches()) {
      auto* column = batch->childAt(0)->asFlatVector<velox::StringView>();
      for (velox::vector_size_t i = 0; i < batch->size(); ++i) {
        if (column->isNullAt(i)) {
          result.emplace_back(std::nullopt);
        } else {
          result.emplace_back(std::string(column->valueAt(i)));
        }
      }
    }
    return result;
  }

  fs::path dir_;
  std::optional<Session> ember_;
};

TEST_F(StokeFunctionsTest, tokenizesBytesAndEndOfText) {
  auto rows = ember_->read()
                  .text(writeText({"ab"}))
                  .select(call("stoke_tokenize", col("value")).alias("tokens"))
                  .collect();
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].toString(), "[[97,98,256]]");
}

TEST_F(StokeFunctionsTest, packsWholeWindowsIntoABatch) {
  // 17 bytes + end of text = 18 tokens: two windows of T + 1 = 8, the last
  // two tokens dropped.
  const std::string text = "abcdefghijklmnopq";
  auto batches = values(ember_->read().text(writeText({text})).select(
      call(
          "stoke_pack",
          call("stoke_tokenize", col("value")),
          lit(kBatch),
          lit(kSequence))
          .alias("batch")));
  ASSERT_EQ(batches.size(), 1);
  ASSERT_TRUE(batches[0].has_value());
  const auto& bytes = *batches[0];
  auto batch = stoke::format::parseBatch(
      {reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()});
  EXPECT_EQ(batch.header.dtype, stoke::format::DType::kInt32);
  EXPECT_EQ(batch.header.shape, (std::vector<uint64_t>{2, 8}));
  std::vector<int32_t> tokens(16);
  ASSERT_EQ(batch.payload.size(), tokens.size() * sizeof(int32_t));
  std::memcpy(tokens.data(), batch.payload.data(), batch.payload.size());
  for (size_t i = 0; i < tokens.size(); ++i) {
    EXPECT_EQ(tokens[i], static_cast<unsigned char>(text[i]));
  }
}

TEST_F(StokeFunctionsTest, packsAtMostBRows) {
  // 40 tokens hold five windows of 8; B = 2 keeps two.
  auto batches = values(
      ember_->read().text(writeText({std::string(39, 'x')})).select(
          call(
              "stoke_pack",
              call("stoke_tokenize", col("value")),
              lit(kBatch),
              lit(kSequence))
              .alias("batch")));
  ASSERT_EQ(batches.size(), 1);
  const auto& bytes = *batches[0];
  auto batch = stoke::format::parseBatch(
      {reinterpret_cast<const uint8_t*>(bytes.data()), bytes.size()});
  EXPECT_EQ(batch.header.shape, (std::vector<uint64_t>{2, 8}));
}

TEST_F(StokeFunctionsTest, packsNullWithoutAWholeWindow) {
  // "xy" + end of text: 3 tokens, no window of 8.
  auto batches = values(ember_->read().text(writeText({"xy"})).select(
      call(
          "stoke_pack",
          call("stoke_tokenize", col("value")),
          lit(kBatch),
          lit(kSequence))
          .alias("batch")));
  ASSERT_EQ(batches.size(), 1);
  EXPECT_FALSE(batches[0].has_value());
}

TEST_F(StokeFunctionsTest, rejectsNonPositiveSizes) {
  auto df = ember_->read().text(writeText({"abcdefgh"})).select(call(
      "stoke_pack", call("stoke_tokenize", col("value")), lit(0), lit(7)));
  EXPECT_ANY_THROW(df.collect());
}

} // namespace
} // namespace ember
