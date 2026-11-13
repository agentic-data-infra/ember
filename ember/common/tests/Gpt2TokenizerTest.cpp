#include "ember/Gpt2Tokenizer.h"

#include <cstdlib>

#include <folly/FileUtil.h>
#include <folly/json.h>
#include <gtest/gtest.h>

#include "ember/Runtime.h"
#include "velox/common/memory/Memory.h"

namespace ember {
namespace {

namespace velox = facebook::velox;

// Hugging Face's GPT-2 files and the ids its tokenizer gives
// (data/gen_gpt2_ids.py), in EMBER_TEST_DATA.
std::string dataDir() {
  const char* dir = std::getenv("EMBER_TEST_DATA");
  return dir == nullptr ? "ember/common/tests/data" : dir;
}

class Gpt2TokenizerTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    initializeRuntime();
    pool_ = velox::memory::memoryManager()->addLeafPool("Gpt2TokenizerTest");
    tokenizer_ = Gpt2Tokenizer::load(dataDir() + "/gpt2", pool_);
  }

  static void TearDownTestSuite() {
    tokenizer_.reset();
    pool_.reset();
  }

  static std::vector<int32_t> encode(std::string_view text) {
    std::vector<int32_t> ids;
    tokenizer_->encode(text, ids);
    return ids;
  }

  static inline std::shared_ptr<velox::memory::MemoryPool> pool_;
  static inline std::shared_ptr<const Gpt2Tokenizer> tokenizer_;
};

TEST_F(Gpt2TokenizerTest, givesTheIdsOfHuggingFace) {
  std::string json;
  ASSERT_TRUE(folly::readFile((dataDir() + "/gpt2_ids.json").c_str(), json));
  const auto cases = folly::parseJson(json);
  ASSERT_GT(cases.size(), 50);
  for (const auto& expected : cases) {
    std::vector<int32_t> ids;
    for (const auto& id : expected["ids"]) {
      ids.push_back(static_cast<int32_t>(id.asInt()));
    }
    EXPECT_EQ(encode(expected["text"].asString()), ids)
        << expected["text"].asString();
  }
}

TEST_F(Gpt2TokenizerTest, appendsToTheIds) {
  std::vector<int32_t> ids{7};
  tokenizer_->encode("Hello world", ids);
  EXPECT_EQ(ids, (std::vector<int32_t>{7, 15496, 995}));
}

TEST_F(Gpt2TokenizerTest, tokenizesBytesThatAreNotUtf8) {
  // 0xff on its own is the byte's token, as is a cut-off character.
  EXPECT_EQ(encode("a\xff"), (std::vector<int32_t>{64, 187}));
  EXPECT_EQ(encode("\xc3"), (std::vector<int32_t>{127}));
}

TEST_F(Gpt2TokenizerTest, failsWithoutTheFiles) {
  EXPECT_THROW(
      Gpt2Tokenizer::load(dataDir() + "/missing", pool_),
      velox::VeloxUserError);
}

} // namespace
} // namespace ember
