// ai::generate through the Dataframe API, against a mock OpenAI-compatible
// server.

#include <cstdlib>
#include <optional>

#include <gtest/gtest.h>

#include "ember/Session.h"
#include "ember/ai.h"
#include "ember/ai/tests/MockChatServer.h"
#include "ember/functions.h"

namespace ember {
namespace {

using namespace ember::functions;
using ai::test::MockChatServer;
using facebook::velox::ROW;
using facebook::velox::TypeKind;
using facebook::velox::Variant;
using facebook::velox::VARCHAR;

constexpr const char* kKeyVariable = "EMBER_AI_GENERATE_TEST_KEY";

// "fail" gets a 400, "busy" a 429 the first time, "yes?" the answer "yes";
// anything else is echoed.
MockChatServer::Reply reply(const std::string& prompt, int previous) {
  if (prompt == "fail") {
    return {400, R"({"error": "bad prompt"})"};
  }
  if (prompt == "busy" && previous == 0) {
    return {429, R"({"error": "slow down"})"};
  }
  if (prompt == "yes?") {
    return {200, "yes"};
  }
  return {200, "echo: " + prompt};
}

class AiGenerateTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    ::setenv(kKeyVariable, "secret", 1);
    server_ = std::make_unique<MockChatServer>(reply);
    ember_ = Session::builder()
                 .appName("AiGenerateTest")
                 .master("local[2]")
                 .config("ember.ai.endpoint.mock.url", server_->url())
                 .config("ember.ai.endpoint.mock.model", "test-model")
                 .config("ember.ai.endpoint.mock.api_key_env", kKeyVariable)
                 .getOrCreate();
  }

  static void TearDownTestCase() {
    ember_->stop();
    ember_.reset();
    server_.reset();
  }

  static Dataframe prompts(const std::vector<Variant>& values) {
    std::vector<std::vector<Variant>> rows;
    for (const auto& value : values) {
      rows.push_back({value});
    }
    return ember_->createDataframe(ROW({"p"}, {VARCHAR()}), std::move(rows));
  }

  // p, the answer and the status, ordered by p.
  static std::vector<Row> generate(const std::vector<Variant>& values) {
    return prompts(values)
        .withColumn("g", ai::generate(col("p"), "mock"))
        .select(
            col("p"),
            col("g").getField("result").as("result"),
            col("g").getField("status").as("status"))
        .orderBy("p")
        .collect();
  }

  static inline std::unique_ptr<MockChatServer> server_;
  static inline std::optional<Session> ember_;
};

TEST_F(AiGenerateTest, answersEachRow) {
  auto rows = generate({"a", "b"});
  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0].getString(1), "echo: a");
  EXPECT_EQ(rows[0].getString(2), "");
  EXPECT_EQ(rows[1].getString(1), "echo: b");
  EXPECT_EQ(server_->lastModel(), "test-model");
  EXPECT_EQ(server_->lastAuthorization(), "Bearer secret");
}

TEST_F(AiGenerateTest, escapesPrompts) {
  const std::string prompt = "say \"hi\"\n\tback\\slash é";
  auto rows = generate({prompt});
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].getString(1), "echo: " + prompt);
}

TEST_F(AiGenerateTest, filtersOnAnswers) {
  auto rows = prompts({"yes?", "no?"})
                  .filter(ai::generate(col("p"), "mock").getField("result") ==
                          "yes")
                  .collect();
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].getString(0), "yes?");
}

TEST_F(AiGenerateTest, nullPromptGivesNullRow) {
  auto rows = generate({Variant::null(TypeKind::VARCHAR), "c"});
  ASSERT_EQ(rows.size(), 2);
  EXPECT_TRUE(rows[0].isNullAt(0));
  EXPECT_TRUE(rows[0].isNullAt(1));
  EXPECT_TRUE(rows[0].isNullAt(2));
  EXPECT_EQ(rows[1].getString(1), "echo: c");
}

TEST_F(AiGenerateTest, retriesAfter429) {
  auto rows = generate({"busy"});
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].getString(1), "echo: busy");
  EXPECT_EQ(rows[0].getString(2), "");
  EXPECT_EQ(server_->requests("busy"), 2);
}

TEST_F(AiGenerateTest, failedRowDoesNotFailTask) {
  auto rows = generate({"fail", "fine"});
  ASSERT_EQ(rows.size(), 2);
  EXPECT_TRUE(rows[0].isNullAt(1));
  EXPECT_EQ(rows[0].getString(2), R"(HTTP 400: {"error": "bad prompt"})");
  EXPECT_EQ(rows[1].getString(1), "echo: fine");
  // Not retried.
  EXPECT_EQ(server_->requests("fail"), 1);
}

TEST_F(AiGenerateTest, distinctPromptsAreSentOnce) {
  auto rows = generate({"same", "same", "same"});
  ASSERT_EQ(rows.size(), 3);
  for (const auto& row : rows) {
    EXPECT_EQ(row.getString(1), "echo: same");
  }
  EXPECT_EQ(server_->requests("same"), 1);
}

TEST_F(AiGenerateTest, unknownEndpointFails) {
  try {
    prompts({"a"})
        .select(ai::generate(col("p"), "nope").getField("result"))
        .collect();
    FAIL() << "expected an error";
  } catch (const std::exception& e) {
    EXPECT_NE(
        std::string(e.what()).find("Unknown AI endpoint 'nope'"),
        std::string::npos)
        << e.what();
  }
}

} // namespace
} // namespace ember
