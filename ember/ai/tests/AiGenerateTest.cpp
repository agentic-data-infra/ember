// ai::generate through the Dataframe API, against a mock OpenAI-compatible
// server.

#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <optional>

#include <fmt/format.h>
#include <folly/json.h>
#include <gtest/gtest.h>

#include "ember/Session.h"
#include "ember/ai.h"
#include "ember/ai/tests/MockChatServer.h"
#include "ember/functions.h"

namespace ember {
namespace {

namespace fs = std::filesystem;

using namespace ember::functions;
using ai::test::MockChatServer;
using facebook::velox::ARRAY;
using facebook::velox::BIGINT;
using facebook::velox::BOOLEAN;
using facebook::velox::DOUBLE;
using facebook::velox::ROW;
using facebook::velox::TypeKind;
using facebook::velox::Variant;
using facebook::velox::VARCHAR;

constexpr const char* kKeyVariable = "EMBER_AI_GENERATE_TEST_KEY";

// The prompts of the log triage example and their logs' answers.
constexpr std::string_view kRelevant =
    "Is this an error or warning, not routine INFO or cron? ";
constexpr std::string_view kTriage =
    "Domain (one of Database, Auth / TLS, Rate Limit, Compute, Streaming) and "
    "customer-facing outage blast radius 1-10: ";
constexpr std::string_view kRootCause =
    "Plain text, <=14 words, root cause + fix: ";
constexpr std::string_view kSummary = "Plain text, <=10 words, triage summary: ";

std::optional<MockChatServer::Reply> triage(const std::string& prompt) {
  auto log = [&](std::string_view prefix) -> std::optional<std::string> {
    if (prompt.starts_with(prefix)) {
      return prompt.substr(prefix.size());
    }
    return std::nullopt;
  };
  if (auto entry = log(kRelevant)) {
    const bool relevant = entry->starts_with("ERROR") || entry->starts_with("WARN");
    return MockChatServer::Reply{
        200, fmt::format(R"({{"relevant": {}}})", relevant)};
  }
  if (auto entry = log(kTriage)) {
    if (entry->find("mTLS") != std::string::npos) {
      return MockChatServer::Reply{200, R"({"domain": "Auth / TLS", "severity": 10})"};
    }
    if (entry->find("pool") != std::string::npos) {
      return MockChatServer::Reply{200, R"({"domain": "Database", "severity": 8})"};
    }
    return MockChatServer::Reply{200, R"({"domain": "Compute", "severity": 3})"};
  }
  if (auto entry = log(kRootCause)) {
    return MockChatServer::Reply{200, "fix " + *entry};
  }
  if (auto entry = log(kSummary)) {
    return MockChatServer::Reply{200, "summary " + *entry};
  }
  return std::nullopt;
}

// "fail" gets a 400, "flaky..." a 400 the first time, "busy" a 429 the first time, "yes?" the answer "yes",
// "json:<answer>" the answer, the triage prompts their answers; anything else
// is echoed.
MockChatServer::Reply reply(const std::string& prompt, int previous) {
  if (auto answer = triage(prompt)) {
    return *answer;
  }
  if (prompt.starts_with("json:")) {
    return {200, prompt.substr(5)};
  }
  if (prompt == "fail") {
    return {400, R"({"error": "bad prompt"})"};
  }
  if (prompt.starts_with("flaky") && previous == 0) {
    return {400, R"({"error": "try later"})"};
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
                 .config("ember.ai.endpoint.carbon.url", server_->url())
                 .config("ember.ai.endpoint.carbon.model", "carbon-model")
                 .config("ember.ai.endpoint.hydrogen.url", server_->url())
                 .config("ember.ai.endpoint.hydrogen.model", "hydrogen-model")
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

  static std::vector<std::string> strings(const std::vector<Row>& rows) {
    std::vector<std::string> result;
    for (const auto& row : rows) {
      result.push_back(row.getString(0));
    }
    return result;
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


TEST_F(AiGenerateTest, outputSchemaGivesTypedFields) {
  auto schema = ROW(
      {"label", "score", "n", "ok", "tags", "where"},
      {VARCHAR(),
       DOUBLE(),
       BIGINT(),
       BOOLEAN(),
       ARRAY(VARCHAR()),
       ROW({"line"}, {BIGINT()})});
  const std::string answer =
      R"({"label": "cat", "score": 0.5, "n": 3, "ok": true, )"
      R"("tags": ["a", "b"], "where": {"line": 7}})";
  auto g = ai::generate(col("p"), "mock", schema);
  auto rows = prompts({"json:" + answer})
                  .select(
                      g.getField("label"),
                      g.getField("score"),
                      g.getField("n"),
                      g.getField("ok"),
                      g.getField("tags"),
                      g.getField("where").getField("line"),
                      g.getField("status"))
                  .collect();
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].getString(0), "cat");
  EXPECT_EQ(rows[0].get<double>(1), 0.5);
  EXPECT_EQ(rows[0].getLong(2), 3);
  EXPECT_EQ(rows[0].get<bool>(3), true);
  EXPECT_EQ(rows[0].values()[4].toJson(ARRAY(VARCHAR())), R"(["a","b"])");
  EXPECT_EQ(rows[0].getLong(5), 7);
  EXPECT_EQ(rows[0].getString(6), "");

  // The request asked for that schema.
  auto format = folly::parseJson(server_->lastResponseFormat());
  EXPECT_EQ(format["type"], "json_schema");
  const auto& json = format["json_schema"]["schema"];
  EXPECT_EQ(json["type"], "object");
  EXPECT_EQ(json["additionalProperties"], false);
  EXPECT_EQ(json["required"].size(), 6);
  EXPECT_EQ(json["properties"]["score"]["type"], "number");
  EXPECT_EQ(json["properties"]["n"]["type"], "integer");
  EXPECT_EQ(json["properties"]["tags"]["items"]["type"], "string");
  EXPECT_EQ(json["properties"]["where"]["properties"]["line"]["type"], "integer");
}

TEST_F(AiGenerateTest, answerThatDoesNotFitHasStatus) {
  auto schema = ROW({"score"}, {DOUBLE()});
  auto g = ai::generate(col("p"), "mock", schema);
  auto rows = prompts({"json:not json", R"(json:{"score": "high"})", R"(json:{"score": 2})"})
                  .select(col("p"), g.getField("score"), g.getField("status"))
                  .orderBy("p")
                  .collect();
  ASSERT_EQ(rows.size(), 3);
  // Ordered by p: json:not json, json:{"score": "high"}, json:{"score": 2}.
  for (auto i : {0, 1}) {
    EXPECT_TRUE(rows[i].isNullAt(1)) << rows[i].getString(0);
    EXPECT_TRUE(
        rows[i].getString(2).starts_with("Answer does not fit the output schema"))
        << rows[i].getString(2);
  }
  EXPECT_EQ(rows[2].get<double>(1), 2);
  EXPECT_EQ(rows[2].getString(2), "");
}

TEST_F(AiGenerateTest, failedCallWithSchemaHasNullFields) {
  auto g = ai::generate(col("p"), "mock", ROW({"score"}, {DOUBLE()}));
  auto rows = prompts({"fail", Variant::null(TypeKind::VARCHAR)})
                  .select(col("p"), g.getField("score"), g.getField("status"))
                  .orderBy("p")
                  .collect();
  ASSERT_EQ(rows.size(), 2);
  // A null prompt: a null row.
  EXPECT_TRUE(rows[0].isNullAt(1));
  EXPECT_TRUE(rows[0].isNullAt(2));
  EXPECT_TRUE(rows[1].isNullAt(1));
  EXPECT_TRUE(rows[1].getString(2).starts_with("HTTP 400"));
}

TEST_F(AiGenerateTest, schemaCannotNameStatus) {
  EXPECT_THROW(
      ai::generate(col("p"), "mock", ROW({"status"}, {VARCHAR()})),
      std::exception);
  EXPECT_THROW(
      ai::generate(col("p"), "mock", ROW({"m"}, {facebook::velox::MAP(VARCHAR(), BIGINT())})),
      std::exception);
}

TEST_F(AiGenerateTest, branchesCallTheModelOnlyForTheirRows) {
  auto answer = [](const std::string& endpoint) {
    return ai::generate(col("p"), endpoint).getField("result");
  };
  auto rows = prompts({"w1", "w2", "w3"})
                  .select(
                      col("p"),
                      when(col("p") == "w1", answer("carbon"))
                          .otherwise(answer("hydrogen"))
                          .as("a"))
                  .orderBy("p")
                  .collect();
  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[0].getString(1), "echo: w1");
  EXPECT_EQ(rows[1].getString(1), "echo: w2");
  EXPECT_EQ(server_->requests("w1"), 1);
  EXPECT_EQ(server_->modelOf("w1"), "carbon-model");
  EXPECT_EQ(server_->modelOf("w2"), "hydrogen-model");
  EXPECT_EQ(server_->modelOf("w3"), "hydrogen-model");

  // Without otherwise(): null, and no call for rows no branch takes.
  rows = prompts({"w4", "w5"})
             .select(col("p"), when(col("p") == "w4", answer("carbon")).as("a"))
             .orderBy("p")
             .collect();
  EXPECT_EQ(rows[0].getString(1), "echo: w4");
  EXPECT_TRUE(rows[1].isNullAt(1));
  EXPECT_EQ(server_->requests("w5"), 0);
}

TEST_F(AiGenerateTest, whenChainsBranches) {
  auto rows = ember_
                  ->createDataframe(
                      ROW({"x"}, {BIGINT()}), {{1L}, {5L}, {9L}})
                  .select(when(col("x") > 8, lit("high"))
                              .when(col("x") > 3, lit("mid"))
                              .otherwise(lit("low"))
                              .as("level"))
                  .collect();
  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(strings(rows), (std::vector<std::string>{"low", "mid", "high"}));
  EXPECT_THROW(col("x").otherwise(lit(1)), std::exception);
  EXPECT_THROW(
      when(col("x") > 1, lit(1)).otherwise(lit(2)).when(col("x") > 2, lit(3)),
      std::exception);
}

// docs/ai-functions.md's log triage example.
TEST_F(AiGenerateTest, logTriage) {
  auto logs = ember_->createDataframe(
      ROW({"service", "log_entry"}, {VARCHAR(), VARCHAR()}),
      {{"payment-mesh", "ERROR mTLS handshake failed: root CA mismatch"},
       {"checkout", "ERROR connection pool exhausted on replica"},
       {"image-resizer", "WARN pod restarted after exceeding memory limit"},
       {"scheduler", "INFO cron heartbeat ok"}});
  auto prompt = [](std::string_view text) {
    return concat({lit(std::string(text)), col("log_entry")});
  };
  auto ask = [&](std::string_view text, const std::string& endpoint) {
    return ai::generate(prompt(text), endpoint).getField("result");
  };

  auto triaged =
      logs.filter(ai::generate(
                      prompt(kRelevant), "hydrogen", ROW({"relevant"}, {BOOLEAN()}))
                      .getField("relevant"))
          .withColumn(
              "t",
              ai::generate(
                  prompt(kTriage),
                  "hydrogen",
                  ROW({"domain", "severity"}, {VARCHAR(), DOUBLE()})))
          .select(
              col("service"),
              col("log_entry"),
              col("t").getField("domain").as("domain"),
              col("t").getField("severity").as("severity"));
  auto report =
      triaged
          .select(
              col("service"),
              col("domain"),
              col("severity"),
              when(col("severity") >= 8, ask(kRootCause, "carbon"))
                  .otherwise(ask(kSummary, "hydrogen"))
                  .as("diagnosis"))
          .orderBy(col("severity").desc());
  auto rows = report.collect();

  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[0].getString(0), "payment-mesh");
  EXPECT_EQ(rows[0].getString(1), "Auth / TLS");
  EXPECT_EQ(rows[0].get<double>(2), 10);
  EXPECT_EQ(rows[0].getString(3), "fix ERROR mTLS handshake failed: root CA mismatch");
  EXPECT_EQ(rows[1].getString(0), "checkout");
  EXPECT_EQ(rows[1].getString(3), "fix ERROR connection pool exhausted on replica");
  EXPECT_EQ(rows[2].getString(0), "image-resizer");
  EXPECT_EQ(rows[2].get<double>(2), 3);
  EXPECT_EQ(
      rows[2].getString(3),
      "summary WARN pod restarted after exceeding memory limit");

  // Only high-severity rows reached carbon; the routine log was never triaged.
  const std::string mtls = "ERROR mTLS handshake failed: root CA mismatch";
  const std::string oom = "WARN pod restarted after exceeding memory limit";
  EXPECT_EQ(server_->modelOf(std::string(kRootCause) + mtls), "carbon-model");
  EXPECT_EQ(server_->requests(std::string(kRootCause) + oom), 0);
  EXPECT_EQ(server_->modelOf(std::string(kSummary) + oom), "hydrogen-model");
  EXPECT_EQ(
      server_->requests(std::string(kTriage) + "INFO cron heartbeat ok"), 0);
}


// The prompts in two files, so that the AI call runs in a map stage, whose
// output the session keeps.
std::string writePrompts(
    const std::string& name,
    const std::vector<std::string>& first,
    const std::vector<std::string>& second) {
  const auto dir = fs::temp_directory_path() /
      fmt::format("ai_generate_test_{}_{}", name, ::getpid());
  fs::remove_all(dir);
  fs::create_directories(dir);
  for (const auto& [file, lines] :
       {std::pair{"part-0.txt", first}, std::pair{"part-1.txt", second}}) {
    std::ofstream out(dir / file);
    for (const auto& line : lines) {
      out << line << "\n";
    }
  }
  return dir.string() + "/";
}

// The answers to the prompts in 'dir' and their counts.
Dataframe countAnswers(Session& session, const std::string& dir) {
  return session.read()
      .text(dir)
      .withColumn("g", ai::generate(col("value"), "mock"))
      .groupBy(col("g").getField("result").as("answer"))
      .count()
      .orderBy("answer");
}

// ai_generate is deterministic: stages that call it are reused.
TEST_F(AiGenerateTest, rerunReusesTheAiStage) {
  const auto dir = writePrompts("rerun", {"again 1", "again 2"}, {"again 1"});
  auto first = countAnswers(*ember_, dir).collect();
  ASSERT_EQ(first.size(), 2);
  EXPECT_EQ(first[0].getString(0), "echo: again 1");
  EXPECT_EQ(first[0].get<int64_t>(1), 2);
  const auto calls = server_->requests("again 1");
  EXPECT_GE(calls, 1);

  EXPECT_EQ(countAnswers(*ember_, dir).collect(), first);
  EXPECT_EQ(server_->requests("again 1"), calls);
  fs::remove_all(dir);
}

// A stage with failed rows is not kept, so a rerun asks again; once all rows
// have answers, the stage is kept.
TEST_F(AiGenerateTest, rerunAsksAgainForFailedRows) {
  const auto dir = writePrompts("failed", {"flaky", "steady"}, {"steady"});
  auto first = countAnswers(*ember_, dir).collect();
  ASSERT_EQ(first.size(), 2);
  EXPECT_TRUE(first[0].isNullAt(0));
  EXPECT_EQ(first[1].getString(0), "echo: steady");
  EXPECT_EQ(server_->requests("flaky"), 1);
  const auto steady = server_->requests("steady");

  auto second = countAnswers(*ember_, dir).collect();
  ASSERT_EQ(second.size(), 2);
  EXPECT_EQ(second[0].getString(0), "echo: flaky");
  EXPECT_EQ(server_->requests("flaky"), 2);
  EXPECT_GT(server_->requests("steady"), steady);

  EXPECT_EQ(countAnswers(*ember_, dir).collect(), second);
  EXPECT_EQ(server_->requests("flaky"), 2);
  fs::remove_all(dir);
}

// Nor are the stages that read a stage with failed rows: the second
// aggregation's stage would otherwise keep the failure.
TEST_F(AiGenerateTest, stagesAfterFailedRowsAreNotKept) {
  const auto dir = writePrompts("downstream", {"flaky later", "a"}, {"b"});
  auto histogram = [&] {
    return countAnswers(*ember_, dir)
        .groupBy(col("answer").isNull().as("failed"))
        .count()
        .orderBy("failed");
  };
  auto first = histogram().collect();
  ASSERT_EQ(first.size(), 2);
  EXPECT_EQ(server_->requests("flaky later"), 1);

  auto second = histogram().collect();
  ASSERT_EQ(second.size(), 1);
  EXPECT_FALSE(second[0].get<bool>(0));
  EXPECT_EQ(server_->requests("flaky later"), 2);
  fs::remove_all(dir);
}

// The stage keys of a program that asks the endpoint "mock" in sessions with
// these settings for it.
std::vector<std::string> stageKeys(
    const std::map<std::string, std::string>& endpoint) {
  auto builder = Session::builder().appName("AiStageKeyTest").master("local[2]");
  for (const auto& [field, value] : endpoint) {
    builder.config("ember.ai.endpoint.mock." + field, value);
  }
  auto session = builder.getOrCreate();
  const auto explain =
      session
          .createDataframe(ROW({"p"}, {VARCHAR()}), {{Variant("a")}})
          .withColumn("answer", ai::generate(col("p"), "mock").getField("result"))
          .groupBy("answer")
          .count()
          .explainString();
  session.stop();
  std::vector<std::string> keys;
  for (auto pos = explain.find("[key "); pos != std::string::npos;
       pos = explain.find("[key ", pos + 1)) {
    keys.push_back(explain.substr(pos, explain.find(']', pos) - pos));
  }
  EXPECT_FALSE(keys.empty()) << explain;
  return keys;
}

TEST(AiStageKeyTest, keysFollowTheModel) {
  const std::map<std::string, std::string> base{
      {"url", "http://localhost:1/v1"}, {"model", "small"}};
  const auto keys = stageKeys(base);
  EXPECT_EQ(stageKeys(base), keys);

  // Answers may change with the url, model or kind.
  auto bigger = base;
  bigger["model"] = "big";
  EXPECT_NE(stageKeys(bigger), keys);
  auto moved = base;
  moved["url"] = "http://localhost:2/v1";
  EXPECT_NE(stageKeys(moved), keys);

  // They do not change with the API key, concurrency or qps.
  auto tuned = base;
  tuned["api_key_env"] = "SOME_KEY";
  tuned["max_concurrency"] = "4";
  tuned["qps"] = "10";
  EXPECT_EQ(stageKeys(tuned), keys);
}

} // namespace
} // namespace ember
