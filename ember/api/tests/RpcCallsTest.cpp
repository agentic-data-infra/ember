// Calls to async RPC functions under conditions and in filters: each call is
// made on exactly the rows that reach it (see ember/api/RpcCalls.h).

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <mutex>

#include <fmt/format.h>
#include <folly/futures/Future.h>
#include <gtest/gtest.h>

#include "ember/Session.h"
#include "ember/functions.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/rpc/AsyncRPCFunction.h"
#include "velox/expression/rpc/AsyncRPCFunctionRegistry.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

namespace ember {
namespace {

namespace fs = std::filesystem;
namespace rpc = facebook::velox::exec::rpc;
namespace velox = facebook::velox;
using namespace ember::functions;

// The prompts echo() was called with.
std::mutex promptsMutex;
std::vector<std::string> prompts;

// echo(prompt) = "echo: " + prompt, as an async RPC function.
class EchoFunction : public rpc::AsyncRPCFunction {
 public:
  std::string name() const override {
    return "echo";
  }

  velox::TypePtr resultType() const override {
    return velox::VARCHAR();
  }

  std::vector<
      std::pair<velox::vector_size_t, folly::SemiFuture<rpc::RPCResponse>>>
  dispatchPerRow(
      const velox::SelectivityVector& rows,
      const std::vector<velox::VectorPtr>& args) override {
    std::vector<
        std::pair<velox::vector_size_t, folly::SemiFuture<rpc::RPCResponse>>>
        responses;
    velox::DecodedVector input(*args[0], rows);
    rows.applyToSelected([&](velox::vector_size_t row) {
      rpc::RPCResponse response;
      response.rowId = row;
      if (input.isNullAt(row)) {
        response.setError(velox::rpc::RPCErrorKind::kNullInput, "null");
      } else {
        const auto prompt = input.valueAt<velox::StringView>(row).str();
        {
          std::lock_guard<std::mutex> lock(promptsMutex);
          prompts.push_back(prompt);
        }
        response.setPayload(rpc::makeTextPayload("echo: " + prompt));
      }
      responses.emplace_back(row, folly::makeSemiFuture(std::move(response)));
    });
    return responses;
  }

  velox::VectorPtr buildOutput(
      const std::vector<rpc::RPCResponse>& responses,
      velox::memory::MemoryPool* pool) const override {
    auto result =
        velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
            velox::VARCHAR(), responses.size(), pool);
    for (size_t i = 0; i < responses.size(); ++i) {
      if (responses[i].hasError()) {
        result->setNull(i, true);
      } else {
        result->set(
            i,
            velox::StringView(
                rpc::responseAs<rpc::TextPayload>(responses[i]).text));
      }
    }
    return result;
  }
};

Column echo(const Column& prompt) {
  return call("echo", prompt);
}

class RpcCallsTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    rpc::AsyncRPCFunctionRegistry::registerFunction(
        "echo",
        [] { return std::make_shared<EchoFunction>(); },
        {velox::exec::FunctionSignatureBuilder()
             .returnType("varchar")
             .argumentType("varchar")
             .build()});
    ember_ =
        Session::builder().appName("RpcCallsTest").master("local[2]").getOrCreate();
  }

  static void TearDownTestCase() {
    ember_->stop();
    ember_.reset();
  }

  void SetUp() override {
    dir_ = fs::temp_directory_path() /
        fmt::format("rpc_calls_test_{}_{}",
                    ::getpid(),
                    testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::create_directories(dir_);
    std::ofstream(dir_ / "part-0.txt") << "a\nb\n";
    std::ofstream(dir_ / "part-1.txt") << "c\n";
    std::lock_guard<std::mutex> lock(promptsMutex);
    prompts.clear();
  }

  void TearDown() override {
    fs::remove_all(dir_);
  }

  Dataframe lines() const {
    return ember_->read().text(dir_.string() + "/");
  }

  // The prompts echo() was called with, sorted.
  static std::vector<std::string> calls() {
    std::lock_guard<std::mutex> lock(promptsMutex);
    auto result = prompts;
    std::sort(result.begin(), result.end());
    return result;
  }

  // Column 'name' of 'df', sorted by "value".
  static std::vector<std::optional<std::string>> column(
      const Dataframe& df,
      const std::string& name) {
    std::vector<std::optional<std::string>> result;
    for (const auto& row : df.orderBy("value").select(name).collect()) {
      result.push_back(
          row.isNullAt(0) ? std::nullopt
                          : std::optional<std::string>(row.getString(0)));
    }
    return result;
  }

  static inline std::optional<Session> ember_;
  fs::path dir_;
};

using Strings = std::vector<std::optional<std::string>>;
using Prompts = std::vector<std::string>;

TEST_F(RpcCallsTest, wholeExpression) {
  EXPECT_EQ(
      column(lines().withColumn("answer", echo(col("value"))), "answer"),
      (Strings{"echo: a", "echo: b", "echo: c"}));
  EXPECT_EQ(calls(), (Prompts{"a", "b", "c"}));
}

TEST_F(RpcCallsTest, whenOtherwise) {
  auto answer = when(col("value") == "a", echo(col("value")))
                    .when(col("value") == "b", echo(concat({col("value"), lit("!")})))
                    .otherwise(lit("skipped"));
  EXPECT_EQ(
      column(lines().withColumn("answer", answer), "answer"),
      (Strings{"echo: a", "echo: b!", "skipped"}));
  EXPECT_EQ(calls(), (Prompts{"a", "b!"}));
}

TEST_F(RpcCallsTest, callInOtherwise) {
  auto answer =
      when(col("value") == "a", lit("first")).otherwise(echo(col("value")));
  EXPECT_EQ(
      column(lines().withColumn("answer", answer), "answer"),
      (Strings{"first", "echo: b", "echo: c"}));
  EXPECT_EQ(calls(), (Prompts{"b", "c"}));
}

TEST_F(RpcCallsTest, callInCondition) {
  // The same call in the condition and the value is made once per row.
  auto answer = when(echo(col("value")) == "echo: a", echo(col("value")))
                    .otherwise(echo(concat({col("value"), lit("?")})));
  EXPECT_EQ(
      column(lines().withColumn("answer", answer), "answer"),
      (Strings{"echo: a", "echo: b?", "echo: c?"}));
  EXPECT_EQ(calls(), (Prompts{"a", "b", "b?", "c", "c?"}));
}

TEST_F(RpcCallsTest, nestedCalls) {
  auto answer = when(col("value") == "a", echo(echo(col("value"))));
  EXPECT_EQ(
      column(lines().withColumn("answer", answer), "answer"),
      (Strings{"echo: echo: a", std::nullopt, std::nullopt}));
  EXPECT_EQ(calls(), (Prompts{"a", "echo: a"}));
}

TEST_F(RpcCallsTest, andOrCoalesce) {
  auto df = lines()
                .withColumn("and", col("value") == "a" && echo(col("value")) == "echo: a")
                .withColumn("or", col("value") == "a" || echo(col("value")) == "echo: b")
                .withColumn(
                    "first",
                    coalesce({when(col("value") == "c", lit("c!")),
                              echo(col("value"))}));
  auto rows = df.orderBy("value").collect();
  ASSERT_EQ(rows.size(), 3);
  EXPECT_TRUE(rows[0].get<bool>(1));
  EXPECT_FALSE(rows[1].get<bool>(1));
  EXPECT_TRUE(rows[0].get<bool>(2));
  EXPECT_TRUE(rows[1].get<bool>(2));
  EXPECT_FALSE(rows[2].get<bool>(2));
  EXPECT_EQ(rows[2].getString(3), "c!");
  // "and": a. "or": b and c. "coalesce": a and b.
  EXPECT_EQ(calls(), (Prompts{"a", "a", "b", "b", "c"}));
}

TEST_F(RpcCallsTest, filterCallsOnlyRowsOfCheapConditions) {
  auto df = lines().filter(
      col("value") != "a" && echo(col("value")) == "echo: b");
  EXPECT_EQ(column(df, "value"), (Strings{"b"}));
  EXPECT_EQ(calls(), (Prompts{"b", "c"}));
}

TEST_F(RpcCallsTest, filterWithCallsInSeveralConjuncts) {
  auto df = lines().filter(
      echo(col("value")) != "echo: a" && echo(concat({col("value"), lit("?")})) == "echo: c?");
  EXPECT_EQ(column(df, "value"), (Strings{"c"}));
  EXPECT_EQ(calls(), (Prompts{"a", "b", "b?", "c", "c?"}));
}

TEST_F(RpcCallsTest, selectAndReplaceColumn) {
  auto df = lines()
                .withColumn("value", when(col("value") != "b", echo(col("value"))))
                .select(col("value"), when(col("value").isNull(), echo(lit("x"))).alias("x"));
  auto rows = df.collect();
  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(calls(), (Prompts{"a", "c", "x"}));
}

} // namespace
} // namespace ember
