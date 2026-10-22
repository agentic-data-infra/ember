// Dataframe API tests on small in-memory inputs.

#include <gtest/gtest.h>

#include "ember/Session.h"
#include "ember/Udf.h"
#include "ember/functions.h"

namespace ember {
namespace {

using namespace ember::functions;
using facebook::velox::ARRAY;
using facebook::velox::BIGINT;
using facebook::velox::ROW;
using facebook::velox::Variant;
using facebook::velox::VARCHAR;

template <typename T>
struct TimesTwoFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(int64_t& out, const int64_t& in) {
    out = in * 2;
  }
};

class DataframeTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    ember_ = Session::builder()
                 .appName("DataframeTest")
                 .master("local[4]")
                 .getOrCreate();
  }

  static void TearDownTestCase() {
    ember_->stop();
    ember_.reset();
  }

  // Columns: name (varchar), dept (varchar), salary (bigint).
  Dataframe people() {
    return ember_->createDataframe(
        ROW({"name", "dept", "salary"}, {VARCHAR(), VARCHAR(), BIGINT()}),
        {{"alice", "eng", 100L},
         {"bob", "eng", 80L},
         {"carol", "sales", 70L},
         {"dave", "sales", 50L},
         {"erin", "hr", 60L}});
  }

  static std::vector<std::string> strings(
      const std::vector<Row>& rows,
      size_t column = 0) {
    std::vector<std::string> result;
    for (const auto& row : rows) {
      result.push_back(row.getString(column));
    }
    return result;
  }

  static inline std::optional<Session> ember_;
};

TEST_F(DataframeTest, selectFilterWithColumn) {
  auto df = people()
                .filter(col("salary") >= 60)
                .withColumn("bonus", col("salary") / 10)
                .select("name", "bonus")
                .orderBy(col("name"));

  EXPECT_EQ(df.columns(), (std::vector<std::string>{"name", "bonus"}));
  auto rows = df.collect();
  ASSERT_EQ(rows.size(), 4);
  EXPECT_EQ(rows[0], Row({Variant("alice"), Variant(10L)}));
  EXPECT_EQ(rows[3], Row({Variant("erin"), Variant(6L)}));
}

TEST_F(DataframeTest, typedColumnsAndSqlStringsAgree) {
  auto typed = people()
                   .select(upper(col("name")).as("n"), (col("salary") + 1).as("s"))
                   .filter("s > 75")
                   .orderBy("n");
  auto sql = people()
                 .selectExpr({"upper(name) AS n", "salary + 1 AS s"})
                 .filter(col("s") > 75)
                 .orderBy("n");
  EXPECT_EQ(typed.collect(), sql.collect());
  EXPECT_EQ(
      strings(typed.collect()),
      (std::vector<std::string>{"ALICE", "BOB"}));
}

TEST_F(DataframeTest, withColumnReplacesExisting) {
  auto df = people().withColumn("salary", col("salary") * 2);
  EXPECT_EQ(df.columns(), (std::vector<std::string>{"name", "dept", "salary"}));
  auto rows = df.filter(col("name") == "alice").collect();
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].getLong(2), 200);
}

TEST_F(DataframeTest, renameAndDrop) {
  auto df = people().withColumnRenamed("dept", "team").drop({"salary"});
  EXPECT_EQ(df.columns(), (std::vector<std::string>{"name", "team"}));
  auto rows = df.filter(col("team") == "hr").collect();
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0], Row({Variant("erin"), Variant("hr")}));
}

TEST_F(DataframeTest, groupByAggregates) {
  auto df = people()
                .groupBy("dept")
                .agg(count(col("name")).as("n"), sum(col("salary")).as("total"))
                .orderBy(col("total").desc());
  auto rows = df.collect();
  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[0], Row({Variant("eng"), Variant(2L), Variant(180L)}));
  EXPECT_EQ(rows[1], Row({Variant("sales"), Variant(2L), Variant(120L)}));
  EXPECT_EQ(rows[2], Row({Variant("hr"), Variant(1L), Variant(60L)}));

  auto counts = people().groupBy("dept").count().orderBy("dept");
  EXPECT_EQ(counts.columns(), (std::vector<std::string>{"dept", "count"}));
  EXPECT_EQ(counts.collect()[0], Row({Variant("eng"), Variant(2L)}));

  auto maxes = people().groupBy("dept").max({"salary"}).orderBy("dept");
  EXPECT_EQ(maxes.columns(), (std::vector<std::string>{"dept", "max(salary)"}));
  EXPECT_EQ(maxes.collect()[2], Row({Variant("sales"), Variant(70L)}));
}

TEST_F(DataframeTest, countLimitDistinct) {
  EXPECT_EQ(people().count(), 5);
  EXPECT_EQ(people().limit(2).count(), 2);
  EXPECT_EQ(people().select("dept").distinct().count(), 3);
}

TEST_F(DataframeTest, unionAndJoin) {
  auto all = people().unionAll(people());
  EXPECT_EQ(all.count(), 10);

  auto depts = ember_->createDataframe(
      ROW({"dept", "floor"}, {VARCHAR(), BIGINT()}),
      {{"eng", 3L}, {"sales", 1L}});
  auto joined =
      people().join(depts, {"dept"}).select("name", "floor").orderBy("name");
  auto rows = joined.collect();
  ASSERT_EQ(rows.size(), 4);
  EXPECT_EQ(rows[0], Row({Variant("alice"), Variant(3L)}));

  auto left = people().join(depts, {"dept"}, "left");
  EXPECT_EQ(left.count(), 5);
}

TEST_F(DataframeTest, explode) {
  auto df = ember_->createDataframe(
      ROW({"id", "tags"}, {BIGINT(), ARRAY(VARCHAR())}),
      {{1L, Variant::array({Variant("a"), Variant("b")})},
       {2L, Variant::array({Variant("c")})}});

  auto typed = df.select(col("id"), explode(col("tags")).as("tag"))
                   .orderBy("tag");
  EXPECT_EQ(typed.columns(), (std::vector<std::string>{"id", "tag"}));
  auto rows = typed.collect();
  ASSERT_EQ(rows.size(), 3);
  EXPECT_EQ(rows[2], Row({Variant(2L), Variant("c")}));

  // The same through a SQL string; an unaliased explode is named "col".
  auto sql = df.selectExpr({"explode(tags)"}).orderBy("col");
  EXPECT_EQ(sql.columns(), (std::vector<std::string>{"col"}));
  EXPECT_EQ(strings(sql.collect()), (std::vector<std::string>{"a", "b", "c"}));
}

TEST_F(DataframeTest, splitTakesRegex) {
  auto df = ember_->createDataframe(
      ROW({"s"}, {VARCHAR()}), {{"one1two22three"}, {"a.b"}});
  auto rows = df.select(explode(split(col("s"), R"(\d+)")).as("part"))
                  .collect();
  EXPECT_EQ(
      strings(rows),
      (std::vector<std::string>{"one", "two", "three", "a.b"}));

  // "." is a regex: every character is a separator.
  auto dots = df.filter(col("s") == "a.b")
                  .select(size(split(col("s"), ".")).as("n"))
                  .collect();
  ASSERT_EQ(dots.size(), 1);
  EXPECT_EQ(dots[0][0], Variant(4));
}

TEST_F(DataframeTest, regexpExtractAndRound) {
  auto df = ember_->createDataframe(
      ROW({"url", "x"}, {VARCHAR(), facebook::velox::DOUBLE()}),
      {{"https://www.Example.org/a?b", 2.345}, {"no url", -1.5}});
  auto rows = df.select(
                    lower(regexp_extract(
                              col("url"), R"(^https?://(?:www\.)?([^/?]+))", 1))
                        .as("host"),
                    round(col("x"), 2).as("r2"),
                    round(col("x")).as("r0"))
                  .orderBy(col("host").desc())
                  .collect();
  ASSERT_EQ(rows.size(), 2);
  EXPECT_EQ(rows[0], Row({Variant("example.org"), Variant(2.35), Variant(2.0)}));
  // No match: an empty string. Rounds half away from zero.
  EXPECT_EQ(rows[1], Row({Variant(""), Variant(-1.5), Variant(-2.0)}));
}

TEST_F(DataframeTest, udfThroughCall) {
  registerUdf<TimesTwoFunction, int64_t, int64_t>("times_two");
  auto rows = people()
                  .select(call("times_two", col("salary")).as("x"))
                  .orderBy(col("x").desc())
                  .limit(1)
                  .collect();
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].getLong(0), 200);

  auto sql = people().selectExpr({"times_two(salary) AS x"}).agg({max(col("x"))});
  EXPECT_EQ(sql.collect()[0].getLong(0), 200);
}

TEST_F(DataframeTest, nullsSortFirstAscendingAndLastDescending) {
  auto df = ember_->createDataframe(
      ROW({"x"}, {BIGINT()}), {{2L}, {Variant::null(facebook::velox::TypeKind::BIGINT)}, {1L}});
  auto asc = df.orderBy(col("x")).collect();
  EXPECT_TRUE(asc[0].isNullAt(0));
  auto desc = df.orderBy(col("x").desc()).collect();
  EXPECT_TRUE(desc[2].isNullAt(0));
  EXPECT_EQ(desc[0].getLong(0), 2);
}

TEST_F(DataframeTest, showFormatsTable) {
  auto df = people().select("name", "salary").orderBy("name");
  EXPECT_EQ(
      df.showString(2),
      "+-----+------+\n"
      "| name|salary|\n"
      "+-----+------+\n"
      "|alice|   100|\n"
      "|  bob|    80|\n"
      "+-----+------+\n"
      "only showing top 2 rows\n");

  auto longValue = ember_->createDataframe(
      ROW({"s"}, {VARCHAR()}), {{"abcdefghijklmnopqrstuvwxyz"}});
  EXPECT_EQ(
      longValue.showString(20, /*truncate=*/true),
      "+--------------------+\n"
      "|                   s|\n"
      "+--------------------+\n"
      "|abcdefghijklmnopq...|\n"
      "+--------------------+\n");
  EXPECT_EQ(
      longValue.showString(20, /*truncate=*/false),
      "+--------------------------+\n"
      "|s                         |\n"
      "+--------------------------+\n"
      "|abcdefghijklmnopqrstuvwxyz|\n"
      "+--------------------------+\n");
}

TEST_F(DataframeTest, dataFramesAreImmutableValues) {
  auto base = people();
  auto filtered = base.filter(col("dept") == "eng");
  auto copy = filtered;
  EXPECT_EQ(base.count(), 5);
  EXPECT_EQ(filtered.count(), 2);
  EXPECT_EQ(copy.count(), 2);
}

TEST_F(DataframeTest, errorsSurfaceAtCompile) {
  // Transformations only record steps; the plan is built by compile().
  auto missingColumn = people().select("nope");
  EXPECT_THROW(missingColumn.compile(), facebook::velox::VeloxUserError);
  EXPECT_THROW(missingColumn.collect(), facebook::velox::VeloxUserError);

  auto missingFunction = people().select(call("no_such_function", col("name")));
  EXPECT_THROW(missingFunction.schema(), facebook::velox::VeloxUserError);
}

TEST_F(DataframeTest, eagerAnalysisReportsErrorsAtTransformation) {
  auto state = std::make_shared<SessionState>(
      "eager", "local[2]",
      std::map<std::string, std::string>{{"ember.eager_analysis", "true"}});
  auto df = DataframeReader(state).text("/dev/null");
  EXPECT_EQ(df.columns(), (std::vector<std::string>{"value"}));
  EXPECT_THROW(df.select("nope"), facebook::velox::VeloxUserError);
}

TEST_F(DataframeTest, compileIsCachedAndShared) {
  auto df = people().filter(col("salary") > 60).select("name");
  auto compiled = df.compile();
  ASSERT_NE(compiled->physicalPlan, nullptr);
  EXPECT_EQ(df.compile(), compiled);

  // Copies share the cache; actions run the cached plan, repeatedly.
  auto copy = df;
  EXPECT_EQ(copy.compile(), compiled);
  EXPECT_EQ(df.collect().size(), 3);
  EXPECT_EQ(copy.collect().size(), 3);
  EXPECT_EQ(df.compile(), compiled);
  EXPECT_EQ(df.physicalPlan(), compiled->physicalPlan);

  // A derived Dataframe compiles its own plan.
  auto derived = df.limit(1);
  EXPECT_NE(derived.compile(), compiled);
  EXPECT_EQ(derived.collect().size(), 1);
}

TEST_F(DataframeTest, longChainsBuildOnce) {
  auto df = people().select("salary");
  for (int i = 0; i < 200; ++i) {
    df = df.withColumn("salary", col("salary") + 1);
  }
  auto rows = df.orderBy("salary").collect();
  ASSERT_EQ(rows.size(), 5);
  EXPECT_EQ(rows[0].getLong(0), 250);
  EXPECT_EQ(df.columns(), (std::vector<std::string>{"salary"}));
}

TEST_F(DataframeTest, columnLiteral) {
  auto rows = people().filter("salary"_c > 90).collect();
  ASSERT_EQ(rows.size(), 1);
  EXPECT_EQ(rows[0].getString(0), "alice");
}

} // namespace
} // namespace ember
