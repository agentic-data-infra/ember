// Builds and runs a trivial Velox plan (Values -> Project) to check that
// Ember compiles and links against the CMake-built Velox.

#include <gtest/gtest.h>

#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace ember {
namespace {

using namespace facebook::velox;

class VeloxSmokeTest : public exec::test::OperatorTestBase {};

TEST_F(VeloxSmokeTest, valuesProject) {
  auto input = makeRowVector(
      {"a", "s"},
      {makeFlatVector<int64_t>({1, 2, 3}),
       makeFlatVector<std::string>({"Hello", "Ember", "World"})});

  auto plan = exec::test::PlanBuilder()
                  .values({input})
                  .project({"a * 10 AS b", "lower(s) AS t"})
                  .planNode();

  auto expected = makeRowVector(
      {"b", "t"},
      {makeFlatVector<int64_t>({10, 20, 30}),
       makeFlatVector<std::string>({"hello", "ember", "world"})});

  exec::test::AssertQueryBuilder(plan).assertResults(expected);
}

} // namespace
} // namespace ember
