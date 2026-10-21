// Builds a trivial Axiom logical plan to check that Ember compiles and links
// against the CMake-built Axiom.

#include <gtest/gtest.h>

#include "axiom/logical_plan/PlanBuilder.h"
#include "axiom/logical_plan/PlanPrinter.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace ember {
namespace {

using namespace facebook::velox;
namespace lp = facebook::axiom::logical_plan;

class AxiomSmokeTest : public testing::Test {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
    functions::prestosql::registerAllScalarFunctions();
    aggregate::prestosql::registerAllAggregateFunctions();
  }
};

TEST_F(AxiomSmokeTest, valuesProjectAggregate) {
  auto plan = lp::PlanBuilder()
                  .values(
                      ROW({"word"}, VARCHAR()),
                      std::vector<Variant>{
                          Variant::row({"a"}),
                          Variant::row({"b"}),
                          Variant::row({"a"})})
                  .project({"lower(word) AS word"})
                  .aggregate({"word"}, {"count(*) AS cnt"})
                  .build();

  ASSERT_NE(plan, nullptr);
  EXPECT_EQ(plan->outputType()->toString(), "ROW<word:VARCHAR,cnt:BIGINT>");
  LOG(INFO) << lp::PlanPrinter::toText(*plan);
}

} // namespace
} // namespace ember
