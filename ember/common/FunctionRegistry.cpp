#include "ember/FunctionRegistry.h"

#include <mutex>

#include <folly/container/F14Set.h>

#include "ember/Stoke.h"

#include "axiom/optimizer/FunctionRegistry.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/functions/sparksql/registration/Register.h"

namespace ember {

namespace lp = facebook::axiom::logical_plan;
namespace velox = facebook::velox;

namespace {

// Functions Axiom's optimizer gives a meaning to (comparisons, boolean logic,
// subscripts, ...). These stay Presto so the optimizer keeps recognizing them.
folly::F14FastSet<std::string> optimizerFunctions() {
  auto* registry = facebook::axiom::optimizer::FunctionRegistry::instance();
  folly::F14FastSet<std::string> names{
      registry->equality(), registry->negation(), "in", "neq"};
  for (const auto& name :
       {registry->elementAt(),
        registry->subscript(),
        registry->cardinality(),
        registry->bigintAddition(),
        registry->lessThan(),
        registry->lessThanOrEqual(),
        registry->greaterThan(),
        registry->greaterThanOrEqual(),
        registry->isNull(),
        registry->distinctFrom(),
        registry->between(),
        registry->like(),
        registry->random()}) {
    if (name.has_value()) {
      names.insert(*name);
    }
  }
  for (const auto& [_, name] : registry->specialForms()) {
    names.insert(name);
  }
  for (const auto& [name, reverse] : registry->reversibleFunctions()) {
    names.insert(name);
    names.insert(reverse);
  }
  return names;
}

} // namespace

void registerFunctions() {
  static std::once_flag once;
  std::call_once(once, [] {
    velox::functions::prestosql::registerAllScalarFunctions();
    velox::aggregate::prestosql::registerAllAggregateFunctions();
    velox::window::prestosql::registerAllWindowFunctions();
    velox::functions::sparksql::registerFunctions(kFunctionPrefix);
    velox::functions::aggregate::sparksql::registerAggregateFunctions(
        kFunctionPrefix);
    facebook::axiom::optimizer::FunctionRegistry::registerPrestoFunctions();
    registerStokeFunctions();
  });
}

lp::ExprResolver::FunctionRewriteHook functionRewriteHook() {
  auto keepPresto =
      std::make_shared<const folly::F14FastSet<std::string>>(
          optimizerFunctions());
  return [keepPresto](
             const std::string& name,
             const std::vector<lp::ExprPtr>& args) -> lp::ExprPtr {
    if (keepPresto->contains(name)) {
      return nullptr;
    }
    auto prefixedName = kFunctionPrefix + name;
    std::vector<velox::TypePtr> argTypes;
    argTypes.reserve(args.size());
    for (const auto& arg : args) {
      argTypes.push_back(arg->type());
    }
    auto type = velox::resolveFunction(prefixedName, argTypes);
    if (type == nullptr) {
      return nullptr;
    }
    return std::make_shared<lp::CallExpr>(type, std::move(prefixedName), args);
  };
}

} // namespace ember
