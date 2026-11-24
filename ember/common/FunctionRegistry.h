// Function setup shared by the driver and (later) the workers, which must
// register the same functions so plans resolve identically on both sides.
//
// Axiom's optimizer and SQL parser speak Presto function names (eq, not,
// count, ...), so the Presto package is the base. Velox's sparksql package is
// registered on top under kFunctionPrefix, and functionRewriteHook() resolves
// each scalar call by name to that version when one exists: split(s, '\\W+')
// takes a regex. Operators and other functions that Axiom's
// optimizer reasons about keep their Presto implementation.

#pragma once

#include <memory>
#include <string>

#include "axiom/logical_plan/ExprResolver.h"

namespace facebook::axiom::optimizer {
class FunctionRegistry;
}

namespace ember {

inline constexpr const char* kFunctionPrefix = "ember_";

/// Registers Presto scalar, aggregate and window functions and sparksql
/// scalar and aggregate functions (prefixed) with Velox. Safe to call more
/// than once.
void registerFunctions();

/// The Presto function semantics Axiom's optimizer plans with.
const std::shared_ptr<const facebook::axiom::optimizer::FunctionRegistry>&
optimizerFunctionRegistry();

/// Rewrites a call to 'name' into a call to the sparksql function of the
/// same name, when it is registered and accepts the arguments.
facebook::axiom::logical_plan::ExprResolver::FunctionRewriteHook
functionRewriteHook();

} // namespace ember
