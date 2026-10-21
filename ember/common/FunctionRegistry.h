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

#include <string>

#include "axiom/logical_plan/ExprResolver.h"

namespace ember {

inline constexpr const char* kFunctionPrefix = "ember_";

/// Registers Presto scalar, aggregate and window functions, sparksql scalar
/// and aggregate functions (prefixed), and Axiom's function metadata. Safe to
/// call more than once.
void registerFunctions();

/// Rewrites a call to 'name' into a call to the sparksql function of the
/// same name, when it is registered and accepts the arguments.
facebook::axiom::logical_plan::ExprResolver::FunctionRewriteHook
functionRewriteHook();

} // namespace ember
