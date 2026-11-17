// Calls to async RPC functions, such as LLM calls, taken out of conditions.
//
// Axiom's v2 optimizer runs each such call in an RPC plan node of its own,
// which calls on every row of its input, and it rejects calls under a
// condition (when/otherwise, if, coalesce). So Ember computes each call in a
// projection before the expression that uses it, with its first argument
// (the prompt) set to null on the rows that would not have reached the call:
// an async RPC function makes no call for a null input. The expression then
// reads the call's column.
//
// The guard of a call is the condition under which Velox would have
// evaluated it:
//
//   when(c1, x).when(c2, f(p))   c2 and not c1 (a null condition is false)
//   otherwise(f(p))              no condition holds
//   a and f(p)                   a is not false
//   a or f(p)                    a is not true
//   coalesce(x, f(p))            x is null
//
// Nested conditions join their guards with "and". Identical calls under the
// same guard are made once.

#pragma once

#include <string>
#include <vector>

#include "axiom/logical_plan/ExprApi.h"

namespace ember {

struct RpcCalls {
  /// Projections to add before the expressions, in this order; each reads the
  /// input and the columns of the ones before it.
  std::vector<std::vector<facebook::axiom::logical_plan::ExprApi>> levels;
  /// The expressions, reading the columns the levels add.
  std::vector<facebook::axiom::logical_plan::ExprApi> exprs;
};

/// True if 'expr' calls an async RPC function.
bool callsRpc(const facebook::velox::core::ExprPtr& expr);

/// Takes every RPC call out of 'exprs', except a call that is a whole
/// expression: it runs on all rows anyway.
RpcCalls takeOutRpcCalls(
    const std::vector<facebook::axiom::logical_plan::ExprApi>& exprs);

} // namespace ember
