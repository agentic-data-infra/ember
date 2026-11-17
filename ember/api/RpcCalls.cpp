#include "ember/RpcCalls.h"

#include <optional>
#include <unordered_map>

#include "velox/expression/ExprConstants.h"
#include "velox/expression/rpc/AsyncRPCFunctionRegistry.h"
#include "velox/parse/Expressions.h"

namespace ember {

namespace lp = facebook::axiom::logical_plan;
namespace core = facebook::velox::core;
namespace expression = facebook::velox::expression;

namespace {

const core::CallExpr* asCall(const core::IExpr& expr) {
  return expr.is(core::IExpr::Kind::kCall) ? expr.as<core::CallExpr>()
                                           : nullptr;
}

bool isRpcCall(const core::IExpr& expr) {
  const auto* call = asCall(expr);
  return call != nullptr &&
      facebook::velox::exec::rpc::AsyncRPCFunctionRegistry::isRegistered(
          call->name());
}

// Lambdas and subqueries have their own inputs; calls inside them stay.
bool hasOwnInputs(const core::IExpr& expr) {
  return expr.is(core::IExpr::Kind::kLambda) ||
      expr.is(core::IExpr::Kind::kSubquery);
}

// An expression and the number of levels it reads (0: only the input).
struct Leveled {
  core::ExprPtr expr;
  size_t level{0};
};

std::optional<Leveled> both(
    const std::optional<Leveled>& left,
    const std::optional<Leveled>& right) {
  if (!left.has_value()) {
    return right;
  }
  if (!right.has_value()) {
    return left;
  }
  return Leveled{
      lp::Call(expression::kAnd, left->expr, right->expr).expr(),
      std::max(left->level, right->level)};
}

Leveled call(const char* name, const Leveled& input, lp::ExprApi other) {
  return {lp::Call(name, input.expr, std::move(other)).expr(), input.level};
}

Leveled call(const char* name, const Leveled& input) {
  return {lp::Call(name, input.expr).expr(), input.level};
}

class TakeOut {
 public:
  explicit TakeOut(RpcCalls& calls) : calls_{calls} {}

  // 'guard' is null where every row reaches 'expr'.
  Leveled rewrite(
      const core::ExprPtr& expr,
      const std::optional<Leveled>& guard,
      bool whole) {
    if (!callsRpc(expr)) {
      return {expr};
    }
    const auto& inputs = expr->inputs();
    std::vector<core::ExprPtr> rewritten;
    rewritten.reserve(inputs.size());
    size_t level = 0;
    auto add = [&](const core::ExprPtr& input,
                   const std::optional<Leveled>& inputGuard) {
      auto result = rewrite(input, inputGuard, false);
      rewritten.push_back(result.expr);
      level = std::max(level, result.level);
      return result;
    };

    const auto* callExpr = asCall(*expr);
    const std::string name = callExpr != nullptr ? callExpr->name() : "";
    if (name == expression::kSwitch || name == expression::kIf) {
      // Some earlier condition holds.
      std::optional<Leveled> matched;
      size_t i = 0;
      for (; i + 1 < inputs.size(); i += 2) {
        const auto reached = matched.has_value()
            ? both(guard, call("not", *matched))
            : guard;
        const auto holds =
            call(expression::kCoalesce, add(inputs[i], reached), lp::Lit(false));
        add(inputs[i + 1], both(reached, holds));
        matched = matched.has_value()
            ? Leveled{lp::Call(expression::kOr, matched->expr, holds.expr)
                          .expr(),
                      std::max(matched->level, holds.level)}
            : holds;
      }
      if (i < inputs.size()) {
        add(inputs[i], both(guard, call("not", *matched)));
      }
    } else if (
        name == expression::kAnd || name == expression::kOr ||
        name == expression::kCoalesce) {
      auto reached = guard;
      for (const auto& input : inputs) {
        const auto result = add(input, reached);
        reached = both(
            reached,
            name == expression::kAnd
                ? call(expression::kCoalesce, result, lp::Lit(true))
                : name == expression::kOr
                ? call("not", call(expression::kCoalesce, result, lp::Lit(false)))
                : call("is_null", result));
      }
    } else {
      for (const auto& input : inputs) {
        add(input, guard);
      }
    }

    auto result = expr->replaceInputs(std::move(rewritten));
    if (!isRpcCall(*expr) || whole) {
      return {std::move(result), level};
    }
    return takeOut(result, guard, level);
  }

 private:
  // Computes 'rpcCall' in a level of its own, with its first input set to
  // null outside 'guard', and returns its column.
  Leveled takeOut(
      const core::ExprPtr& rpcCall,
      const std::optional<Leveled>& guard,
      size_t level) {
    const auto alias = rpcCall->alias();
    auto unguarded = rpcCall->dropAlias();
    // The same call made on all rows covers these rows too.
    if (auto it = columns_.find(unguarded->toString()); it != columns_.end()) {
      return withAlias(it->second, alias);
    }
    auto inputs = unguarded->inputs();
    if (guard.has_value()) {
      // A switch without an else is null where the condition does not hold.
      inputs[0] = lp::Call(expression::kSwitch, guard->expr, inputs[0]).expr();
      level = std::max(level, guard->level);
    }
    auto unaliased = unguarded->replaceInputs(std::move(inputs));
    auto [it, added] = columns_.try_emplace(unaliased->toString());
    if (added) {
      const auto column = fmt::format("__rpc{}", columns_.size() - 1);
      if (calls_.levels.size() <= level) {
        calls_.levels.resize(level + 1);
      }
      calls_.levels[level].emplace_back(unaliased, column);
      it->second = Leveled{lp::Col(column).expr(), level + 1};
    }
    return withAlias(it->second, alias);
  }

  static Leveled withAlias(
      Leveled column,
      const std::optional<std::string>& alias) {
    if (alias.has_value()) {
      column.expr = column.expr->withAlias(*alias);
    }
    return column;
  }

  RpcCalls& calls_;
  // Columns of the calls taken out, by call.
  std::unordered_map<std::string, Leveled> columns_;
};

} // namespace

bool callsRpc(const core::ExprPtr& expr) {
  if (isRpcCall(*expr)) {
    return true;
  }
  if (hasOwnInputs(*expr)) {
    return false;
  }
  for (const auto& input : expr->inputs()) {
    if (callsRpc(input)) {
      return true;
    }
  }
  return false;
}

RpcCalls takeOutRpcCalls(const std::vector<lp::ExprApi>& exprs) {
  RpcCalls calls;
  TakeOut takeOut(calls);
  for (const auto& expr : exprs) {
    calls.exprs.push_back(
        callsRpc(expr.expr())
            ? lp::ExprApi(
                  takeOut.rewrite(expr.expr(), std::nullopt, true).expr,
                  expr.alias())
            : expr);
  }
  return calls;
}

} // namespace ember
