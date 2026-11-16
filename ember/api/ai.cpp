#include "ember/ai.h"

#include "ember/ai/AiFunctions.h"
#include "ember/ai/OutputSchema.h"

namespace ember::ai {

namespace lp = facebook::axiom::logical_plan;
namespace velox = facebook::velox;

Column generate(const Column& prompt, const std::string& endpoint) {
  return Column(lp::Call(kGenerateFunction, prompt.expr(), lit(endpoint).expr()));
}

Column generate(
    const Column& prompt,
    const std::string& endpoint,
    const velox::RowTypePtr& output) {
  // The result type travels as the type of a null argument.
  auto type = resultType(output);
  return Column(lp::Call(
      kGenerateFunction,
      prompt.expr(),
      lit(endpoint).expr(),
      lp::Lit(velox::Variant::null(velox::TypeKind::ROW), std::move(type))));
}

} // namespace ember::ai
