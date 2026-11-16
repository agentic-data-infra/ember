#include "ember/ai.h"

#include "ember/ai/AiFunctions.h"

namespace ember::ai {

namespace lp = facebook::axiom::logical_plan;

Column generate(const Column& prompt, const std::string& endpoint) {
  return Column(lp::Call(kGenerateFunction, prompt.expr(), lit(endpoint).expr()));
}

} // namespace ember::ai
