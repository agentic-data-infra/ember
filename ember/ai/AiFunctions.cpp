#include "ember/ai/AiFunctions.h"

#include <mutex>

#include <folly/container/F14Map.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>

#include "ember/ai/AiBackend.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"

namespace ember::ai {

namespace velox = facebook::velox;
namespace exec = facebook::velox::exec;

namespace {

class GenerateFunction : public exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& rows,
      std::vector<velox::VectorPtr>& args,
      const velox::TypePtr& outputType,
      exec::EvalCtx& context,
      velox::VectorPtr& result) const override {
    VELOX_USER_CHECK(
        args[1]->isConstantEncoding(),
        "{}: the endpoint must be a constant",
        kGenerateFunction);
    const auto name = args[1]
                          ->as<velox::ConstantVector<velox::StringView>>()
                          ->valueAt(0)
                          .str();
    auto& backend = backendFor(AiEndpoint::fromConfig(
        name, context.execCtx()->queryCtx()->queryConfig()));

    // One call per distinct prompt.
    exec::DecodedArgs decoded(rows, {args[0]}, context);
    const auto* prompts = decoded.at(0);
    folly::F14FastMap<velox::StringView, int32_t> calls;
    std::vector<int32_t> callOfRow(rows.end());
    // The tasks read their prompts when they run. A short StringView holds
    // its characters inline, so the prompts stay here, never reallocated.
    std::vector<velox::StringView> distinct;
    distinct.reserve(rows.countSelected());
    std::vector<folly::coro::Task<AiAnswer>> tasks;
    rows.applyToSelected([&](velox::vector_size_t row) {
      const auto prompt = prompts->valueAt<velox::StringView>(row);
      auto [it, inserted] = calls.try_emplace(prompt, distinct.size());
      if (inserted) {
        const auto& stable = distinct.emplace_back(prompt);
        tasks.push_back(
            backend.generate(std::string_view(stable.data(), stable.size())));
      }
      callOfRow[row] = it->second;
    });
    auto answers =
        folly::coro::blockingWait(folly::coro::collectAllRange(std::move(tasks)));

    auto* pool = context.pool();
    auto texts = velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
        velox::VARCHAR(), rows.end(), pool);
    auto statuses =
        velox::BaseVector::create<velox::FlatVector<velox::StringView>>(
            velox::VARCHAR(), rows.end(), pool);
    rows.applyToSelected([&](velox::vector_size_t row) {
      const auto& answer = answers[callOfRow[row]];
      if (answer.text != nullptr) {
        texts->set(
            row,
            velox::StringView(answer.text->as<char>(), answer.text->size()));
        statuses->set(row, velox::StringView());
      } else {
        texts->setNull(row, true);
        statuses->set(row, velox::StringView(answer.error));
      }
    });
    auto output = std::make_shared<velox::RowVector>(
        pool,
        outputType,
        nullptr,
        rows.end(),
        std::vector<velox::VectorPtr>{std::move(texts), std::move(statuses)});
    context.moveOrCopyResult(output, rows, result);
  }
};

} // namespace

void registerAiFunctions() {
  static std::once_flag once;
  std::call_once(once, [] {
    exec::registerVectorFunction(
        kGenerateFunction,
        {exec::FunctionSignatureBuilder()
             .returnType("row(result varchar, status varchar)")
             .argumentType("varchar")
             .constantArgumentType("varchar")
             .build()},
        std::make_unique<GenerateFunction>(),
        exec::VectorFunctionMetadataBuilder().deterministic(false).build());
  });
}

} // namespace ember::ai
