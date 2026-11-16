#include "ember/ai/AiFunctions.h"

#include <mutex>

#include <fmt/format.h>
#include <folly/container/F14Map.h>
#include <folly/coro/BlockingWait.h>
#include <folly/coro/Collect.h>

#include "ember/ai/AiBackend.h"
#include "ember/ai/OutputSchema.h"
#include "velox/core/Expressions.h"
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace ember::ai {

namespace velox = facebook::velox;
namespace core = facebook::velox::core;
namespace exec = facebook::velox::exec;

namespace {

// How much of an answer that does not fit the schema its status quotes.
constexpr size_t kQuotedBytes = 300;

using StringVector = velox::FlatVector<velox::StringView>;

// ai_generate(prompt, endpoint[, output]). With 'output', a typed null whose
// type is resultType(schema), the answers are JSON objects of the schema,
// parsed by Velox's cast from JSON. Not default-null: that null argument
// would null every row.
class GenerateFunction : public exec::VectorFunction {
 public:
  void apply(
      const velox::SelectivityVector& rows,
      std::vector<velox::VectorPtr>& args,
      const velox::TypePtr& outputType,
      exec::EvalCtx& context,
      velox::VectorPtr& result) const override {
    VELOX_USER_CHECK(
        args[1]->isConstantEncoding() && !args[1]->isNullAt(0),
        "{}: the endpoint must be a constant",
        kGenerateFunction);
    const auto name = args[1]
                          ->as<velox::ConstantVector<velox::StringView>>()
                          ->valueAt(0)
                          .str();
    auto& backend = backendFor(AiEndpoint::fromConfig(
        name, context.execCtx()->queryCtx()->queryConfig()));
    const auto schema =
        args.size() == 3 ? schemaOf(outputType->asRow()) : nullptr;

    // One call per distinct prompt; a null prompt gives a null row.
    exec::DecodedArgs decoded(rows, {args[0]}, context);
    const auto* prompts = decoded.at(0);
    folly::F14FastMap<velox::StringView, int32_t> calls;
    std::vector<int32_t> callOfRow(rows.end(), -1);
    // The tasks read their prompts when they run. A short StringView holds
    // its characters inline, so the prompts stay here, never reallocated.
    std::vector<velox::StringView> distinct;
    distinct.reserve(rows.countSelected());
    std::vector<folly::coro::Task<AiAnswer>> tasks;
    rows.applyToSelected([&](velox::vector_size_t row) {
      if (prompts->isNullAt(row)) {
        return;
      }
      const auto prompt = prompts->valueAt<velox::StringView>(row);
      auto [it, inserted] = calls.try_emplace(prompt, distinct.size());
      if (inserted) {
        const auto& stable = distinct.emplace_back(prompt);
        tasks.push_back(backend.generate(
            std::string_view(stable.data(), stable.size()), schema));
      }
      callOfRow[row] = it->second;
    });
    auto answers =
        folly::coro::blockingWait(folly::coro::collectAllRange(std::move(tasks)));

    // The answers' text (JSON with a schema), and their statuses.
    auto* pool = context.pool();
    auto texts = velox::BaseVector::create<StringVector>(
        schema == nullptr ? velox::VARCHAR() : velox::JSON(), rows.end(), pool);
    auto statuses = velox::BaseVector::create<StringVector>(
        velox::VARCHAR(), rows.end(), pool);
    velox::SelectivityVector answered(rows.end(), false);
    rows.applyToSelected([&](velox::vector_size_t row) {
      statuses->set(row, velox::StringView());
      if (callOfRow[row] < 0 || answers[callOfRow[row]].text == nullptr) {
        texts->setNull(row, true);
        if (callOfRow[row] >= 0) {
          statuses->set(
              row, velox::StringView(answers[callOfRow[row]].error));
        }
        return;
      }
      const auto& text = answers[callOfRow[row]].text;
      texts->set(row, velox::StringView(text->as<char>(), text->size()));
      answered.setValid(row, true);
    });
    answered.updateBounds();

    std::vector<velox::VectorPtr> fields;
    if (schema == nullptr) {
      fields.push_back(texts);
    } else {
      fields = parse(texts, schema, answered, *statuses, context)->children();
    }
    fields.push_back(statuses);
    auto output = std::make_shared<velox::RowVector>(
        pool, outputType, nullptr, rows.end(), std::move(fields));
    rows.applyToSelected([&](velox::vector_size_t row) {
      if (callOfRow[row] < 0) {
        output->setNull(row, true);
      }
    });
    context.moveOrCopyResult(output, rows, result);
  }

 private:
  // The 'answered' rows of 'texts' cast to 'schema'. Other rows, and answers
  // that do not fit, have null fields; the latter get a status.
  static velox::RowVectorPtr parse(
      const velox::VectorPtr& texts,
      const velox::RowTypePtr& schema,
      const velox::SelectivityVector& answered,
      StringVector& statuses,
      exec::EvalCtx& context) {
    auto* pool = context.pool();
    auto fields = velox::BaseVector::create<velox::RowVector>(
        schema, texts->size(), pool);
    for (const auto& child : fields->children()) {
      child->resize(texts->size());
      for (velox::vector_size_t row = 0; row < texts->size(); ++row) {
        child->setNull(row, true);
      }
    }
    if (!answered.hasSelections()) {
      return fields;
    }

    const auto input = std::make_shared<velox::RowVector>(
        pool,
        velox::ROW({"answer"}, {velox::JSON()}),
        nullptr,
        texts->size(),
        std::vector<velox::VectorPtr>{texts});
    exec::ExprSet cast(
        {std::make_shared<core::CastTypedExpr>(
            schema,
            std::make_shared<core::FieldAccessTypedExpr>(
                velox::JSON(), "answer"),
            /*isTryCast=*/true)},
        context.execCtx());
    exec::EvalCtx castContext(context.execCtx(), &cast, input.get());
    std::vector<velox::VectorPtr> parsed(1);
    cast.eval(answered, castContext, parsed);

    velox::DecodedVector decoded(*parsed[0], answered);
    velox::SelectivityVector fit(texts->size(), false);
    const auto* answers = texts->as<StringVector>();
    answered.applyToSelected([&](velox::vector_size_t row) {
      if (decoded.isNullAt(row)) {
        const auto text = answers->valueAt(row);
        const auto status = fmt::format(
            "Answer does not fit the output schema: {}",
            std::string_view(text.data(), text.size()).substr(0, kQuotedBytes));
        statuses.set(row, velox::StringView(status));
      } else {
        fit.setValid(row, true);
      }
    });
    fit.updateBounds();
    fields->copy(parsed[0].get(), fit, nullptr);
    return fields;
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
             .build(),
         exec::FunctionSignatureBuilder()
             .typeVariable("T")
             .returnType("T")
             .argumentType("varchar")
             .constantArgumentType("varchar")
             .argumentType("T")
             .build()},
        std::make_unique<GenerateFunction>(),
        exec::VectorFunctionMetadataBuilder()
            .deterministic(false)
            .defaultNullBehavior(false)
            .build());
  });
}

} // namespace ember::ai
