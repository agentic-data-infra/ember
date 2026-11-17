#include "ember/ai/AiFunctions.h"

#include <algorithm>
#include <mutex>

#include <fmt/format.h>
#include <folly/container/F14Map.h>
#include <folly/executors/InlineExecutor.h>
#include <folly/futures/SharedPromise.h>

#include "ember/ai/AiBackend.h"
#include "ember/ai/OutputSchema.h"
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/expression/rpc/AsyncRPCFunction.h"
#include "velox/expression/rpc/AsyncRPCFunctionRegistry.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace ember::ai {

namespace velox = facebook::velox;
namespace core = facebook::velox::core;
namespace exec = facebook::velox::exec;
namespace rpc = facebook::velox::exec::rpc;

namespace {

// How much of an answer that does not fit the schema its status quotes.
constexpr size_t kQuotedBytes = 300;

using StringVector = velox::FlatVector<velox::StringView>;

// A successful answer's text, as the payload of its response.
struct AnswerPayload {
  velox::BufferPtr text;

  int64_t retainedBytes() const noexcept {
    return text->capacity();
  }
};

// ai_generate(prompt, endpoint[, output]) as an async RPC function: Velox's
// RPC operator dispatches the rows and holds them until their answers
// arrive, without blocking the driver thread. With 'output', a typed null
// whose type is resultType(schema), the answers are JSON objects of the
// schema, parsed by Velox's cast from JSON.
class GenerateFunction : public rpc::AsyncRPCFunction {
 public:
  void initialize(
      const core::QueryConfig& config,
      const std::vector<velox::TypePtr>& inputTypes,
      const std::vector<velox::VectorPtr>& constantInputs,
      rpc::RPCStreamingMode /*mode*/) override {
    const auto& name = constantInputs.at(1);
    VELOX_USER_CHECK(
        name != nullptr && !name->isNullAt(0),
        "{}: the endpoint must be a constant",
        kGenerateFunction);
    endpoint_ = AiEndpoint::fromConfig(
        name->as<velox::ConstantVector<velox::StringView>>()->valueAt(0).str(),
        config);
    backend_ = backendFor(endpoint_);
    if (inputTypes.size() == 3) {
      type_ = inputTypes[2];
      schema_ = schemaOf(type_->asRow());
    }
  }

  std::string name() const override {
    return kGenerateFunction;
  }

  velox::TypePtr resultType() const override {
    return type_;
  }

  bool requiresRowInspectionBeforeAdmission() const override {
    return false;
  }

  // Operators that call one endpoint share its limit of requests in flight.
  std::string admissionKey() const override {
    return endpoint_.key();
  }

  int64_t configuredCeiling() const override {
    return endpoint_.maxConcurrency;
  }

  int64_t numRetriesAttempted() const override {
    return counts_->retries.total;
  }

  // One request per distinct prompt; rows with the same prompt share its
  // answer. A null prompt makes no request.
  std::vector<
      std::pair<velox::vector_size_t, folly::SemiFuture<rpc::RPCResponse>>>
  dispatchPerRow(
      const velox::SelectivityVector& rows,
      const std::vector<velox::VectorPtr>& args) override {
    velox::DecodedVector prompts(*args[0], rows);
    folly::F14FastMap<
        std::string_view,
        std::shared_ptr<folly::SharedPromise<AiAnswer>>>
        calls;
    std::vector<
        std::pair<velox::vector_size_t, folly::SemiFuture<rpc::RPCResponse>>>
        responses;
    responses.reserve(rows.countSelected());
    rows.applyToSelected([&](velox::vector_size_t row) {
      if (prompts.isNullAt(row)) {
        responses.emplace_back(
            row,
            folly::makeSemiFuture(rpc::RPCResponse::failed(
                row, velox::rpc::RPCErrorKind::kNullInput, "")));
        return;
      }
      const auto prompt = prompts.valueAt<velox::StringView>(row);
      auto& answer = calls[std::string_view(prompt.data(), prompt.size())];
      if (answer == nullptr) {
        answer = std::make_shared<folly::SharedPromise<AiAnswer>>();
        ++counts_->calls.total;
        // The answer may arrive after the operator is gone, so the callback
        // holds what it writes.
        backend_
            ->generate(std::string_view(prompt.data(), prompt.size()), schema_)
            .via(&folly::InlineExecutor::instance())
            .thenTry([counts = counts_, answer](folly::Try<AiAnswer> value) {
              if (value.hasException()) {
                value.emplace(AiAnswer{
                    .error = value.exception().what().toStdString()});
              }
              counts->add(*value);
              answer->setValue(std::move(*value));
            });
      }
      responses.emplace_back(
          row, answer->getSemiFuture().deferValue([row](AiAnswer value) {
            return toResponse(row, std::move(value));
          }));
    });
    return responses;
  }

  velox::VectorPtr buildOutput(
      const std::vector<rpc::RPCResponse>& responses,
      velox::memory::MemoryPool* pool) const override {
    const auto size = static_cast<velox::vector_size_t>(responses.size());
    // The answers' text (JSON with a schema), and their statuses.
    auto texts = velox::BaseVector::create<StringVector>(
        schema_ == nullptr ? velox::VARCHAR() : velox::JSON(), size, pool);
    auto statuses =
        velox::BaseVector::create<StringVector>(velox::VARCHAR(), size, pool);
    velox::SelectivityVector answered(size, false);
    for (velox::vector_size_t row = 0; row < size; ++row) {
      const auto& response = responses[row];
      statuses->set(row, velox::StringView());
      if (response.hasError()) {
        texts->setNull(row, true);
        statuses->set(row, velox::StringView(response.error().message));
        continue;
      }
      const auto& text = rpc::responseAs<AnswerPayload>(response).text;
      texts->set(row, velox::StringView(text->as<char>(), text->size()));
      answered.setValid(row, true);
    }
    answered.updateBounds();

    std::vector<velox::VectorPtr> fields;
    if (schema_ == nullptr) {
      fields.push_back(texts);
    } else {
      fields = parse(texts, schema_, answered, *statuses, pool)->children();
    }
    fields.push_back(statuses);
    auto output = std::make_shared<velox::RowVector>(
        pool, type_, nullptr, size, std::move(fields));
    int64_t failed = 0;
    for (velox::vector_size_t row = 0; row < size; ++row) {
      if (responses[row].errorKind() == velox::rpc::RPCErrorKind::kNullInput) {
        output->setNull(row, true);
      } else {
        failed += statuses->valueAt(row).size() > 0;
      }
    }
    reportStats(failed);
    return output;
  }

  CongestionSignal evaluateCongestion(
      const std::vector<rpc::RPCResponse>& responses) const override {
    for (const auto& response : responses) {
      const auto kind = response.errorKind();
      if (kind == velox::rpc::RPCErrorKind::kRateLimited ||
          kind == velox::rpc::RPCErrorKind::kTimeout) {
        return CongestionSignal::kOverloaded;
      }
    }
    return CongestionSignal::kSuccess;
  }

 private:
  static rpc::RPCResponse toResponse(velox::vector_size_t row, AiAnswer answer) {
    if (answer.text != nullptr) {
      rpc::RPCResponse response;
      response.rowId = row;
      response.setPayload(AnswerPayload{std::move(answer.text)});
      return response;
    }
    return rpc::RPCResponse::failed(
        row,
        answer.overloaded ? velox::rpc::RPCErrorKind::kRateLimited
                          : velox::rpc::RPCErrorKind::kBackendError,
        std::move(answer.error));
  }

  // Adds the counts since the last call to the operator's runtime stats. Runs
  // on the driver thread, where they reach the operator.
  void reportStats(int64_t failedRows) const {
    for (const auto& [name, value] :
         {std::pair{kCallsStat, counts_->calls.take()},
          std::pair{kRetriesStat, counts_->retries.take()},
          std::pair{kFailedRowsStat, failedRows},
          std::pair{kInputTokensStat, counts_->inputTokens.take()},
          std::pair{kOutputTokensStat, counts_->outputTokens.take()}}) {
      if (value > 0) {
        velox::addThreadLocalRuntimeStat(name, velox::RuntimeCounter(value));
      }
    }
  }

  // The 'answered' rows of 'texts' cast to 'schema'. Other rows, and answers
  // that do not fit, have null fields; the latter get a status.
  static velox::RowVectorPtr parse(
      const velox::VectorPtr& texts,
      const velox::RowTypePtr& schema,
      const velox::SelectivityVector& answered,
      StringVector& statuses,
      velox::memory::MemoryPool* pool) {
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
    // The cast reads no query config, so one empty query context serves all.
    static const auto queryCtx = core::QueryCtx::create();
    core::ExecCtx execCtx(pool, queryCtx.get());
    exec::ExprSet cast(
        {std::make_shared<core::CastTypedExpr>(
            schema,
            std::make_shared<core::FieldAccessTypedExpr>(
                velox::JSON(), "answer"),
            /*isTryCast=*/true)},
        &execCtx);
    exec::EvalCtx castContext(&execCtx, &cast, input.get());
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

  // A count added up on the backend's threads and reported on the driver
  // thread.
  struct Counter {
    std::atomic<int64_t> total{0};
    int64_t reported{0};

    // The count since the last call.
    int64_t take() {
      const int64_t now = total;
      const auto value = now - reported;
      reported = now;
      return value;
    }
  };

  struct Counts {
    Counter calls;
    Counter retries;
    Counter inputTokens;
    Counter outputTokens;

    // Runs on the backend's thread as each answer arrives.
    void add(const AiAnswer& answer) {
      retries.total += std::max(answer.attempts - 1, 0);
      inputTokens.total += answer.inputTokens;
      outputTokens.total += answer.outputTokens;
    }
  };

  AiEndpoint endpoint_;
  std::shared_ptr<AiBackend> backend_;
  velox::TypePtr type_{velox::ROW(
      {"result", "status"}, {velox::VARCHAR(), velox::VARCHAR()})};
  // The output schema, or null for free text.
  velox::RowTypePtr schema_;
  const std::shared_ptr<Counts> counts_{std::make_shared<Counts>()};
};

} // namespace

void registerAiFunctions() {
  static std::once_flag once;
  std::call_once(once, [] {
    // Deterministic, so stages that call it are reused. Not default-null: the
    // typed null 'output' argument would null every row.
    VELOX_CHECK(rpc::AsyncRPCFunctionRegistry::registerFunction(
        kGenerateFunction,
        [] { return std::make_shared<GenerateFunction>(); },
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
        exec::VectorFunctionMetadataBuilder()
            .deterministic(true)
            .defaultNullBehavior(false)
            .build()));
  });
}

} // namespace ember::ai
