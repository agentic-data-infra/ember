#include "ember/Session.h"

#include <folly/Singleton.h>
#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/system/HardwareConcurrency.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "axiom/connectors/SchemaResolver.h"
#include "axiom/connectors/file/FileConnector.h"
#include "axiom/connectors/file/core/FileConnectorMetadata.h"
#include "axiom/connectors/file/parquet/ParquetFileHandler.h"
#include "axiom/optimizer/OptimizerSession.h"
#include "axiom/optimizer/QueryGraphContext.h"
#include "axiom/optimizer/v2/Optimize.h"
#include "axiom/runner/LocalRunner.h"
#include "ember/FunctionRegistry.h"
#include "ember/TextFileHandler.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/HashStringAllocator.h"
#include "velox/exec/Exchange.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/expression/Expr.h"
#include "velox/functions/prestosql/coercion/PrestoCoercions.h"
#include "velox/serializers/PrestoSerializer.h"

namespace ember {

namespace lp = facebook::axiom::logical_plan;
namespace optimizer = facebook::axiom::optimizer;
namespace runner = facebook::axiom::runner;
namespace axiom_connector = facebook::axiom::connector;
namespace velox = facebook::velox;

namespace {

// Process-wide setup, shared by all sessions.
void initializeProcess() {
  static std::once_flag once;
  std::call_once(once, [] {
    // Ember programs need not call folly::Init; this is the part Velox needs.
    folly::SingletonVault::singleton()->registrationComplete();
    if (!velox::memory::MemoryManager::testInstance()) {
      velox::memory::initializeMemoryManager(
          velox::memory::MemoryManager::Options{});
    }
    velox::filesystems::registerLocalFileSystem();
    registerFunctions();

    // Exchanges between fragments run in-process until Ember's shuffle
    // replaces them.
    if (!velox::isRegisteredNamedVectorSerde("Presto")) {
      velox::serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
    }
    velox::exec::ExchangeSource::registerFactory(
        velox::exec::test::createLocalExchangeSource);

    axiom_connector::file::registerParquetHandler();
    registerTextFileHandler();
    auto connector = std::make_shared<axiom_connector::file::FileConnector>(
        SessionState::fileConnectorId());
    velox::connector::registerConnector(connector);
    axiom_connector::ConnectorMetadataRegistry::global().insert(
        connector->connectorId(),
        std::make_shared<axiom_connector::file::FileConnectorMetadata>(
            connector.get()));
  });
}

int32_t parseParallelism(const std::string& master) {
  if (master == "local") {
    return 1;
  }
  const std::string prefix = "local[";
  VELOX_USER_CHECK(
      master.starts_with(prefix) && master.ends_with("]"),
      "Unsupported master: {}. Use local, local[N] or local[*].",
      master);
  auto inner = master.substr(prefix.size(), master.size() - prefix.size() - 1);
  if (inner == "*") {
    return static_cast<int32_t>(folly::available_concurrency());
  }
  auto parallelism = folly::tryTo<int32_t>(inner);
  VELOX_USER_CHECK(
      parallelism.hasValue() && *parallelism > 0,
      "Invalid parallelism in master: {}",
      master);
  return *parallelism;
}

std::mutex activeMutex;
std::shared_ptr<SessionState> activeSession;

} // namespace

SessionState::SessionState(
    std::string appName,
    std::string master,
    std::map<std::string, std::string> config)
    : appName_{std::move(appName)},
      master_{std::move(master)},
      config_{std::move(config)},
      parallelism_{parseParallelism(master_)} {
  initializeProcess();
  // Drivers are cooperative, but leave headroom for blocked ones.
  executor_ = std::make_unique<folly::CPUThreadPoolExecutor>(
      std::max(4, 2 * parallelism_));
  rootPool_ = velox::memory::memoryManager()->addRootPool(
      fmt::format("ember_{}", appName_));
  resultPool_ = rootPool_->addLeafChild("results");
  optimizerPool_ = rootPool_->addLeafChild("optimizer");
  functionHook_ = functionRewriteHook();
}

SessionState::~SessionState() {
  stop();
}

const std::string& SessionState::fileConnectorId() {
  static const std::string kId = "ember_file";
  return kId;
}

lp::PlanBuilder::Context SessionState::planContext() const {
  lp::PlanBuilder::Context context{fileConnectorId()};
  context.hook = functionHook_;
  // Implicit casts, e.g. bigint_column > 60 compares as bigint.
  context.coercer = &velox::functions::prestosql::typeCoercer();
  return context;
}

std::string SessionState::nextQueryId() {
  return fmt::format("{}_{}", appName_, ++queryCounter_);
}

namespace {

struct QueryEnv {
  std::shared_ptr<velox::core::QueryCtx> queryCtx;
  axiom_connector::ConnectorContextPtr connectorContext;
  std::shared_ptr<velox::BaseRuntimeStatWriter> statsWriter;
};

QueryEnv makeQueryEnv(
    const std::string& queryId,
    const std::string& user,
    folly::Executor* executor,
    const std::map<std::string, std::string>& config) {
  QueryEnv env;
  env.queryCtx = velox::core::QueryCtx::create(
      executor,
      velox::core::QueryConfig(
          std::unordered_map<std::string, std::string>(
              config.begin(), config.end())),
      /*connectorConfigs=*/{},
      velox::cache::AsyncDataCache::getInstance(),
      /*pool=*/nullptr,
      /*spillExecutor=*/nullptr,
      queryId);
  env.connectorContext = std::make_shared<axiom_connector::ConnectorContext>(
      queryId,
      user,
      axiom_connector::ConnectorProperties{},
      axiom_connector::ConnectorContext::noopStatWriterProvider());
  env.statsWriter = std::make_shared<velox::NoopRuntimeStatWriter>();
  return env;
}

// 'pool' must outlive the returned plan, which may hold vectors allocated
// from it (e.g. folded constants).
optimizer::PlanAndStats optimizePlan(
    const lp::LogicalPlanNodePtr& plan,
    const QueryEnv& env,
    int32_t parallelism,
    const std::shared_ptr<velox::memory::MemoryPool>& pool) {
  velox::HashStringAllocator allocator(pool.get());
  optimizer::QueryGraphContext context(
      allocator, optimizer::OptimizerOptions::kMaxPlanObjectsDefault);
  optimizer::queryCtx() = &context;
  SCOPE_EXIT {
    optimizer::queryCtx() = nullptr;
  };
  velox::exec::SimpleExpressionEvaluator evaluator(
      env.queryCtx.get(), pool.get());
  axiom_connector::SchemaResolver schemaResolver{
      axiom_connector::ConnectorMetadataRegistry::global()};
  optimizer::OptimizerSession session(
      env.connectorContext,
      env.statsWriter,
      axiom_connector::Properties{},
      optimizer::OptimizerOptions{});

  optimizer::MultiFragmentPlan::Options options;
  options.queryId = env.queryCtx->queryId();
  // One task per core for each stage, single-threaded.
  options.maxRemotePartitions = parallelism;
  options.maxLocalPartitions = 1;
  return optimizer::v2::Optimizer(
             *plan, schemaResolver, session, evaluator, env.queryCtx)
      .optimize(options);
}

} // namespace

optimizer::PlanAndStats SessionState::optimize(
    const lp::LogicalPlanNodePtr& plan) {
  auto env = makeQueryEnv(
      nextQueryId(), appName_, executor_.get(), config_);
  return optimizePlan(plan, env, parallelism_, optimizerPool_);
}

std::vector<velox::RowVectorPtr> SessionState::execute(
    const lp::LogicalPlanNodePtr& plan) {
  VELOX_USER_CHECK_NOT_NULL(executor_, "Session {} is stopped", appName_);
  auto env = makeQueryEnv(
      nextQueryId(), appName_, executor_.get(), config_);
  auto planAndStats = optimizePlan(plan, env, parallelism_, optimizerPool_);

  auto localRunner = std::make_shared<runner::LocalRunner>(
      std::make_shared<runner::RunnerSession>(
          env.connectorContext, env.statsWriter, runner::Properties{}),
      planAndStats.plan,
      std::move(planAndStats.finishWrite),
      env.queryCtx,
      std::make_shared<runner::ConnectorSplitSourceFactory>(),
      env.queryCtx->pool()->addLeafChild("runner"),
      /*baseSpillDirectory=*/"");

  std::vector<velox::RowVectorPtr> results;
  localRunner->drain([&](velox::RowVectorPtr batch) {
    // Copy out of the runner's pools, which go away with the runner.
    results.push_back(std::dynamic_pointer_cast<velox::RowVector>(
        velox::BaseVector::copy(*batch, resultPool_.get())));
  });
  return results;
}

void SessionState::stop() {
  if (executor_ != nullptr) {
    executor_->join();
    executor_.reset();
  }
}

Session::Builder& Session::Builder::appName(std::string name) {
  appName_ = std::move(name);
  return *this;
}

Session::Builder& Session::Builder::master(std::string master) {
  master_ = std::move(master);
  return *this;
}

Session::Builder& Session::Builder::config(
    const std::string& key,
    std::string value) {
  config_[key] = std::move(value);
  return *this;
}

Session Session::Builder::getOrCreate() {
  std::lock_guard<std::mutex> l(activeMutex);
  if (activeSession == nullptr) {
    activeSession = std::make_shared<SessionState>(appName_, master_, config_);
  }
  return Session(activeSession);
}

Session::Builder Session::builder() {
  return Builder{};
}

std::optional<Session> Session::active() {
  std::lock_guard<std::mutex> l(activeMutex);
  if (activeSession == nullptr) {
    return std::nullopt;
  }
  return Session(activeSession);
}

DataframeReader Session::read() const {
  return DataframeReader(state_);
}

Dataframe Session::createDataframe(
    const velox::RowTypePtr& type,
    std::vector<std::vector<velox::Variant>> rows) const {
  std::vector<velox::Variant> values;
  values.reserve(rows.size());
  for (auto& row : rows) {
    values.push_back(velox::Variant::row(std::move(row)));
  }
  return Dataframe(
      state_,
      std::make_shared<const PlanStep>(PlanStep{
          nullptr,
          [type, values = std::move(values)](
              lp::PlanBuilder& builder, const auto& /*context*/) {
            builder.values(type, values);
          }}));
}

Dataframe Session::createDataframe(
    const std::vector<velox::RowVectorPtr>& data) const {
  return Dataframe(
      state_,
      std::make_shared<const PlanStep>(PlanStep{
          nullptr, [data](lp::PlanBuilder& builder, const auto& /*context*/) {
            builder.values(data);
          }}));
}

void Session::stop() {
  std::lock_guard<std::mutex> l(activeMutex);
  state_->stop();
  if (activeSession == state_) {
    activeSession.reset();
  }
}

} // namespace ember
