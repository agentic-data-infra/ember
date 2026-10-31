// The "stoke" connector (StokeConnector.h), in builds with stoke: Velox
// TableWriter plans writing batches into a stream, and the Axiom metadata
// that begins and finishes writes; against an in-process Kiln cluster and
// stoke shards.

#include <gtest/gtest.h>

#include <folly/coro/BlockingWait.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "ember/Stoke.h"
#include "ember/StokeConnector.h"
#include "ember/shuffle/KilnShuffle.h"
#include "kiln/local/LocalCluster.h"
#include "stoke/client/Registry.h"
#include "stoke/client/StreamClient.h"
#include "stoke/format/Batch.h"
#include "stoke/loader/Loader.h"
#include "stoke/rpc/Rpc.h"
#include "stoke/shard/Shard.h"
#include "velox/core/TableWriteTraits.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace ember {
namespace {

namespace velox = facebook::velox;
namespace axiom_connector = facebook::axiom::connector;
using folly::coro::blockingWait;

constexpr std::string_view kStream = "train";

class StokeConnectorTest : public velox::exec::test::OperatorTestBase {
 protected:
  static void SetUpTestCase() {
    OperatorTestBase::SetUpTestCase();
    registerStokeConnector();
    cluster_ = new kiln::local::LocalCluster(kiln::local::LocalClusterOptions{
        .dramServers = 1, .localFsServers = 0});
  }

  static void TearDownTestCase() {
    delete cluster_;
    cluster_ = nullptr;
    OperatorTestBase::TearDownTestCase();
  }

  void SetUp() override {
    OperatorTestBase::SetUp();
    registry_ =
        std::make_shared<stoke::Registry>(shuffle::kilnClient(metadata()));
    std::vector<std::string> endpoints;
    for (int i = 0; i < 2; ++i) {
      auto shard = std::make_shared<stoke::Shard>(
          registry_,
          stoke::ShardOptions{.refreshInterval = std::chrono::milliseconds(20)});
      servers_.push_back(stoke::rpc::serveShard(shard, "127.0.0.1", 0));
      endpoints.push_back(
          "127.0.0.1:" + std::to_string(servers_.back()->port()));
    }
    blockingWait(registry_->co_create(std::string(kStream), endpoints, 1.0));
  }

  void TearDown() override {
    blockingWait(registry_->co_remove(std::string(kStream)));
    servers_.clear();
    OperatorTestBase::TearDown();
  }

  static std::string metadata() {
    return cluster_->metadataAddress().toString();
  }

  // 'count' batches of 'tokens' int32s each, batch i holding the value i.
  std::vector<std::string> makeBatches(int32_t count, int32_t tokens) {
    std::vector<std::string> batches;
    for (int32_t i = 0; i < count; ++i) {
      std::vector<int32_t> values(tokens, i);
      batches.push_back(stoke::format::encodeBatch(
          {.dtype = stoke::format::DType::kInt32,
           .shape = {1, static_cast<uint64_t>(tokens)}},
          {reinterpret_cast<const uint8_t*>(values.data()),
           values.size() * sizeof(int32_t)}));
    }
    return batches;
  }

  // Writes 'batches' (NULL for nullopt) into the stream through a
  // TableWriter, in chunks of 'chunkBytes'; returns the write's result rows.
  velox::RowVectorPtr write(
      const std::vector<std::optional<std::string>>& batches,
      uint64_t chunkBytes) {
    std::vector<std::optional<velox::StringView>> values;
    for (const auto& batch : batches) {
      values.push_back(
          batch.has_value() ? std::optional(velox::StringView(*batch))
                            : std::nullopt);
    }
    auto input = makeRowVector(
        {"batch"},
        {makeNullableFlatVector<velox::StringView>(values, velox::VARBINARY())});
    auto handle = std::make_shared<velox::core::InsertTableHandle>(
        std::string(kStokeConnectorId),
        std::make_shared<StokeInsertTableHandle>(
            std::string(kStream), metadata(), chunkBytes),
        /*notNullColumns=*/folly::F14FastSet<std::string>{});
    auto source = velox::exec::test::PlanBuilder().values({input}).planNode();
    auto plan = std::make_shared<velox::core::TableWriteNode>(
        "write",
        asRowType(input->type()),
        std::vector<std::string>{"batch"},
        std::nullopt,
        handle,
        /*hasPartitioningScheme=*/false,
        velox::core::TableWriteTraits::outputType(std::nullopt),
        velox::connector::CommitStrategy::kNoCommit,
        source);
    return velox::exec::test::AssertQueryBuilder(plan).copyResults(pool());
  }

  // The stream's shard stats, added up.
  stoke::ShardStats stats() {
    auto stream = blockingWait(
        stoke::StreamClient::co_open(registry_, std::string(kStream)));
    stoke::ShardStats total;
    for (const auto& shard : blockingWait(stream->co_stats())) {
      total.put += shard.put;
      total.duplicates += shard.duplicates;
    }
    return total;
  }

  // Seals the stream and reads every batch of it, as its bytes.
  std::multiset<std::string> consume() {
    blockingWait(registry_->co_seal(std::string(kStream)));
    stoke::Loader loader(
        blockingWait(
            stoke::StreamClient::co_open(registry_, std::string(kStream))),
        shuffle::kilnClient(metadata()),
        stoke::LoaderOptions{
            .epoch = {"rank0", 0}, .wait = std::chrono::milliseconds(20)});
    std::multiset<std::string> batches;
    while (auto batch = loader.next()) {
      batches.insert(
          stoke::format::encodeBatch(batch->view.header, batch->view.payload));
    }
    return batches;
  }

  std::shared_ptr<axiom_connector::ConnectorSession> session(
      axiom_connector::Properties properties) {
    return std::make_shared<axiom_connector::ConnectorSession>(
        "query",
        "user",
        std::move(properties),
        std::make_shared<velox::NoopRuntimeStatWriter>());
  }

  static axiom_connector::ConnectorMetadata& connectorMetadata() {
    return *axiom_connector::ConnectorMetadataRegistry::get(
        std::string(kStokeConnectorId));
  }

  static kiln::local::LocalCluster* cluster_;
  std::shared_ptr<stoke::Registry> registry_;
  std::vector<std::unique_ptr<stoke::rpc::RpcServer>> servers_;
};

kiln::local::LocalCluster* StokeConnectorTest::cluster_ = nullptr;

TEST_F(StokeConnectorTest, writesBatchesInChunksOfChunkBytes) {
  // 10 batches of 64 + 400 bytes plus an 8-byte frame each; a chunk is put
  // once it reaches 1000 bytes, i.e. every 3 batches, and the last one at
  // the end.
  auto batches = makeBatches(10, 100);
  auto result = write({batches.begin(), batches.end()}, 1000);
  EXPECT_EQ(
      result->childAt(velox::core::TableWriteTraits::kRowCountChannel)
          ->as<velox::SimpleVector<int64_t>>()
          ->valueAt(0),
      10);
  EXPECT_EQ(stats().put, 4);
  EXPECT_EQ(consume(), std::multiset<std::string>(batches.begin(), batches.end()));
}

TEST_F(StokeConnectorTest, skipsNullRows) {
  auto batches = makeBatches(2, 10);
  write({batches[0], std::nullopt, batches[1], std::nullopt}, 1 << 20);
  EXPECT_EQ(stats().put, 1);
  EXPECT_EQ(consume(), std::multiset<std::string>(batches.begin(), batches.end()));
}

TEST_F(StokeConnectorTest, writesNoChunkWithoutBatches) {
  write({std::nullopt, std::nullopt}, 1 << 20);
  EXPECT_EQ(stats().put, 0);
  EXPECT_TRUE(consume().empty());
}

TEST_F(StokeConnectorTest, dropsTheChunksOfARetriedWrite) {
  auto batches = makeBatches(6, 100);
  write({batches.begin(), batches.end()}, 1000);
  write({batches.begin(), batches.end()}, 1000);
  auto total = stats();
  EXPECT_EQ(total.put, 2);
  EXPECT_EQ(total.duplicates, 2);
  EXPECT_EQ(consume(), std::multiset<std::string>(batches.begin(), batches.end()));
}

TEST_F(StokeConnectorTest, serializesItsInsertHandle) {
  StokeInsertTableHandle handle("train", "localhost:7470", 4096);
  auto copy = StokeInsertTableHandle::create(handle.serialize());
  EXPECT_EQ(copy->stream(), "train");
  EXPECT_EQ(copy->kilnMetadata(), "localhost:7470");
  EXPECT_EQ(copy->chunkBytes(), 4096);
}

TEST_F(StokeConnectorTest, beginsWritesIntoOpenStreams) {
  auto& metadata = connectorMetadata();
  auto table = metadata.findTable({"default", std::string(kStream)});
  EXPECT_EQ(table->type()->toString(), "ROW<batch:VARBINARY>");

  auto handle = metadata.beginWrite(
      session({{std::string(kStokeKilnMetadata), this->metadata()},
               {std::string(kStokeChunkBytes), "4096"}}),
      table,
      axiom_connector::WriteKind::kInsert,
      nullptr,
      /*explain=*/false);
  const auto* insert =
      handle->veloxHandle()->asChecked<StokeInsertTableHandle>();
  EXPECT_EQ(insert->stream(), kStream);
  EXPECT_EQ(insert->kilnMetadata(), this->metadata());
  EXPECT_EQ(insert->chunkBytes(), 4096);

  // No Kiln cluster.
  EXPECT_ANY_THROW(metadata.beginWrite(
      session({}), table, axiom_connector::WriteKind::kInsert, nullptr, false));
  // No such stream.
  EXPECT_ANY_THROW(metadata.beginWrite(
      session({{std::string(kStokeKilnMetadata), this->metadata()}}),
      metadata.findTable({"default", "missing"}),
      axiom_connector::WriteKind::kInsert,
      nullptr,
      false));
  // A sealed stream.
  blockingWait(registry_->co_seal(std::string(kStream)));
  EXPECT_ANY_THROW(metadata.beginWrite(
      session({{std::string(kStokeKilnMetadata), this->metadata()}}),
      table,
      axiom_connector::WriteKind::kInsert,
      nullptr,
      false));
}

TEST_F(StokeConnectorTest, finishesWritesWithTheirRowCount) {
  auto& metadata = connectorMetadata();
  auto table = metadata.findTable({"default", std::string(kStream)});
  auto batches = makeBatches(3, 10);
  auto result = write({batches.begin(), batches.end()}, 1 << 20);

  auto open = session({{std::string(kStokeKilnMetadata), this->metadata()}});
  auto handle = metadata.beginWrite(
      open, table, axiom_connector::WriteKind::kInsert, nullptr, false);
  EXPECT_EQ(
      metadata.finishWrite(open, handle, {result, result}, nullptr, {}).get(),
      6);
  EXPECT_EQ(
      blockingWait(registry_->co_get(std::string(kStream)))->state,
      stoke::StreamState::kOpen);

  auto seal = session(
      {{std::string(kStokeKilnMetadata), this->metadata()},
       {std::string(kStokeSeal), "true"}});
  EXPECT_EQ(metadata.finishWrite(seal, handle, {result}, nullptr, {}).get(), 3);
  EXPECT_EQ(
      blockingWait(registry_->co_get(std::string(kStream)))->state,
      stoke::StreamState::kSealed);
}

} // namespace
} // namespace ember
