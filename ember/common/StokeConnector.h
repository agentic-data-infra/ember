// The "stoke" connector, in builds with stoke: stoke streams as tables that
// Ember writes training batches into through Axiom's table write.
//
// A stream <stream> is the table <stream> (any schema) with two columns, of
// which a write gives one: "batch", VARBINARY, each value one batch in
// stoke's format (e.g. stoke_pack's output); or "tokens", ARRAY<INTEGER>,
// which the sink packs (kStokePackBatch, kStokePackSequence): it
// concatenates the tokens of its rows in order and cuts them into int32
// batches of [B, T + 1] windows, as stoke_pack does for one row. A task's
// last batch holds its whole windows left, and fewer tokens than a window
// are dropped. Axiom plans the write as a Velox TableWriter in the
// fragment that produces the rows, so every task of that stage writes its
// own rows: its StokeDataSink frames them into chunks of about
// kStokeChunkBytes in the stream's Kiln bucket and puts each chunk into the
// stream as soon as it is written, while the task still runs. Only a row of
// write results per task goes on to the final task, and
// StokeConnectorMetadata::finishWrite adds up the rows written.
//
// With a stream task budget (stoke create --task_budget N), each sink has
// at most N chunks put and not yet consumed: once it has, its next chunk
// waits in appendData until a trainer consumed the oldest (stoke::Producer),
// and the task ends only once all of its chunks were consumed, so a worker
// has at most slots x N unconsumed. Ember just sees a slow write; the job
// runs at its trainers' pace.
//
// A retried task writes the same bytes, hence the same chunks, which the
// stream drops; a task that fails leaves the chunks it put. The stream must
// exist (stoke create, or stoke's create_stream); a write seals it when
// committed only with kStokeSeal set.
//
// Session properties of the connector (Axiom ConnectorProperties["stoke"]):
// kStokeKilnMetadata (required), kStokeChunkBytes, kStokeSeal, and for
// "tokens" kStokePackBatch and kStokePackSequence (both required).

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "axiom/connectors/ConnectorMetadata.h"
#include "ember/Stoke.h"
#include "velox/connectors/Connector.h"

namespace ember {

inline constexpr std::string_view kStokeConnectorId = "stoke";

/// Session property: the Kiln cluster of the streams (its metadata server).
inline constexpr std::string_view kStokeKilnMetadata = "kiln_metadata";
/// Session property: bytes after which a chunk is put; default 16 MiB.
inline constexpr std::string_view kStokeChunkBytes = "chunk_bytes";
/// Session property: "true" to seal the stream when the write commits.
inline constexpr std::string_view kStokeSeal = "seal";

/// Session properties of a write of "tokens": B and T of its batches.
inline constexpr std::string_view kStokePackBatch = "pack_batch";
inline constexpr std::string_view kStokePackSequence = "pack_sequence";

inline constexpr uint64_t kDefaultStokeChunkBytes = 16 << 20;

/// What a write's tasks need: the stream and how to cut its chunks.
class StokeInsertTableHandle
    : public facebook::velox::connector::ConnectorInsertTableHandle {
 public:
  StokeInsertTableHandle(
      std::string stream,
      std::string kilnMetadata,
      uint64_t chunkBytes,
      std::optional<StokePacking> packing);

  const std::string& stream() const {
    return stream_;
  }

  const std::string& kilnMetadata() const {
    return kilnMetadata_;
  }

  uint64_t chunkBytes() const {
    return chunkBytes_;
  }

  /// Set for a write of "tokens".
  const std::optional<StokePacking>& packing() const {
    return packing_;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static std::shared_ptr<StokeInsertTableHandle> create(
      const folly::dynamic& obj);

  static void registerSerDe();

 private:
  const std::string stream_;
  const std::string kilnMetadata_;
  const uint64_t chunkBytes_;
  const std::optional<StokePacking> packing_;
};

/// Write only: its data sinks write into streams.
class StokeConnector : public facebook::velox::connector::Connector {
 public:
  StokeConnector() : Connector(std::string(kStokeConnectorId)) {}

  std::unique_ptr<facebook::velox::connector::DataSource> createDataSource(
      const facebook::velox::RowTypePtr& outputType,
      const facebook::velox::connector::ConnectorTableHandlePtr& tableHandle,
      const facebook::velox::connector::ColumnHandleMap& columnHandles,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx)
      override;

  std::unique_ptr<facebook::velox::connector::DataSink> createDataSink(
      facebook::velox::RowTypePtr inputType,
      facebook::velox::connector::ConnectorInsertTableHandlePtr handle,
      facebook::velox::connector::ConnectorQueryCtx* connectorQueryCtx,
      facebook::velox::connector::CommitStrategy commitStrategy) override;
};

/// Streams as Axiom tables, and the begin and end of writes into them.
class StokeConnectorMetadata
    : public facebook::axiom::connector::ConnectorMetadata {
 public:
  explicit StokeConnectorMetadata(
      facebook::velox::connector::Connector* connector)
      : connector_{connector} {}

  /// Any name is a stream; beginWrite checks that it exists.
  facebook::axiom::connector::TablePtr findTable(
      const facebook::axiom::SchemaTableName& tableName) override;

  /// Streams are not split for scans.
  facebook::axiom::connector::ConnectorSplitManager* splitManager() override {
    return nullptr;
  }

  facebook::axiom::connector::ConnectorWriteHandlePtr beginWrite(
      const facebook::axiom::connector::ConnectorSessionPtr& session,
      const facebook::axiom::connector::TablePtr& table,
      facebook::axiom::connector::WriteKind kind,
      const facebook::velox::connector::ConnectorTableHandlePtr& scanHandle,
      bool explain) override;

  facebook::axiom::connector::RowsFuture finishWrite(
      const facebook::axiom::connector::ConnectorSessionPtr& session,
      const facebook::axiom::connector::ConnectorWriteHandlePtr& handle,
      const std::vector<facebook::velox::RowVectorPtr>& writeResults,
      facebook::velox::RowVectorPtr groupingKeys,
      std::vector<std::vector<facebook::axiom::connector::ColumnStatistics>>
          groupStats) override;

  std::vector<std::string> listSchemaNames(
      const facebook::axiom::connector::ConnectorSessionPtr& session) override;

  bool schemaExists(
      const facebook::axiom::connector::ConnectorSessionPtr& session,
      const std::string& schemaName) override;

  bool listTableNamesSupported() const override {
    return false;
  }

  std::vector<std::string> listTableNames(
      const facebook::axiom::connector::ConnectorSessionPtr& session,
      const std::string& schemaName) override;

 private:
  facebook::velox::connector::Connector* const connector_;
};

} // namespace ember
