// The "stoke" connector, in builds with stoke: stoke streams as tables that
// Ember writes training batches into through Axiom's table write.
//
// A stream <stream> is the table <stream> (any schema) with one VARBINARY
// column "batch": each row's value is one batch in stoke's format (e.g.
// stoke_pack's output). Axiom plans the write as a Velox TableWriter in the
// fragment that produces the rows, so every task of that stage writes its
// own rows: its StokeDataSink frames them into chunks of about
// kStokeChunkBytes in the stream's Kiln bucket and puts each chunk into the
// stream as soon as it is written, while the task still runs. Only a row of
// write results per task goes on to the final task, and
// StokeConnectorMetadata::finishWrite adds up the rows written.
//
// A retried task writes the same bytes, hence the same chunks, which the
// stream drops; a task that fails leaves the chunks it put. The stream must
// exist (stoke create, or stoke's create_stream); a write seals it when
// committed only with kStokeSeal set.
//
// Session properties of the connector (Axiom ConnectorProperties["stoke"]):
// kStokeKilnMetadata (required), kStokeChunkBytes, kStokeSeal.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "axiom/connectors/ConnectorMetadata.h"
#include "velox/connectors/Connector.h"

namespace ember {

inline constexpr std::string_view kStokeConnectorId = "stoke";

/// Session property: the Kiln cluster of the streams (its metadata server).
inline constexpr std::string_view kStokeKilnMetadata = "kiln_metadata";
/// Session property: bytes after which a chunk is put; default 16 MiB.
inline constexpr std::string_view kStokeChunkBytes = "chunk_bytes";
/// Session property: "true" to seal the stream when the write commits.
inline constexpr std::string_view kStokeSeal = "seal";

inline constexpr uint64_t kDefaultStokeChunkBytes = 16 << 20;

/// What a write's tasks need: the stream and how to cut its chunks.
class StokeInsertTableHandle
    : public facebook::velox::connector::ConnectorInsertTableHandle {
 public:
  StokeInsertTableHandle(
      std::string stream,
      std::string kilnMetadata,
      uint64_t chunkBytes);

  const std::string& stream() const {
    return stream_;
  }

  const std::string& kilnMetadata() const {
    return kilnMetadata_;
  }

  uint64_t chunkBytes() const {
    return chunkBytes_;
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
