// The "ember_output" connector: a dataframe's rows written as files in a
// directory, through Axiom's table write (df.write().parquet(directory)).
//
// The target table is the directory: DataframeWriter makes it with the
// dataframe's schema (makeOutputTable()) and plans a CREATE TABLE AS of it,
// so Axiom puts a Velox TableWriter into the fragment that makes the rows.
// Every task of that stage writes its own rows to part-NNNNN.<format>, with
// NNNNN its partition (taskPartition()), encoded by Velox's writer for the
// format. After a range-partitioned sort, the parts in name order hold the
// sorted rows. A task without rows writes no part.
//
// Where the bytes go is chosen by the directory, behind Velox's WriteFile:
// - A local or shared file system: the task writes a hidden temporary file
//   and renames it to its part when done, so a failed or retried attempt
//   leaves no partial part. The directory may be on each worker's own disk.
//   finishWrite writes an empty _SUCCESS into it; an aborted write removes
//   the parts the driver sees there (all of them on a shared file system).
// - kiln://<metadata>/data/<dataset> (KilnFiles.h): the task writes a chunk
//   (KilnWriteFile) into a bucket beginWrite made for the write, and
//   finishWrite publishes the chunks as the dataset's parts, so the dataset
//   appears with all of them or not at all. An aborted write drops the
//   bucket.
//
// Session properties of the connector (Axiom ConnectorProperties
// ["ember_output"]): kOutputFormat (required), and Ember's kKilnFilesTier
// and kKilnFilesWriteThrough for Kiln, under their Ember config names.

#pragma once

#include <map>
#include <memory>
#include <string>
#include <string_view>

#include "axiom/connectors/ConnectorMetadata.h"
#include "kiln/common/Types.h"
#include "velox/connectors/Connector.h"

namespace ember {

inline constexpr std::string_view kOutputConnectorId = "ember_output";

/// Session property: the format of the parts: "parquet".
inline constexpr std::string_view kOutputFormat = "format";

/// What a write's tasks need: where the parts go and how to encode them.
class OutputInsertTableHandle
    : public facebook::velox::connector::ConnectorInsertTableHandle {
 public:
  /// 'kilnConfig' holds the Kiln placement keys of KilnFiles.h, for
  /// kilnFilesPlacement(); 'kilnBucket' the bucket of a kiln:// write.
  OutputInsertTableHandle(
      std::string directory,
      std::string format,
      std::map<std::string, std::string> kilnConfig,
      kiln::BucketId kilnBucket);

  const std::string& directory() const {
    return directory_;
  }

  const std::string& format() const {
    return format_;
  }

  const std::map<std::string, std::string>& kilnConfig() const {
    return kilnConfig_;
  }

  kiln::BucketId kilnBucket() const {
    return kilnBucket_;
  }

  std::string toString() const override;

  folly::dynamic serialize() const override;

  static std::shared_ptr<OutputInsertTableHandle> create(
      const folly::dynamic& obj);

  static void registerSerDe();

 private:
  const std::string directory_;
  const std::string format_;
  const std::map<std::string, std::string> kilnConfig_;
  const kiln::BucketId kilnBucket_;
};

/// Write only: its data sinks write part files.
class OutputConnector : public facebook::velox::connector::Connector {
 public:
  OutputConnector() : Connector(std::string(kOutputConnectorId)) {}

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

/// The begin and end of writes. Output tables exist only as the target of
/// a write (makeOutputTable()), so none is found by name.
class OutputConnectorMetadata
    : public facebook::axiom::connector::ConnectorMetadata {
 public:
  facebook::axiom::connector::TablePtr findTable(
      const facebook::axiom::SchemaTableName& /*tableName*/) override {
    return nullptr;
  }

  facebook::axiom::connector::ConnectorSplitManager* splitManager() override {
    return nullptr;
  }

  /// Checks that the output does not exist yet; for Kiln, makes the bucket
  /// of its chunks.
  facebook::axiom::connector::ConnectorWriteHandlePtr beginWrite(
      const facebook::axiom::connector::ConnectorSessionPtr& session,
      const facebook::axiom::connector::TablePtr& table,
      facebook::axiom::connector::WriteKind kind,
      const facebook::velox::connector::ConnectorTableHandlePtr& scanHandle,
      bool explain) override;

  /// Adds up the rows written. Writes _SUCCESS into a local or shared
  /// directory; for Kiln, publishes the dataset.
  facebook::axiom::connector::RowsFuture finishWrite(
      const facebook::axiom::connector::ConnectorSessionPtr& session,
      const facebook::axiom::connector::ConnectorWriteHandlePtr& handle,
      const std::vector<facebook::velox::RowVectorPtr>& writeResults,
      facebook::velox::RowVectorPtr groupingKeys,
      std::vector<std::vector<facebook::axiom::connector::ColumnStatistics>>
          groupStats) override;

  /// Removes the parts it sees in a local or shared directory; for Kiln,
  /// drops the bucket with the chunks written.
  facebook::velox::ContinueFuture abortWrite(
      const facebook::axiom::connector::ConnectorSessionPtr& session,
      const facebook::axiom::connector::ConnectorWriteHandlePtr&
          handle) noexcept override;

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
};

/// The table to plan a write of 'type' into 'directory' (a local, shared or
/// kiln:// dataset path) as, named 'directory' in schema "default".
facebook::axiom::connector::TablePtr makeOutputTable(
    const std::string& directory,
    const facebook::velox::RowTypePtr& type);

/// Registers the connector and Velox's Parquet writer. Called by
/// initializeRuntime().
void registerOutputConnector();

} // namespace ember
