// The "ember_output" connector (OutputConnector.h).

#include "ember/OutputConnector.h"

#include <folly/coro/BlockingWait.h>
#include <folly/json.h>

#include "axiom/connectors/ConnectorMetadataRegistry.h"
#include "ember/GensortFileHandler.h"
#include "ember/KilnFiles.h"
#include "ember/KilnWriteFile.h"
#include "ember/TaskId.h"
#include "ember/shuffle/KilnShuffle.h"
#include "kiln/catalog/Catalog.h"
#include "kiln/client/Client.h"
#include "velox/common/file/FileSystems.h"
#include "velox/core/TableWriteTraits.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/WriterFactory.h"
#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/vector/DecodedVector.h"

namespace ember {

namespace velox = facebook::velox;
namespace axiom_connector = facebook::axiom::connector;
using folly::coro::blockingWait;

namespace {

constexpr std::string_view kParquet = "parquet";

// Written into a local or shared directory when its write commits. Directory
// reads skip names starting with '_'.
constexpr std::string_view kSuccess = "_SUCCESS";

void checkFormat(const std::string& format) {
  VELOX_USER_CHECK(
      format == kParquet || format == kGensortSchema,
      "Unsupported output format: {}",
      format);
}

// gensort parts have no suffix: valsort takes any name.
std::string partName(int32_t partition, const std::string& format) {
  return format == kGensortSchema
      ? fmt::format("part-{:05d}", partition)
      : fmt::format("part-{:05d}.{}", partition, format);
}

// Hidden from directory reads, which skip names starting with '.'.
std::string tempName(const std::string& part, const std::string& taskId) {
  auto name = fmt::format(".{}.{}", part, taskId);
  for (auto& c : name) {
    if (c == '/' || c == ':') {
      c = '_';
    }
  }
  return name;
}

// Hands Velox's file writer, which owns its WriteFile, a file the sink keeps
// too: the sink reads the size and the Kiln chunk after the writer closed.
class SharedWriteFile : public velox::WriteFile {
 public:
  explicit SharedWriteFile(std::shared_ptr<velox::WriteFile> file)
      : file_{std::move(file)} {}

  void append(std::string_view data) override {
    file_->append(data);
  }

  void flush() override {
    file_->flush();
  }

  void close() override {
    file_->close();
  }

  uint64_t size() const override {
    return file_->size();
  }

 private:
  const std::shared_ptr<velox::WriteFile> file_;
};

// Writes each row's key and payload as a gensort record, as valsort reads
// them.
class GensortWriter : public velox::dwio::common::Writer {
 public:
  GensortWriter(const velox::RowType& type, velox::WriteFile& file)
      : file_{file} {
    VELOX_USER_CHECK(
        type.size() == 2 && type.childAt(0)->isVarbinary() &&
            type.childAt(1)->isVarbinary(),
        "gensort output needs a key and a payload column, both VARBINARY: {}",
        type.toString());
    setState(State::kRunning);
  }

  void write(const velox::VectorPtr& data) override {
    const auto& rows = *data->asChecked<velox::RowVector>();
    velox::DecodedVector keys(*rows.childAt(0));
    velox::DecodedVector payloads(*rows.childAt(1));
    buffer_.clear();
    buffer_.reserve(rows.size() * kGensortRecordSize);
    for (velox::vector_size_t i = 0; i < rows.size(); ++i) {
      VELOX_USER_CHECK(
          !keys.isNullAt(i) && !payloads.isNullAt(i),
          "gensort records cannot be null");
      const auto key = keys.valueAt<velox::StringView>(i);
      const auto payload = payloads.valueAt<velox::StringView>(i);
      VELOX_USER_CHECK_EQ(
          key.size() + payload.size(),
          kGensortRecordSize,
          "gensort records are {} bytes",
          kGensortRecordSize);
      buffer_.append(key.data(), key.size());
      buffer_.append(payload.data(), payload.size());
    }
    file_.append(buffer_);
  }

  void flush() override {}

  bool finish() override {
    return true;
  }

  std::unique_ptr<velox::dwio::common::FileMetadata> close() override {
    file_.close();
    setState(State::kClosed);
    return nullptr;
  }

  void abort() override {
    setState(State::kAborted);
  }

 private:
  velox::WriteFile& file_;
  std::string buffer_;
};

// Writes a task's rows to its part with the writer for the format. The
// part is opened on the first rows, and done in finish(); close() reports
// it to finishWrite as one fragment: name, rows, bytes and Kiln chunk.
class OutputDataSink : public velox::connector::DataSink {
 public:
  OutputDataSink(
      velox::RowTypePtr type,
      std::shared_ptr<const OutputInsertTableHandle> handle,
      std::string taskId,
      velox::memory::MemoryPool& pool)
      : type_{std::move(type)},
        handle_{std::move(handle)},
        taskId_{std::move(taskId)},
        pool_{pool} {}

  ~OutputDataSink() override {
    if (!done_) {
      removePart();
    }
  }

  void appendData(velox::RowVectorPtr input) override {
    if (input->size() == 0) {
      return;
    }
    if (writer_ == nullptr) {
      open();
    }
    writer_->write(input);
    rows_ += input->size();
  }

  bool finish() override {
    if (writer_ == nullptr || done_) {
      return true;
    }
    writer_->close();
    writer_.reset();
    if (kilnFile_ == nullptr) {
      fileSystem_->rename(tempPath_, path_, /*overwrite=*/true);
    }
    done_ = true;
    return true;
  }

  std::vector<std::string> close() override {
    if (!done_) {
      return {};
    }
    folly::dynamic part = folly::dynamic::object;
    part["name"] = name_;
    part["rows"] = rows_;
    part["bytes"] = static_cast<int64_t>(file_->size());
    part["chunk"] = kilnFile_ != nullptr ? kilnFile_->chunk() : "";
    return {folly::toJson(part)};
  }

  void abort() override {
    if (writer_ != nullptr) {
      writer_->abort();
      writer_.reset();
    }
    removePart();
    done_ = false;
  }

  Stats stats() const override {
    return Stats{
        .numWrittenBytes = file_ != nullptr ? file_->size() : 0,
        .numWrittenFiles = done_ ? 1u : 0u};
  }

 private:
  void open() {
    name_ = partName(taskPartition(taskId_), handle_->format());
    if (isKilnPath(handle_->directory())) {
      kilnFile_ = std::make_shared<KilnWriteFile>(
          parseKilnPath(handle_->directory()).metadata,
          handle_->kilnBucket(),
          kilnFilesPlacement(handle_->kilnConfig()));
      file_ = kilnFile_;
    } else {
      path_ = fmt::format("{}/{}", handle_->directory(), name_);
      tempPath_ =
          fmt::format("{}/{}", handle_->directory(), tempName(name_, taskId_));
      fileSystem_ =
          velox::filesystems::getFileSystem(handle_->directory(), nullptr);
      fileSystem_->mkdir(handle_->directory());
      file_ = fileSystem_->openFileForWrite(tempPath_);
    }
    if (handle_->format() == kGensortSchema) {
      writer_ = std::make_unique<GensortWriter>(*type_, *file_);
      return;
    }
    auto options = std::make_shared<velox::dwio::common::WriterOptions>();
    options->schema = type_;
    options->memoryPool = &pool_;
    // As other engines write Parquet by default.
    options->compressionKind = velox::common::CompressionKind_SNAPPY;
    writer_ = velox::dwio::common::getWriterFactory(
                  velox::dwio::common::FileFormat::PARQUET)
                  ->createWriter(
                      std::make_unique<velox::dwio::common::WriteFileSink>(
                          std::make_unique<SharedWriteFile>(file_),
                          path_.empty() ? name_ : path_),
                      options);
  }

  // Drops what the task wrote: the temporary file, or the Kiln chunk, which
  // KilnWriteFile drops unless it was committed.
  void removePart() {
    kilnFile_.reset();
    file_.reset();
    if (fileSystem_ == nullptr || tempPath_.empty()) {
      return;
    }
    try {
      if (fileSystem_->exists(tempPath_)) {
        fileSystem_->remove(tempPath_);
      }
    } catch (const std::exception& e) {
      LOG(WARNING) << "Could not remove " << tempPath_ << ": " << e.what();
    }
  }

  const velox::RowTypePtr type_;
  const std::shared_ptr<const OutputInsertTableHandle> handle_;
  const std::string taskId_;
  // The connector's pool of the TableWriter operator, which outlives it.
  velox::memory::MemoryPool& pool_;

  std::string name_;
  std::string path_;
  std::string tempPath_;
  std::shared_ptr<velox::filesystems::FileSystem> fileSystem_;
  std::shared_ptr<KilnWriteFile> kilnFile_;
  std::shared_ptr<velox::WriteFile> file_;
  std::unique_ptr<velox::dwio::common::Writer> writer_;
  int64_t rows_{0};
  bool done_{false};
};

// Output tables are only written: the layout has no scans.
class OutputTableLayout : public axiom_connector::TableLayout {
 public:
  OutputTableLayout(
      const axiom_connector::Table* table,
      velox::connector::Connector* connector,
      std::vector<const axiom_connector::Column*> columns)
      : TableLayout(
            std::string(kOutputConnectorId),
            table,
            connector,
            std::move(columns),
            /*partitionColumns=*/{},
            /*orderColumns=*/{},
            /*sortOrder=*/{},
            /*lookupKeys=*/{},
            /*supportsScan=*/false) {}

  velox::connector::ColumnHandlePtr createColumnHandle(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      const std::string& /*columnName*/,
      std::vector<velox::common::Subfield> /*subfields*/,
      std::optional<velox::TypePtr> /*castToType*/,
      axiom_connector::SubfieldMapping /*subfieldMapping*/) const override {
    VELOX_UNSUPPORTED("Written files are read with read()");
  }

  velox::connector::ConnectorTableHandlePtr createTableHandle(
      const axiom_connector::ConnectorSessionPtr& /*session*/,
      std::vector<velox::connector::ColumnHandlePtr> /*columnHandles*/,
      velox::core::ExpressionEvaluator& /*evaluator*/,
      std::vector<velox::core::TypedExprPtr> /*filters*/,
      std::vector<int32_t>& /*rejectedFilterIndices*/,
      velox::RowTypePtr /*dataColumns*/,
      std::optional<axiom_connector::LookupKeys> /*lookupKeys*/)
      const override {
    VELOX_UNSUPPORTED("Written files are read with read()");
  }
};

class OutputTable : public axiom_connector::Table {
 public:
  OutputTable(
      facebook::axiom::SchemaTableName name,
      const velox::RowTypePtr& type,
      velox::connector::Connector* connector)
      : Table(std::move(name), makeColumns(type)),
        layout_{this, connector, allColumns()},
        layouts_{&layout_} {}

  const std::vector<const axiom_connector::TableLayout*>& layouts()
      const override {
    return layouts_;
  }

  std::optional<uint64_t> numRows() const override {
    return std::nullopt;
  }

 private:
  const OutputTableLayout layout_;
  const std::vector<const axiom_connector::TableLayout*> layouts_;
};

std::optional<std::string> property(
    const axiom_connector::ConnectorSessionPtr& session,
    std::string_view name) {
  if (session == nullptr) {
    return std::nullopt;
  }
  auto value = session->property(name);
  if (!value.has_value()) {
    return std::nullopt;
  }
  return std::string(*value);
}

// Manifests go where the parts go.
kiln::catalog::Catalog kilnCatalog(
    const std::string& metadata,
    const std::map<std::string, std::string>& kilnConfig) {
  return kiln::catalog::Catalog(
      *shuffle::kilnClient(metadata),
      kiln::catalog::CatalogOptions{
          .placement = kilnFilesPlacement(kilnConfig),
          .buffers = shuffle::kilnStagingPool(metadata)});
}

void deleteKilnBucket(const OutputInsertTableHandle& handle) {
  try {
    const auto metadata = parseKilnPath(handle.directory()).metadata;
    blockingWait(
        shuffle::kilnClient(metadata)->co_deleteBucket(handle.kilnBucket()));
  } catch (const std::exception& e) {
    LOG(WARNING) << "Cannot delete Kiln bucket " << handle.kilnBucket()
                 << ": " << e.what();
  }
}

struct WrittenPart {
  int64_t rows{0};
  int64_t bytes{0};
  std::string chunk;
};

} // namespace

OutputInsertTableHandle::OutputInsertTableHandle(
    std::string directory,
    std::string format,
    std::map<std::string, std::string> kilnConfig,
    kiln::BucketId kilnBucket)
    : directory_{std::move(directory)},
      format_{std::move(format)},
      kilnConfig_{std::move(kilnConfig)},
      kilnBucket_{kilnBucket} {}

std::string OutputInsertTableHandle::toString() const {
  return fmt::format("{} files in {}", format_, directory_);
}

folly::dynamic OutputInsertTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "OutputInsertTableHandle";
  obj["directory"] = directory_;
  obj["format"] = format_;
  folly::dynamic kilnConfig = folly::dynamic::object;
  for (const auto& [key, value] : kilnConfig_) {
    kilnConfig[key] = value;
  }
  obj["kilnConfig"] = std::move(kilnConfig);
  obj["kilnBucket"] = static_cast<int64_t>(kilnBucket_);
  return obj;
}

std::shared_ptr<OutputInsertTableHandle> OutputInsertTableHandle::create(
    const folly::dynamic& obj) {
  std::map<std::string, std::string> kilnConfig;
  for (const auto& [key, value] : obj["kilnConfig"].items()) {
    kilnConfig[key.asString()] = value.asString();
  }
  return std::make_shared<OutputInsertTableHandle>(
      obj["directory"].asString(),
      obj["format"].asString(),
      std::move(kilnConfig),
      static_cast<kiln::BucketId>(obj["kilnBucket"].asInt()));
}

void OutputInsertTableHandle::registerSerDe() {
  velox::DeserializationRegistryForSharedPtr().Register(
      "OutputInsertTableHandle", OutputInsertTableHandle::create);
}

std::unique_ptr<velox::connector::DataSource> OutputConnector::createDataSource(
    const velox::RowTypePtr& /*outputType*/,
    const velox::connector::ConnectorTableHandlePtr& /*tableHandle*/,
    const velox::connector::ColumnHandleMap& /*columnHandles*/,
    velox::connector::ConnectorQueryCtx* /*connectorQueryCtx*/) {
  VELOX_UNSUPPORTED("Written files are read with read()");
}

std::unique_ptr<velox::connector::DataSink> OutputConnector::createDataSink(
    velox::RowTypePtr inputType,
    velox::connector::ConnectorInsertTableHandlePtr handle,
    velox::connector::ConnectorQueryCtx* connectorQueryCtx,
    velox::connector::CommitStrategy /*commitStrategy*/) {
  handle->asChecked<OutputInsertTableHandle>();
  return std::make_unique<OutputDataSink>(
      std::move(inputType),
      std::static_pointer_cast<const OutputInsertTableHandle>(handle),
      connectorQueryCtx->taskId(),
      *connectorQueryCtx->connectorMemoryPool());
}

axiom_connector::ConnectorWriteHandlePtr OutputConnectorMetadata::beginWrite(
    const axiom_connector::ConnectorSessionPtr& session,
    const axiom_connector::TablePtr& table,
    axiom_connector::WriteKind kind,
    const velox::connector::ConnectorTableHandlePtr& /*scanHandle*/,
    bool explain) {
  VELOX_USER_CHECK(
      kind == axiom_connector::WriteKind::kCreate,
      "Output files are only created");
  const auto& directory = table->name().table;
  auto format = property(session, kOutputFormat);
  VELOX_USER_CHECK(format.has_value(), "No output format");
  checkFormat(*format);

  std::map<std::string, std::string> kilnConfig;
  kiln::BucketId bucket = kiln::kDefaultBucket;
  if (isKilnPath(directory)) {
    for (auto key : {kKilnFilesTier, kKilnFilesWriteThrough}) {
      if (auto value = property(session, key)) {
        kilnConfig[std::string(key)] = *value;
      }
    }
    if (!explain) {
      const auto path = parseKilnPath(directory);
      VELOX_USER_CHECK(
          !blockingWait(kilnCatalog(path.metadata, kilnConfig)
                            .co_getDataset(path.dataset))
               .has_value(),
          "Kiln dataset {} exists already",
          directory);
      bucket = blockingWait(
          shuffle::kilnClient(path.metadata)->co_createBucket());
    }
  }
  return std::make_shared<axiom_connector::ConnectorWriteHandle>(
      std::make_shared<OutputInsertTableHandle>(
          directory, std::move(*format), std::move(kilnConfig), bucket),
      velox::core::TableWriteTraits::outputType(std::nullopt));
}

axiom_connector::RowsFuture OutputConnectorMetadata::finishWrite(
    const axiom_connector::ConnectorSessionPtr& /*session*/,
    const axiom_connector::ConnectorWriteHandlePtr& handle,
    const std::vector<velox::RowVectorPtr>& writeResults,
    velox::RowVectorPtr /*groupingKeys*/,
    std::vector<std::vector<axiom_connector::ColumnStatistics>> /*groupStats*/) {
  const auto& insert =
      *handle->veloxHandle()->asChecked<OutputInsertTableHandle>();
  // By name, which is part order; a part written twice is counted once.
  std::map<std::string, WrittenPart> parts;
  int64_t rows = 0;
  velox::DecodedVector rowCounts;
  velox::DecodedVector fragments;
  for (const auto& result : writeResults) {
    rowCounts.decode(
        *result->childAt(velox::core::TableWriteTraits::kRowCountChannel));
    fragments.decode(
        *result->childAt(velox::core::TableWriteTraits::kFragmentChannel));
    for (velox::vector_size_t i = 0; i < result->size(); ++i) {
      if (!rowCounts.isNullAt(i)) {
        rows += rowCounts.valueAt<int64_t>(i);
      }
      if (fragments.isNullAt(i)) {
        continue;
      }
      const auto part =
          folly::parseJson(fragments.valueAt<velox::StringView>(i).str());
      parts[part["name"].asString()] = WrittenPart{
          .rows = part["rows"].asInt(),
          .bytes = part["bytes"].asInt(),
          .chunk = part["chunk"].asString()};
    }
  }
  if (!isKilnPath(insert.directory())) {
    // As other engines do: readers tell complete output by the marker.
    auto fileSystem =
        velox::filesystems::getFileSystem(insert.directory(), nullptr);
    fileSystem->mkdir(insert.directory());
    fileSystem
        ->openFileForWrite(fmt::format("{}/{}", insert.directory(), kSuccess))
        ->close();
    return folly::makeSemiFuture(std::optional<int64_t>{rows});
  }

  const auto path = parseKilnPath(insert.directory());
  kiln::catalog::Manifest manifest;
  manifest.schema = insert.format();
  manifest.bucket = insert.kilnBucket();
  for (const auto& [name, part] : parts) {
    manifest.rows += part.rows;
    manifest.bytes += part.bytes;
    manifest.chunks.push_back(kiln::catalog::ChunkRange{
        .id = kiln::ChunkId::parse(part.chunk),
        .length = static_cast<uint64_t>(part.bytes),
        .name = name,
        .rows = static_cast<uint64_t>(part.rows)});
  }
  std::optional<uint64_t> version;
  try {
    version = blockingWait(kilnCatalog(path.metadata, insert.kilnConfig())
                               .co_publishDataset(path.dataset, manifest));
  } catch (...) {
    deleteKilnBucket(insert);
    throw;
  }
  if (!version.has_value()) {
    deleteKilnBucket(insert);
    VELOX_USER_FAIL(
        "Kiln dataset {} was published while the job ran", insert.directory());
  }
  return folly::makeSemiFuture(std::optional<int64_t>{rows});
}

velox::ContinueFuture OutputConnectorMetadata::abortWrite(
    const axiom_connector::ConnectorSessionPtr& /*session*/,
    const axiom_connector::ConnectorWriteHandlePtr& handle) noexcept {
  const auto& insert =
      *handle->veloxHandle()->asChecked<OutputInsertTableHandle>();
  if (isKilnPath(insert.directory())) {
    if (insert.kilnBucket() != kiln::kDefaultBucket) {
      deleteKilnBucket(insert);
    }
    return {};
  }
  // The parts that tasks finished. Parts on workers' own disks are not
  // seen here and stay.
  try {
    auto fileSystem =
        velox::filesystems::getFileSystem(insert.directory(), nullptr);
    if (!fileSystem->exists(insert.directory())) {
      return {};
    }
    for (const auto& path : fileSystem->list(insert.directory())) {
      const auto slash = path.rfind('/');
      const auto name =
          slash == std::string::npos ? path : path.substr(slash + 1);
      if (name.starts_with("part-")) {
        fileSystem->remove(path);
      }
    }
  } catch (const std::exception& e) {
    LOG(WARNING) << "Could not remove the parts in " << insert.directory()
                 << ": " << e.what();
  }
  return {};
}

std::vector<std::string> OutputConnectorMetadata::listSchemaNames(
    const axiom_connector::ConnectorSessionPtr& /*session*/) {
  return {};
}

bool OutputConnectorMetadata::schemaExists(
    const axiom_connector::ConnectorSessionPtr& /*session*/,
    const std::string& /*schemaName*/) {
  return true;
}

std::vector<std::string> OutputConnectorMetadata::listTableNames(
    const axiom_connector::ConnectorSessionPtr& /*session*/,
    const std::string& /*schemaName*/) {
  VELOX_UNSUPPORTED("Output files are listed with the file system");
}

axiom_connector::TablePtr makeOutputTable(
    const std::string& directory,
    const velox::RowTypePtr& type) {
  return std::make_shared<OutputTable>(
      facebook::axiom::SchemaTableName{"default", directory},
      type,
      velox::connector::getConnector(std::string(kOutputConnectorId)).get());
}

void registerOutputConnector() {
  velox::parquet::registerParquetWriterFactory();
  OutputInsertTableHandle::registerSerDe();
  auto connector = std::make_shared<OutputConnector>();
  velox::connector::registerConnector(connector);
  axiom_connector::ConnectorMetadataRegistry::global().insert(
      connector->connectorId(),
      std::make_shared<OutputConnectorMetadata>());
}

} // namespace ember
