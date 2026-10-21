#include "ember/FileSerde.h"

#include <mutex>

#include "velox/common/serialization/Serializable.h"

namespace ember {

namespace file = facebook::axiom::connector::file;
namespace velox = facebook::velox;

std::shared_ptr<const SerializableFileTableHandle>
SerializableFileTableHandle::from(const file::FileTableHandle& handle) {
  std::vector<velox::connector::ColumnHandlePtr> columns;
  for (const auto& column : handle.columnHandles()) {
    columns.push_back(
        std::make_shared<SerializableFileColumnHandle>(column->name()));
  }
  return std::make_shared<SerializableFileTableHandle>(
      handle.connectorId(),
      handle.schemaTableName(),
      handle.filePath(),
      handle.filePaths(),
      handle.suffix(),
      handle.fullSchema(),
      std::move(columns));
}

folly::dynamic SerializableFileTableHandle::serialize() const {
  auto obj = serializeBase("SerializableFileTableHandle");
  obj["schema"] = schemaTableName().schema;
  obj["table"] = schemaTableName().table;
  obj["filePath"] = filePath();
  folly::dynamic paths = folly::dynamic::array;
  for (const auto& path : filePaths()) {
    paths.push_back(path);
  }
  obj["filePaths"] = std::move(paths);
  obj["suffix"] = suffix();
  obj["fullSchema"] = fullSchema()->serialize();
  folly::dynamic columns = folly::dynamic::array;
  for (const auto& column : columnHandles()) {
    columns.push_back(column->name());
  }
  obj["columns"] = std::move(columns);
  return obj;
}

std::shared_ptr<SerializableFileTableHandle>
SerializableFileTableHandle::create(const folly::dynamic& obj, void* context) {
  std::vector<std::string> paths;
  for (const auto& path : obj["filePaths"]) {
    paths.push_back(path.asString());
  }
  std::vector<velox::connector::ColumnHandlePtr> columns;
  for (const auto& column : obj["columns"]) {
    columns.push_back(
        std::make_shared<SerializableFileColumnHandle>(column.asString()));
  }
  return std::make_shared<SerializableFileTableHandle>(
      obj["connectorId"].asString(),
      facebook::axiom::SchemaTableName{
          obj["schema"].asString(), obj["table"].asString()},
      obj["filePath"].asString(),
      std::move(paths),
      obj["suffix"].asString(),
      velox::ISerializable::deserialize<velox::RowType>(
          obj["fullSchema"], context),
      std::move(columns));
}

folly::dynamic SerializableFileColumnHandle::serialize() const {
  auto obj = serializeBase("SerializableFileColumnHandle");
  obj["columnName"] = name();
  return obj;
}

std::shared_ptr<SerializableFileColumnHandle>
SerializableFileColumnHandle::create(
    const folly::dynamic& obj,
    void* /*context*/) {
  return std::make_shared<SerializableFileColumnHandle>(
      obj["columnName"].asString());
}

folly::dynamic FileRangeSplit::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "FileRangeSplit";
  obj["connectorId"] = connectorId;
  obj["filePath"] = filePath;
  obj["start"] = start;
  obj["length"] = length;
  return obj;
}

std::string FileRangeSplit::toString() const {
  return fmt::format("{}:{}+{}", filePath, start, length);
}

std::shared_ptr<FileRangeSplit> FileRangeSplit::create(
    const folly::dynamic& obj) {
  return std::make_shared<FileRangeSplit>(
      obj["connectorId"].asString(),
      obj["filePath"].asString(),
      obj["start"].asInt(),
      obj["length"].asInt());
}

void registerFileSerde() {
  static std::once_flag once;
  std::call_once(once, [] {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register(
        "SerializableFileTableHandle", SerializableFileTableHandle::create);
    registry.Register(
        "SerializableFileColumnHandle", SerializableFileColumnHandle::create);
    velox::DeserializationRegistryForSharedPtr().Register(
        "FileRangeSplit", FileRangeSplit::create);
  });
}

} // namespace ember
