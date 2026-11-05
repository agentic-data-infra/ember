#include "ember/FileSerde.h"

#include <mutex>

#include "axiom/connectors/file/FileConnector.h"
#include "velox/common/serialization/Serializable.h"

namespace ember {

namespace file = facebook::axiom::connector::file;
namespace velox = facebook::velox;

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
    file::FileConnector::registerSerDe();
    velox::DeserializationRegistryForSharedPtr().Register(
        "FileRangeSplit", FileRangeSplit::create);
  });
}

} // namespace ember
