// Text files for Axiom's file connector: one row per line, in a single
// VARCHAR column "value", as Spark's spark.read.text. Lines end at "\n" or
// "\r\n"; the terminator is not part of the value.

#pragma once

#include "axiom/connectors/file/FileHandler.h"

namespace ember {

inline constexpr const char* kTextSchema = "text";
inline constexpr const char* kTextColumn = "value";

class TextFileHandler : public facebook::axiom::connector::file::FileHandler {
 public:
  facebook::velox::RowTypePtr resolve(
      const std::string& filePath,
      facebook::velox::memory::MemoryPool* pool) const override;

 protected:
  std::unique_ptr<facebook::velox::connector::DataSource> createDataSource(
      const facebook::velox::RowTypePtr& outputType,
      const facebook::velox::RowTypePtr& fullSchema,
      const facebook::velox::connector::ColumnHandleMap& columnHandles,
      facebook::velox::memory::MemoryPool* pool) const override;
};

/// Registers TextFileHandler under the file connector schema kTextSchema.
/// Safe to call more than once.
void registerTextFileHandler();

} // namespace ember
