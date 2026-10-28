// gensort files for Axiom's file connector: the fixed 100-byte records of the
// sort benchmark (sortbenchmark.org), each a 10-byte key and a 90-byte
// payload, read as two VARBINARY columns "key" and "payload". A
// FileRangeSplit owns the records that start in its byte range.

#pragma once

#include "axiom/connectors/file/FileHandler.h"

namespace ember {

inline constexpr const char* kGensortSchema = "gensort";
inline constexpr const char* kGensortKeyColumn = "key";
inline constexpr const char* kGensortPayloadColumn = "payload";
inline constexpr int64_t kGensortKeySize = 10;
inline constexpr int64_t kGensortRecordSize = 100;

class GensortFileHandler : public facebook::axiom::connector::file::FileHandler {
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

/// Registers GensortFileHandler under the file connector schema
/// kGensortSchema. Safe to call more than once.
void registerGensortFileHandler();

} // namespace ember
