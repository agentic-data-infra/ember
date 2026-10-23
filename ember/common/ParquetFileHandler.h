// Parquet files for Axiom's file connector. Axiom's Parquet handler reads the
// columns named by the scan's output type, but a scan may rename its columns
// (Axiom does when one table is scanned twice in a plan, e.g. url -> url_3).
// This handler reads the file columns named by the column handles and returns
// them under the output names. A FileRangeSplit reads only the row groups
// that start in its byte range, so one file can spread over several tasks.

#pragma once

#include "axiom/connectors/file/parquet/ParquetFileHandler.h"

namespace ember {

class ParquetFileHandler
    : public facebook::axiom::connector::file::ParquetFileHandler {
 protected:
  std::unique_ptr<facebook::velox::connector::DataSource> createDataSource(
      const facebook::velox::RowTypePtr& outputType,
      const facebook::velox::RowTypePtr& fullSchema,
      const facebook::velox::connector::ColumnHandleMap& columnHandles,
      facebook::velox::memory::MemoryPool* pool) const override;
};

/// Registers ParquetFileHandler under the file connector schema "parquet", in
/// place of Axiom's registerParquetHandler(). Safe to call more than once.
void registerParquetFileHandler();

} // namespace ember
