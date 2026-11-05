// FileRangeSplit: a file connector split with a byte range, which the text,
// gensort and Parquet readers use to split one file across tasks. Axiom
// serializes the file connector's own handles and split.

#pragma once

#include "axiom/connectors/file/core/FileTable.h"

namespace ember {

/// Bytes [start, start + length) of a file. A length of -1 means to the end.
/// Text files are read line by line: a range owns the lines that start in it
/// (gensort files: the records).
struct FileRangeSplit : public facebook::axiom::connector::file::FileSplit {
  FileRangeSplit(
      const std::string& connectorId,
      std::string filePath,
      int64_t start = 0,
      int64_t length = -1)
      : FileSplit(connectorId, std::move(filePath)),
        start{start},
        length{length} {}

  const int64_t start;
  const int64_t length;

  folly::dynamic serialize() const override;

  std::string toString() const override;

  static std::shared_ptr<FileRangeSplit> create(const folly::dynamic& obj);
};

/// Registers the file connector's deserializers and FileRangeSplit's. Safe to
/// call more than once.
void registerFileSerde();

} // namespace ember
