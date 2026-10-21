// Serializable versions of the file connector's table handle, column handle
// and split, so scans can travel to executors inside a plan fragment.
//
// Axiom's file connector handles do not serialize themselves. These subclasses
// carry the same fields and add serialize()/create(); the connector reads them
// as its own types. FileRangeSplit adds a byte range, which the text reader
// uses to split one file across tasks.

#pragma once

#include "axiom/connectors/file/core/FileTable.h"

namespace ember {

class SerializableFileTableHandle
    : public facebook::axiom::connector::file::FileTableHandle {
 public:
  using FileTableHandle::FileTableHandle;

  /// A copy of 'handle' that serializes.
  static std::shared_ptr<const SerializableFileTableHandle> from(
      const facebook::axiom::connector::file::FileTableHandle& handle);

  folly::dynamic serialize() const override;

  static std::shared_ptr<SerializableFileTableHandle> create(
      const folly::dynamic& obj,
      void* context);
};

class SerializableFileColumnHandle
    : public facebook::axiom::connector::file::FileColumnHandle {
 public:
  using FileColumnHandle::FileColumnHandle;

  folly::dynamic serialize() const override;

  static std::shared_ptr<SerializableFileColumnHandle> create(
      const folly::dynamic& obj,
      void* context);
};

/// Bytes [start, start + length) of a file. A length of -1 means to the end.
/// Text files are read line by line: a range owns the lines that start in it.
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

/// Registers the deserializers above. Safe to call more than once.
void registerFileSerde();

} // namespace ember
