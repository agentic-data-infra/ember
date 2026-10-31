// DataframeWriter: dataframe.write().gensort(directory) or .stream(name).
//
// gensort: each task of the result stage writes its rows to
// <directory>/part-NNNNN (NNNNN = the task's partition, 5 digits) and returns
// only counts, so the rows never pass through the driver. After a
// range-partitioned sort, the part files in name order hold the sorted rows.
// The directory may be on a shared file system or on each worker's local
// disk. Writing fails if the driver sees part files in it already.
//
// stream: in builds with stoke (-c ember.stoke=true), writes training
// batches into a stoke stream through Axiom's table write into the "stoke"
// connector (StokeConnector.h): every task of the stage that makes the rows
// puts its chunks into the stream while it runs, and the driver commits the
// write at the end of the job.

#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "ember/Dataframe.h"

namespace ember {

/// Config key: bytes after which a stream's chunk is put; default 16 MiB.
inline constexpr std::string_view kStokeChunkBytesConfig =
    "ember.stoke.chunk_bytes";

class DataframeWriter {
 public:
  explicit DataframeWriter(Dataframe df) : df_{std::move(df)} {}

  /// Writes the rows as gensort records: each row's "key" and "payload"
  /// (VARBINARY, 100 bytes together), as valsort reads them. Returns the
  /// number of records written.
  int64_t gensort(const std::string& directory) const;

  /// Writes the rows, one VARBINARY column of batches in stoke's format
  /// (e.g. stoke_pack's), into the stoke stream 'name' of the Kiln cluster
  /// ember.kiln.metadata (or ember.shuffle.kiln.metadata), in chunks of
  /// ember.stoke.chunk_bytes (default 16 MiB). With 'seal', seals the stream
  /// once the write is committed. Returns the number of rows written, NULLs
  /// included.
  int64_t stream(const std::string& name, bool seal = false) const;

 private:
  Dataframe df_;
};

} // namespace ember
