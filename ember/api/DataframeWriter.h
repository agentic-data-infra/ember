// DataframeWriter: dataframe.write().gensort(directory). Each task of the
// result stage writes its rows to <directory>/part-NNNNN (NNNNN = the task's
// partition, 5 digits) and returns only counts, so the rows never pass
// through the driver. After a range-partitioned sort, the part files in name
// order hold the sorted rows.
//
// The directory may be on a shared file system or on each worker's local
// disk. Writing fails if the driver sees part files in it already.

#pragma once

#include <cstdint>
#include <string>

#include "ember/Dataframe.h"

namespace ember {

class DataframeWriter {
 public:
  explicit DataframeWriter(Dataframe df) : df_{std::move(df)} {}

  /// Writes the rows as gensort records: each row's "key" and "payload"
  /// (VARBINARY, 100 bytes together), as valsort reads them. Returns the
  /// number of records written.
  int64_t gensort(const std::string& directory) const;

 private:
  Dataframe df_;
};

} // namespace ember
