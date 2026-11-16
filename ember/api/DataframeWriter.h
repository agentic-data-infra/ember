// DataframeWriter: dataframe.write().parquet(directory), .gensort(directory)
// or .format(connector).option(key, value).save(name).
//
// parquet and gensort: an Axiom table write into the "ember_output" connector
// (OutputConnector.h): every task of the stage that makes the rows writes
// them to <directory>/part-NNNNN.parquet with Velox's Parquet writer, or to
// <directory>/part-NNNNN as gensort records, so the rows never pass through
// the driver. After a range-partitioned sort, the parts in name order hold
// the sorted rows. The directory may be on a shared file system, on each
// worker's local disk, or a Kiln dataset, kiln://<metadata>/data/<dataset>
// (KilnFiles.h), published when the job is done. Writing fails if the
// directory holds part files, or the dataset exists, already.
//
// format(connector).option(key, value).save(name): an Axiom table write into
// the table 'name' of any connector, e.g. "stoke" (StokeConnector.h) in
// builds with stoke. Ember passes the options to the connector as they are.

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>

#include "ember/Dataframe.h"

namespace ember {

class DataframeWriter {
 public:
  explicit DataframeWriter(Dataframe df) : df_{std::move(df)} {}

  /// Writes the rows as snappy-compressed Parquet files, read back with
  /// read().parquet(directory). Returns the number of rows written.
  int64_t parquet(const std::string& directory) const;

  /// Writes the rows as gensort records: each row's "key" and "payload"
  /// (VARBINARY, 100 bytes together), as valsort reads them. Returns the
  /// number of records written.
  int64_t gensort(const std::string& directory) const;

  /// The connector save() writes into.
  DataframeWriter& format(std::string connector) {
    connector_ = std::move(connector);
    return *this;
  }

  /// A session property of the connector for save(), passed as it is.
  DataframeWriter& option(std::string key, std::string value) {
    options_[std::move(key)] = std::move(value);
    return *this;
  }

  /// Writes the rows into the table 'name' of the connector of format(),
  /// each column into the table's column of the same name, with the
  /// options as the connector's session properties. A session with a Kiln
  /// cluster (ember.kiln.metadata, or else the Kiln shuffle's) also passes
  /// it as kKilnMetadataProperty, unless an option sets that. Returns the
  /// number of rows written.
  int64_t save(const std::string& name) const;

 private:
  int64_t files(const std::string& directory, const std::string& format) const;

  Dataframe df_;
  std::string connector_;
  std::map<std::string, std::string> options_;
};

} // namespace ember
