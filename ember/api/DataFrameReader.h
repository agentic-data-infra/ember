// DataFrameReader: spark.read().text(path) and friends. A path is a file or a
// directory; a directory is read as one table of all its data files (names
// starting with "_" or "." are skipped, as in Spark).

#pragma once

#include <memory>
#include <string>

namespace ember {

class DataFrame;
class SessionState;

class DataFrameReader {
 public:
  explicit DataFrameReader(std::shared_ptr<SessionState> session)
      : session_{std::move(session)} {}

  /// One row per line, in a single string column "value".
  DataFrame text(const std::string& path) const;

  DataFrame parquet(const std::string& path) const;

  DataFrameReader& format(std::string format) {
    format_ = std::move(format);
    return *this;
  }

  /// Reads 'path' in the format set by format() (default parquet).
  DataFrame load(const std::string& path) const;

 private:
  std::shared_ptr<SessionState> session_;
  std::string format_{"parquet"};
};

} // namespace ember
