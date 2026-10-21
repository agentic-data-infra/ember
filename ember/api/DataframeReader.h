// DataframeReader: ember.read().text(path) and friends. A path is a file or a
// directory; a directory is read as one table of all its data files (names
// starting with "_" or "." are skipped).

#pragma once

#include <memory>
#include <string>

namespace ember {

class Dataframe;
class SessionState;

class DataframeReader {
 public:
  explicit DataframeReader(std::shared_ptr<SessionState> session)
      : session_{std::move(session)} {}

  /// One row per line, in a single string column "value".
  Dataframe text(const std::string& path) const;

  Dataframe parquet(const std::string& path) const;

  DataframeReader& format(std::string format) {
    format_ = std::move(format);
    return *this;
  }

  /// Reads 'path' in the format set by format() (default parquet).
  Dataframe load(const std::string& path) const;

 private:
  std::shared_ptr<SessionState> session_;
  std::string format_{"parquet"};
};

} // namespace ember
