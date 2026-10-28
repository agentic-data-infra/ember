// PartFileWriter: writes a result task's rows to its part file. Rows go to a
// hidden temporary file next to it, renamed to part-NNNNN on commit, so a
// failed or retried attempt never leaves a partial part file behind.

#pragma once

#include <memory>
#include <string>

#include "ember/worker/TaskDescription.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox {
class WriteFile;
namespace filesystems {
class FileSystem;
}
} // namespace facebook::velox

namespace ember {

class PartFileWriter {
 public:
  /// Writes partition 'partition' of 'files' for task 'taskId'.
  PartFileWriter(
      OutputFiles files,
      int32_t partition,
      const std::string& taskId);

  /// Removes the temporary file unless committed.
  ~PartFileWriter();

  /// The part file 'partition' of 'directory' is written to.
  static std::string partPath(const std::string& directory, int32_t partition);

  void append(const facebook::velox::RowVectorPtr& rows);

  /// Closes the file and gives it its name. Returns its size.
  int64_t commit();

 private:
  const OutputFiles files_;
  const std::string path_;
  const std::string tempPath_;
  std::shared_ptr<facebook::velox::filesystems::FileSystem> fileSystem_;
  std::unique_ptr<facebook::velox::WriteFile> file_;
  std::string buffer_;
  int64_t bytes_{0};
  bool committed_{false};
};

} // namespace ember
