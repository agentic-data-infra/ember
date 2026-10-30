// PartFileWriter: writes a result task's rows to its part file. Rows go to a
// hidden temporary file next to it, renamed to part-NNNNN on commit, so a
// failed or retried attempt never leaves a partial part file behind. In a
// kiln:// directory, rows go to a Kiln chunk instead, which the driver
// publishes as the part once the job is done (KilnFiles.h).

#pragma once

#include <map>
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

namespace kiln {
class Buffer;
namespace client {
class ChunkWriter;
class Client;
} // namespace client
} // namespace kiln

namespace ember {

class PartFileWriter {
 public:
  /// Writes partition 'partition' of 'files' for task 'taskId', with the
  /// task's 'config' (for kilnFilesPlacement()).
  PartFileWriter(
      OutputFiles files,
      int32_t partition,
      const std::string& taskId,
      const std::map<std::string, std::string>& config);

  /// Removes the temporary file, or drops the chunk, unless committed.
  ~PartFileWriter();

  /// The part file 'partition' of 'directory' is written to.
  static std::string partPath(const std::string& directory, int32_t partition);

  /// The name of part 'partition': part-NNNNN.
  static std::string partName(int32_t partition);

  void append(const facebook::velox::RowVectorPtr& rows);

  /// Closes the file and gives it its name. Returns its size.
  int64_t commit();

  /// The committed chunk (hex) of a kiln:// part; empty for no rows, or for
  /// a file.
  const std::string& chunk() const {
    return chunk_;
  }

 private:
  void appendToChunk(const std::string& data);

  const OutputFiles files_;
  const std::string path_;
  const std::string tempPath_;
  std::shared_ptr<facebook::velox::filesystems::FileSystem> fileSystem_;
  std::unique_ptr<facebook::velox::WriteFile> file_;
  std::shared_ptr<kiln::client::Client> kiln_;
  std::unique_ptr<kiln::client::ChunkWriter> chunkWriter_;
  // Bytes waiting to be appended to the chunk.
  std::unique_ptr<kiln::Buffer> kilnBuffer_;
  std::string chunk_;
  std::string buffer_;
  int64_t bytes_{0};
  bool committed_{false};
};

} // namespace ember
