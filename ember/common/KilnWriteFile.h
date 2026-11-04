// KilnWriteFile: a Velox WriteFile whose bytes go into a new Kiln chunk, so
// that Velox's file writers (e.g. Parquet) write into Kiln as into any file.
//
// append() copies the bytes into a buffer of the cluster's write pool and
// appends the buffer to the chunk each time it is full. close() appends the
// rest and commits the chunk; a file with no bytes gets no chunk. A file
// destroyed before close() drops its chunk.

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "kiln/common/Types.h"
#include "velox/common/file/File.h"

namespace kiln {
class Buffer;
namespace client {
class BufferPool;
class ChunkWriter;
} // namespace client
} // namespace kiln

namespace ember {

class KilnWriteFile : public facebook::velox::WriteFile {
 public:
  /// A new chunk in 'bucket' of the Kiln cluster of metadata server
  /// 'metadata' (as shuffle::kilnClient()), placed by 'placement'. Holds one
  /// write buffer until closed.
  KilnWriteFile(
      const std::string& metadata,
      kiln::BucketId bucket,
      const kiln::Placement& placement);

  ~KilnWriteFile() override;

  void append(std::string_view data) override;

  /// Bytes reach Kiln as the buffer fills, and all of them on close().
  void flush() override {}

  void close() override;

  uint64_t size() const override {
    return size_;
  }

  /// The committed chunk, as hex: empty before close(), and for no bytes.
  const std::string& chunk() const {
    return chunk_;
  }

 private:
  void appendBuffer();

  std::shared_ptr<kiln::client::BufferPool> pool_;
  std::unique_ptr<kiln::client::ChunkWriter> writer_;
  // One buffer of pool_: a vector for Kiln's gather append, which gives it
  // back.
  std::vector<kiln::Buffer> buffers_;
  std::string chunk_;
  uint64_t size_{0};
  bool closed_{false};
};

} // namespace ember
