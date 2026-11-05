#include "ember/shuffle/FileShuffle.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <numeric>

#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/String.h>
#include <folly/io/Cursor.h>
#include <folly/lang/Bits.h>

#include "velox/common/base/Exceptions.h"

namespace ember::shuffle {

namespace fs = std::filesystem;

namespace {

constexpr size_t kFrameHeaderBytes = sizeof(uint32_t);

void writeAll(int fd, const void* data, size_t size, const std::string& path) {
  auto written = folly::writeFull(fd, data, size);
  VELOX_CHECK_EQ(
      written,
      static_cast<ssize_t>(size),
      "Failed to write shuffle file {}: {}",
      path,
      folly::errnoStr(errno));
}

using Write = PartitionedPageWriter::Write;

Write fileWrite(int fd, const std::string& path) {
  return [fd, &path](const void* data, size_t size) {
    writeAll(fd, data, size, path);
  };
}

// Writes 'page' as one frame; returns the frame's size.
int64_t writeFrame(const Write& write, const folly::IOBuf& page) {
  const auto length = page.computeChainDataLength();
  VELOX_CHECK_LE(length, std::numeric_limits<uint32_t>::max());
  const auto header = folly::Endian::little(static_cast<uint32_t>(length));
  write(&header, sizeof(header));
  for (auto range : page) {
    write(range.data(), range.size());
  }
  return kFrameHeaderBytes + length;
}

// Copies 'length' bytes at 'offset' of 'from' to 'write'.
void copyRange(
    int from,
    int64_t offset,
    int64_t length,
    const Write& write,
    const std::string& path) {
  constexpr int64_t kChunk = 1 << 20;
  std::string buffer;
  buffer.resize(std::min(length, kChunk));
  while (length > 0) {
    const auto n = std::min<int64_t>(length, buffer.size());
    auto read = folly::preadFull(from, buffer.data(), n, offset);
    VELOX_CHECK_EQ(read, n, "Short read from shuffle spill for {}", path);
    write(buffer.data(), n);
    offset += n;
    length -= n;
  }
}

std::string readRange(const std::string& path, int64_t offset, int64_t length) {
  folly::File file(path, O_RDONLY);
  std::string data;
  data.resize(length);
  auto read = folly::preadFull(file.fd(), data.data(), length, offset);
  VELOX_CHECK_EQ(read, length, "Short read from {}", path);
  return data;
}

class FileShuffleWriter : public PartitionedPageWriter {
 public:
  using PartitionedPageWriter::PartitionedPageWriter;

  ~FileShuffleWriter() override {
    if (!done()) {
      abort();
    }
  }

  void abort() override {
    PartitionedPageWriter::abort();
    std::error_code ec;
    fs::remove(tmpPath("data"), ec);
    fs::remove(tmpPath("index"), ec);
  }

 protected:
  MapStatus commitOutput() override {
    const auto data = tmpPath("data");
    const auto index = tmpPath("index");
    std::vector<int64_t> offsets;
    {
      folly::File out(data, O_WRONLY | O_CREAT | O_TRUNC);
      offsets = writePartitions(fileWrite(out.fd(), data));
    }
    {
      folly::File out(index, O_WRONLY | O_CREAT | O_TRUNC);
      writeAll(
          out.fd(), offsets.data(), offsets.size() * sizeof(int64_t), index);
    }
    store().commitFiles(shuffleId(), mapId(), data, index);
    return store().mapStatus(shuffleId(), mapId());
  }
};

} // namespace

PartitionedPageWriter::PartitionedPageWriter(
    std::shared_ptr<ShuffleBlockStore> store,
    int32_t shuffleId,
    int32_t mapId,
    int32_t numPartitions,
    std::string attemptTag,
    int64_t spillThreshold)
    : store_{std::move(store)},
      shuffleId_{shuffleId},
      mapId_{mapId},
      numPartitions_{numPartitions},
      attemptTag_{std::move(attemptTag)},
      spillThreshold_{spillThreshold},
      pages_(numPartitions) {
  VELOX_CHECK_GT(numPartitions_, 0);
}

PartitionedPageWriter::~PartitionedPageWriter() {
  removeRuns();
}

void PartitionedPageWriter::collect(
    int32_t partition,
    std::unique_ptr<folly::IOBuf> page) {
  VELOX_CHECK(!done_);
  VELOX_CHECK_GE(partition, 0);
  VELOX_CHECK_LT(partition, numPartitions_);
  bufferedBytes_ += page->computeChainDataLength();
  pages_[partition].push_back(std::move(page));
  if (bufferedBytes_ >= spillThreshold_) {
    spill();
  }
}

void PartitionedPageWriter::releaseMemory() {
  if (!done_ && bufferedBytes_ > 0) {
    spill();
  }
}

MapStatus PartitionedPageWriter::commit() {
  VELOX_CHECK(!done_);
  done_ = true;
  auto status = commitOutput();
  removeRuns();
  return status;
}

void PartitionedPageWriter::abort() {
  done_ = true;
  for (auto& pages : pages_) {
    pages.clear();
  }
  removeRuns();
}

std::vector<int64_t> PartitionedPageWriter::writePartitions(
    const Write& write) {
  std::vector<int64_t> offsets{0};
  std::vector<folly::File> runs;
  for (const auto& run : runs_) {
    runs.emplace_back(run.path, O_RDONLY);
  }
  int64_t offset = 0;
  for (int32_t p = 0; p < numPartitions_; ++p) {
    for (size_t i = 0; i < runs_.size(); ++i) {
      const auto start = runs_[i].offsets[p];
      const auto length = runs_[i].offsets[p + 1] - start;
      copyRange(runs[i].fd(), start, length, write, runs_[i].path);
      offset += length;
    }
    for (const auto& page : pages_[p]) {
      offset += writeFrame(write, *page);
    }
    pages_[p].clear();
    offsets.push_back(offset);
  }
  bytesWritten_ = offset;
  return offsets;
}

int64_t PartitionedPageWriter::outputBytes() const {
  int64_t bytes = 0;
  for (const auto& run : runs_) {
    bytes += run.offsets.back();
  }
  for (const auto& pages : pages_) {
    for (const auto& page : pages) {
      bytes += kFrameHeaderBytes + page->computeChainDataLength();
    }
  }
  return bytes;
}

std::string PartitionedPageWriter::tmpPath(const std::string& suffix) const {
  return fmt::format(
      "{}/shuffle_{}_{}.{}.{}.tmp",
      store_->directory(),
      shuffleId_,
      mapId_,
      attemptTag_,
      suffix);
}

void PartitionedPageWriter::spill() {
  SpillRun run;
  run.path = tmpPath(fmt::format("spill{}", runs_.size()));
  folly::File out(run.path, O_WRONLY | O_CREAT | O_TRUNC);
  run.offsets = writeBuffered(fileWrite(out.fd(), run.path));
  runs_.push_back(std::move(run));
  ++spillCount_;
}

std::vector<int64_t> PartitionedPageWriter::writeBuffered(const Write& write) {
  std::vector<int64_t> offsets{0};
  int64_t offset = 0;
  for (auto& pages : pages_) {
    for (const auto& page : pages) {
      offset += writeFrame(write, *page);
    }
    pages.clear();
    offsets.push_back(offset);
  }
  bufferedBytes_ = 0;
  return offsets;
}

void PartitionedPageWriter::removeRuns() {
  std::error_code ec;
  for (const auto& run : runs_) {
    fs::remove(run.path, ec);
  }
  runs_.clear();
}

std::string BlockId::toString() const {
  return fmt::format("shuffle_{}_{}_{}", shuffleId, mapId, reduceId);
}

std::vector<int64_t> MapStatus::partitionLengths() const {
  std::vector<int64_t> lengths;
  for (const auto& segment : segments) {
    lengths.resize(std::max(lengths.size(), segment.size()), 0);
    for (size_t p = 0; p < segment.size(); ++p) {
      lengths[p] += segment[p];
    }
  }
  return lengths;
}

std::vector<std::pair<int64_t, int64_t>> MapStatus::blockRanges(
    int32_t reduceId) const {
  std::vector<std::pair<int64_t, int64_t>> ranges;
  int64_t start = 0;
  for (const auto& segment : segments) {
    VELOX_CHECK_LT(reduceId, segment.size());
    const auto offset = std::accumulate(
        segment.begin(), segment.begin() + reduceId, start);
    if (segment[reduceId] > 0) {
      ranges.emplace_back(offset, segment[reduceId]);
    }
    start = std::accumulate(segment.begin(), segment.end(), start);
  }
  return ranges;
}

namespace {
constexpr std::string_view kBlockScheme = "ember-shuffle://";
constexpr std::string_view kKilnScheme = "kiln://";

// The '/'-separated parts of 'url' after its scheme.
std::vector<std::string> urlParts(const std::string& url, size_t schemeSize) {
  std::vector<std::string> parts;
  folly::split('/', url.substr(schemeSize), parts);
  return parts;
}
} // namespace

std::string blockUrl(
    const std::string& location,
    const BlockId& id,
    const std::vector<std::pair<int64_t, int64_t>>& ranges) {
  if (isKilnLocation(location)) {
    std::vector<int64_t> offsets;
    std::vector<int64_t> lengths;
    for (const auto& [offset, length] : ranges) {
      offsets.push_back(offset);
      lengths.push_back(length);
    }
    return fmt::format(
        "{}/{}/{}/{}/{}/{}",
        location,
        folly::join(',', offsets),
        folly::join(',', lengths),
        id.shuffleId,
        id.mapId,
        id.reduceId);
  }
  return fmt::format(
      "{}{}/{}/{}/{}",
      kBlockScheme,
      location,
      id.shuffleId,
      id.mapId,
      id.reduceId);
}

bool isBlockUrl(const std::string& url) {
  return url.starts_with(kBlockScheme) || url.starts_with(kKilnScheme);
}

std::pair<std::string, BlockId> parseBlockUrl(const std::string& url) {
  VELOX_CHECK(isBlockUrl(url), "Not a shuffle block: {}", url);
  if (isKilnLocation(url)) {
    // kiln://<metadata>/<chunk>/<offsets>/<lengths>/<shuffle>/<map>/<reduce>
    auto parts = urlParts(url, kKilnScheme.size());
    VELOX_CHECK_EQ(parts.size(), 7, "Malformed shuffle block: {}", url);
    return {
        fmt::format("{}{}/{}", kKilnScheme, parts[0], parts[1]),
        BlockId{
            folly::to<int32_t>(parts[4]),
            folly::to<int32_t>(parts[5]),
            folly::to<int32_t>(parts[6])}};
  }
  auto parts = urlParts(url, kBlockScheme.size());
  VELOX_CHECK_EQ(parts.size(), 4, "Malformed shuffle block: {}", url);
  return {
      parts[0],
      BlockId{
          folly::to<int32_t>(parts[1]),
          folly::to<int32_t>(parts[2]),
          folly::to<int32_t>(parts[3])}};
}

std::string kilnLocation(const std::string& metadata, const std::string& chunk) {
  // Only what would break the '/'-separated URL: a URL list such as
  // "tcp://meta:7470,rdma://10.0.0.1:7471" keeps its other characters.
  std::string escaped;
  for (const char c : metadata) {
    if (c == '/' || c == '%') {
      escaped += fmt::format("%{:02X}", static_cast<unsigned char>(c));
    } else {
      escaped += c;
    }
  }
  return fmt::format("{}{}/{}", kKilnScheme, escaped, chunk);
}

bool isKilnLocation(const std::string& location) {
  return location.starts_with(kKilnScheme);
}

std::vector<std::pair<int64_t, int64_t>> kilnBlockRanges(
    const std::string& url) {
  auto parts = urlParts(url, kKilnScheme.size());
  VELOX_CHECK_EQ(parts.size(), 7, "Malformed Kiln shuffle block: {}", url);
  std::vector<int64_t> offsets;
  std::vector<int64_t> lengths;
  if (!parts[2].empty()) {
    folly::split(',', parts[2], offsets);
  }
  if (!parts[3].empty()) {
    folly::split(',', parts[3], lengths);
  }
  VELOX_CHECK_EQ(
      offsets.size(), lengths.size(), "Malformed Kiln shuffle block: {}", url);
  std::vector<std::pair<int64_t, int64_t>> ranges;
  for (size_t i = 0; i < offsets.size(); ++i) {
    ranges.emplace_back(offsets[i], lengths[i]);
  }
  return ranges;
}

ShuffleBlockStore::ShuffleBlockStore(std::string location, std::string directory)
    : location_{std::move(location)}, directory_{std::move(directory)} {
  fs::create_directories(directory_);
}

std::unique_ptr<ShuffleWriter> ShuffleBlockStore::createWriter(
    int32_t shuffleId,
    int32_t mapId,
    int32_t numPartitions,
    const std::string& attemptTag,
    int64_t spillThreshold) {
  return std::make_unique<FileShuffleWriter>(
      shared_from_this(),
      shuffleId,
      mapId,
      numPartitions,
      attemptTag,
      spillThreshold);
}

std::string ShuffleBlockStore::dataFile(int32_t shuffleId, int32_t mapId)
    const {
  return fmt::format("{}/shuffle_{}_{}.data", directory_, shuffleId, mapId);
}

std::string ShuffleBlockStore::indexFile(int32_t shuffleId, int32_t mapId)
    const {
  return fmt::format("{}/shuffle_{}_{}.index", directory_, shuffleId, mapId);
}

bool ShuffleBlockStore::hasMapOutput(int32_t shuffleId, int32_t mapId) const {
  {
    std::lock_guard<std::mutex> l(commitMutex_);
    if (kilnOutputs_.contains({shuffleId, mapId})) {
      return true;
    }
  }
  return fs::exists(indexFile(shuffleId, mapId));
}

bool ShuffleBlockStore::commitFiles(
    int32_t shuffleId,
    int32_t mapId,
    const std::string& tmpData,
    const std::string& tmpIndex) {
  std::lock_guard<std::mutex> l(commitMutex_);
  if (fs::exists(indexFile(shuffleId, mapId))) {
    std::error_code ec;
    fs::remove(tmpData, ec);
    fs::remove(tmpIndex, ec);
    return false;
  }
  fs::rename(tmpData, dataFile(shuffleId, mapId));
  fs::rename(tmpIndex, indexFile(shuffleId, mapId));
  return true;
}

bool ShuffleBlockStore::commitKilnOutput(
    int32_t shuffleId,
    int32_t mapId,
    MapStatus status) {
  std::lock_guard<std::mutex> l(commitMutex_);
  return kilnOutputs_.emplace(std::pair{shuffleId, mapId}, std::move(status))
      .second;
}

std::vector<int64_t> ShuffleBlockStore::readOffsets(
    int32_t shuffleId,
    int32_t mapId) const {
  const auto path = indexFile(shuffleId, mapId);
  const auto size = static_cast<int64_t>(fs::file_size(path));
  VELOX_CHECK_EQ(size % sizeof(int64_t), 0, "Corrupt shuffle index {}", path);
  auto bytes = readRange(path, 0, size);
  std::vector<int64_t> offsets(size / sizeof(int64_t));
  std::memcpy(offsets.data(), bytes.data(), size);
  return offsets;
}

MapStatus ShuffleBlockStore::mapStatus(int32_t shuffleId, int32_t mapId) const {
  {
    std::lock_guard<std::mutex> l(commitMutex_);
    auto it = kilnOutputs_.find({shuffleId, mapId});
    if (it != kilnOutputs_.end()) {
      return it->second;
    }
  }
  auto offsets = readOffsets(shuffleId, mapId);
  MapStatus status;
  status.location = location_;
  status.segments.push_back(partitionLengths(offsets));
  return status;
}

std::unique_ptr<folly::IOBuf> ShuffleBlockStore::readBlock(
    const BlockId& id) const {
  auto offsets = readOffsets(id.shuffleId, id.mapId);
  VELOX_CHECK_LT(
      id.reduceId + 1,
      static_cast<int32_t>(offsets.size()),
      "No partition {} in {}",
      id.reduceId,
      id.toString());
  const auto start = offsets[id.reduceId];
  const auto length = offsets[id.reduceId + 1] - start;
  if (length == 0) {
    return folly::IOBuf::create(0);
  }
  auto data = readRange(dataFile(id.shuffleId, id.mapId), start, length);
  return folly::IOBuf::fromString(std::move(data));
}

void ShuffleBlockStore::removeShuffle(int32_t shuffleId) {
  {
    // The driver deletes the chunks with the shuffle's Kiln bucket.
    std::lock_guard<std::mutex> l(commitMutex_);
    std::erase_if(kilnOutputs_, [&](const auto& entry) {
      return entry.first.first == shuffleId;
    });
  }
  const auto prefix = fmt::format("shuffle_{}_", shuffleId);
  std::error_code ec;
  for (const auto& entry : fs::directory_iterator(directory_, ec)) {
    if (entry.path().filename().string().starts_with(prefix)) {
      fs::remove(entry.path(), ec);
    }
  }
}

folly::coro::Task<std::unique_ptr<folly::IOBuf>>
LocalBlockTransport::co_fetchBlock(std::string location, BlockId id) {
  if (location == store_->location()) {
    co_return store_->readBlock(id);
  }
  VELOX_CHECK_NOT_NULL(remote_, "No shuffle store at {}", location);
  co_return co_await remote_->co_fetchBlock(std::move(location), id);
}

std::vector<int64_t> partitionLengths(const std::vector<int64_t>& offsets) {
  std::vector<int64_t> lengths;
  for (size_t i = 1; i < offsets.size(); ++i) {
    lengths.push_back(offsets[i] - offsets[i - 1]);
  }
  return lengths;
}

std::vector<std::unique_ptr<folly::IOBuf>> splitFrames(
    std::unique_ptr<folly::IOBuf> block) {
  std::vector<std::unique_ptr<folly::IOBuf>> pages;
  if (block == nullptr) {
    return pages;
  }
  folly::io::Cursor cursor(block.get());
  while (!cursor.isAtEnd()) {
    const auto length = cursor.readLE<uint32_t>();
    std::unique_ptr<folly::IOBuf> page;
    cursor.clone(page, length);
    pages.push_back(std::move(page));
  }
  return pages;
}

std::vector<std::unique_ptr<folly::IOBuf>> takeFrames(folly::IOBufQueue& queue) {
  std::vector<std::unique_ptr<folly::IOBuf>> pages;
  while (queue.chainLength() >= sizeof(uint32_t)) {
    const auto length = folly::io::Cursor(queue.front()).readLE<uint32_t>();
    if (queue.chainLength() < sizeof(uint32_t) + length) {
      break;
    }
    queue.trimStart(sizeof(uint32_t));
    pages.push_back(length > 0 ? queue.split(length) : folly::IOBuf::create(0));
  }
  return pages;
}

} // namespace ember::shuffle
