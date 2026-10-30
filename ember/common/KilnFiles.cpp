#include "ember/KilnFiles.h"

#include <optional>

#include <folly/coro/BlockingWait.h>

#include "ember/shuffle/KilnShuffle.h"
#include "ember/shuffle/ShuffleConfig.h"
#include "ember/shuffle/ShuffleInterface.h"
#include "kiln/catalog/Catalog.h"
#include "kiln/client/Client.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/file/File.h"
#include "velox/common/file/FileSystems.h"

namespace ember {

namespace velox = facebook::velox;
using folly::coro::blockingWait;

namespace {

constexpr std::string_view kScheme = "kiln://";
// The first path segment of a dataset path, after the metadata server.
constexpr std::string_view kDataSegment = "data";

// The dataset's current version; throws a user error if there is none.
kiln::catalog::Dataset getDataset(const KilnPath& path) {
  kiln::catalog::Catalog catalog(*shuffle::kilnClient(path.metadata));
  auto dataset = blockingWait(catalog.co_getDataset(path.dataset));
  VELOX_USER_CHECK(
      dataset.has_value(),
      "No Kiln dataset {}",
      kilnPath(path.metadata, path.dataset));
  return std::move(*dataset);
}

// A file of a dataset: bytes [offset, offset + size) of a chunk.
class KilnReadFile : public velox::ReadFile {
 public:
  KilnReadFile(
      std::string path,
      std::shared_ptr<kiln::client::Client> client,
      kiln::ChunkId chunk,
      uint64_t offset,
      uint64_t size)
      : path_{std::move(path)},
        client_{std::move(client)},
        chunk_{chunk},
        offset_{offset},
        size_{size} {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* buf,
      const velox::FileIoContext& /*context*/) const override {
    VELOX_CHECK_LE(offset + length, size_, "Read past the end of {}", path_);
    auto* data = static_cast<char*>(buf);
    for (uint64_t done = 0; done < length;) {
      auto buffer = blockingWait(client_->co_read(
          chunk_,
          offset_ + offset + done,
          length - done,
          client_->registerMemory(data + done, length - done)));
      VELOX_CHECK_GT(buffer.size(), 0, "{} is cut short", path_);
      done += buffer.size();
    }
    return {data, length};
  }

  bool shouldCoalesce() const override {
    return false;
  }

  uint64_t size() const override {
    return size_;
  }

  uint64_t memoryUsage() const override {
    return 0;
  }

  std::string getName() const override {
    return path_;
  }

  uint64_t getNaturalReadSize() const override {
    return 8 << 20;
  }

 private:
  const std::string path_;
  const std::shared_ptr<kiln::client::Client> client_;
  const kiln::ChunkId chunk_;
  const uint64_t offset_;
  const uint64_t size_;
};

class KilnFileSystem : public velox::filesystems::FileSystem {
 public:
  explicit KilnFileSystem(
      std::shared_ptr<const velox::config::ConfigBase> config)
      : FileSystem(std::move(config)) {}

  std::string name() const override {
    return "Kiln";
  }

  std::unique_ptr<velox::ReadFile> openFileForRead(
      std::string_view path,
      const velox::filesystems::FileOptions& /*options*/) override {
    const auto parsed = parseKilnPath(path);
    VELOX_USER_CHECK(!parsed.file.empty(), "Not a file: {}", path);
    const auto dataset = getDataset(parsed);
    for (const auto& entry : dataset.manifest.chunks) {
      if (entry.name == parsed.file &&
          entry.kind == kiln::catalog::EntryKind::kChunk) {
        VELOX_CHECK(entry.length.has_value(), "{} has no length", path);
        return std::make_unique<KilnReadFile>(
            std::string(path),
            shuffle::kilnClient(parsed.metadata),
            entry.id,
            entry.offset,
            *entry.length);
      }
    }
    VELOX_USER_FAIL("No file {}", path);
  }

  std::unique_ptr<velox::WriteFile> openFileForWrite(
      std::string_view path,
      const velox::filesystems::FileOptions& /*options*/) override {
    VELOX_UNSUPPORTED(
        "Cannot write {}: Kiln datasets are written by write()", path);
  }

  void remove(std::string_view path) override {
    VELOX_UNSUPPORTED("Cannot remove {}", path);
  }

  void rename(std::string_view oldPath, std::string_view, bool) override {
    VELOX_UNSUPPORTED("Cannot rename {}", oldPath);
  }

  bool exists(std::string_view path) override {
    const auto parsed = parseKilnPath(path);
    if (parsed.dataset.empty()) {
      return true;
    }
    kiln::catalog::Catalog catalog(*shuffle::kilnClient(parsed.metadata));
    const auto dataset = blockingWait(catalog.co_getDataset(parsed.dataset));
    if (!dataset.has_value()) {
      return false;
    }
    if (parsed.file.empty()) {
      return true;
    }
    for (const auto& entry : dataset->manifest.chunks) {
      if (entry.name == parsed.file) {
        return true;
      }
    }
    return false;
  }

  bool isDirectory(std::string_view path) const override {
    const auto parsed = parseKilnPath(path);
    if (!parsed.file.empty()) {
      return false;
    }
    if (parsed.dataset.empty()) {
      return true;
    }
    kiln::catalog::Catalog catalog(*shuffle::kilnClient(parsed.metadata));
    return blockingWait(catalog.co_getDataset(parsed.dataset)).has_value();
  }

  // A dataset's files in manifest order, or the datasets.
  std::vector<std::string> list(std::string_view path) override {
    const auto parsed = parseKilnPath(path);
    VELOX_USER_CHECK(parsed.file.empty(), "Not a directory: {}", path);
    std::vector<std::string> paths;
    if (parsed.dataset.empty()) {
      kiln::catalog::Catalog catalog(*shuffle::kilnClient(parsed.metadata));
      for (const auto& name : blockingWait(catalog.co_listDatasets())) {
        paths.push_back(kilnPath(parsed.metadata, name));
      }
      return paths;
    }
    for (const auto& entry : getDataset(parsed).manifest.chunks) {
      paths.push_back(kilnPath(parsed.metadata, parsed.dataset, entry.name));
    }
    return paths;
  }

  void mkdir(
      std::string_view path,
      const velox::filesystems::DirectoryOptions& /*options*/) override {
    VELOX_UNSUPPORTED("Cannot make directory {}", path);
  }

  void rmdir(std::string_view path) override {
    VELOX_UNSUPPORTED("Cannot remove {}", path);
  }
};

} // namespace

bool isKilnPath(std::string_view path) {
  if (!path.starts_with(kScheme)) {
    return false;
  }
  const auto slash = path.find('/', kScheme.size());
  if (slash == std::string_view::npos) {
    return false;
  }
  const auto rest = path.substr(slash + 1);
  return rest == kDataSegment ||
      (rest.starts_with(kDataSegment) && rest[kDataSegment.size()] == '/');
}

KilnPath parseKilnPath(std::string_view path) {
  VELOX_USER_CHECK(
      isKilnPath(path),
      "Not a Kiln dataset path (kiln://<metadata>/data/<dataset>/<file>): {}",
      path);
  // kiln://<metadata>/data[/<dataset>[/<file>]]
  auto [metadata, rest] = shuffle::parseKilnLocation(std::string(path));
  KilnPath result{.metadata = std::move(metadata)};
  rest = rest.substr(std::min(rest.size(), kDataSegment.size() + 1));
  const auto slash = rest.find('/');
  result.dataset = rest.substr(0, slash);
  if (slash != std::string::npos) {
    result.file = rest.substr(slash + 1);
  }
  VELOX_USER_CHECK(
      !result.dataset.empty() || result.file.empty(),
      "Kiln path without a dataset: {}",
      path);
  return result;
}

std::string kilnPath(
    const std::string& metadata,
    const std::string& dataset,
    const std::string& file) {
  if (dataset.empty()) {
    return shuffle::kilnLocation(metadata, fmt::format("{}/", kDataSegment));
  }
  return shuffle::kilnLocation(
      metadata, fmt::format("{}/{}/{}", kDataSegment, dataset, file));
}

std::string resolveKilnPath(
    const std::string& path,
    const std::map<std::string, std::string>& config) {
  if (!isKilnPath(path)) {
    return path;
  }
  auto parsed = parseKilnPath(path);
  if (!parsed.metadata.empty()) {
    return path;
  }
  auto it = config.find(std::string(kKilnFilesMetadata));
  if (it == config.end()) {
    it = config.find(std::string(shuffle::kKilnMetadata));
  }
  VELOX_USER_CHECK(
      it != config.end(),
      "{} names no Kiln cluster, and neither {} nor {} is set",
      path,
      kKilnFilesMetadata,
      shuffle::kKilnMetadata);
  return kilnPath(it->second, parsed.dataset, parsed.file);
}

kiln::Placement kilnFilesPlacement(
    const std::map<std::string, std::string>& config) {
  auto tier = [&](std::string_view key) -> std::optional<kiln::Tier> {
    const auto it = config.find(std::string(key));
    if (it == config.end() || it->second == "none") {
      return std::nullopt;
    }
    if (it->second == "dram") {
      return kiln::Tier::kDram;
    }
    VELOX_USER_CHECK_EQ(
        it->second, "localfs", "{} is dram, localfs or none", key);
    return kiln::Tier::kLocalFs;
  };
  return kiln::Placement{
      .tier = tier(kKilnFilesTier).value_or(kiln::Tier::kDram),
      .writeThrough = tier(kKilnFilesWriteThrough)};
}

void registerKilnFileSystem() {
  velox::filesystems::registerFileSystem(
      [](std::string_view path) { return isKilnPath(path); },
      [](std::shared_ptr<const velox::config::ConfigBase> config,
         std::string_view /*path*/) {
        static auto fileSystem =
            std::make_shared<KilnFileSystem>(std::move(config));
        return fileSystem;
      });
}

} // namespace ember
