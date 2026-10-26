#include "ember/driver/KilnMemo.h"

#include <folly/coro/BlockingWait.h>
#include <folly/json.h>

#include "ember/driver/DagScheduler.h"
#include "ember/shuffle/KilnShuffle.h"
#include "ember/shuffle/ShuffleConfig.h"
#include "kiln/catalog/Catalog.h"
#include "kiln/client/Client.h"
#include "velox/common/base/Exceptions.h"

namespace ember::driver {

namespace {

using folly::coro::blockingWait;

// The manifest property that lists the map outputs: per map, the index of
// its chunk in Manifest::chunks (-1 for an empty output, which has none)
// and its partition lengths.
constexpr const char* kMapsProperty = "ember.maps";

} // namespace

std::unique_ptr<KilnMemo> KilnMemo::fromConfig(
    const std::map<std::string, std::string>& config) {
  auto it = config.find(DagScheduler::kReuseInKiln);
  if (it == config.end() || !folly::to<bool>(it->second)) {
    return nullptr;
  }
  auto store = config.find(std::string(shuffle::kShuffleStore));
  auto metadata = config.find(std::string(shuffle::kKilnMetadata));
  VELOX_USER_CHECK(
      store != config.end() && store->second == "kiln" &&
          metadata != config.end(),
      "{} needs {}=kiln and {}",
      DagScheduler::kReuseInKiln,
      shuffle::kShuffleStore,
      shuffle::kKilnMetadata);
  return std::make_unique<KilnMemo>(metadata->second);
}

KilnMemo::KilnMemo(std::string metadata)
    : metadata_{std::move(metadata)},
      catalog_{std::make_unique<kiln::catalog::Catalog>(
          *shuffle::kilnClient(metadata_),
          // Like the shuffle chunks they list: in DRAM only.
          kiln::catalog::CatalogOptions{
              .placement = {.tier = kiln::Tier::kDram},
              .refPrefix = "memo/ember/"})} {}

KilnMemo::~KilnMemo() = default;

std::optional<std::vector<shuffle::MapStatus>> KilnMemo::get(
    const std::string& key) {
  auto manifest = blockingWait(catalog_->co_getMemo(key));
  if (!manifest.has_value()) {
    return std::nullopt;
  }
  auto it = manifest->properties.find(kMapsProperty);
  VELOX_CHECK(it != manifest->properties.end(), "Memo {} lists no maps", key);
  std::vector<shuffle::MapStatus> maps;
  for (const auto& map : folly::parseJson(it->second)) {
    const auto chunk = map["chunk"].asInt();
    shuffle::MapStatus status{
        .location = shuffle::kilnLocation(
            metadata_,
            chunk < 0 ? "" : manifest->chunks.at(chunk).id.toString())};
    for (const auto& length : map["lengths"]) {
      status.partitionLengths.push_back(length.asInt());
    }
    maps.push_back(std::move(status));
  }
  return maps;
}

bool KilnMemo::put(
    const std::string& key,
    const std::vector<shuffle::MapStatus>& maps) {
  kiln::catalog::Manifest manifest;
  folly::dynamic list = folly::dynamic::array;
  for (const auto& status : maps) {
    if (!shuffle::isKilnLocation(status.location)) {
      return false;
    }
    const auto [metadata, chunk] = shuffle::parseKilnLocation(status.location);
    if (metadata != metadata_) {
      return false;
    }
    int64_t size = 0;
    folly::dynamic lengths = folly::dynamic::array;
    for (const auto length : status.partitionLengths) {
      lengths.push_back(length);
      size += length;
    }
    int64_t index = -1;
    if (!chunk.empty()) {
      index = manifest.chunks.size();
      manifest.chunks.push_back(
          {.id = kiln::ChunkId::parse(chunk),
           .length = static_cast<uint64_t>(size)});
    }
    manifest.bytes += size;
    list.push_back(folly::dynamic::object("chunk", index)("lengths", lengths));
  }
  manifest.properties[kMapsProperty] = folly::toJson(list);
  return blockingWait(catalog_->co_putMemo(key, std::move(manifest)));
}

} // namespace ember::driver
