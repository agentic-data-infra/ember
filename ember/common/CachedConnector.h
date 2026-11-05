// The "ember_cached" connector: how a cached Dataframe appears in the plans
// that read it.
//
// Dataframe::cache() registers a CachedTable and starts a new plan with a scan
// of it. The optimizer plans that scan like any other, and the table takes no
// filters, so nothing downstream of cache() moves into the cached plan. The
// driver materializes the cached plan as a stored shuffle first
// (driver/DagScheduler.h), and each task of the stage gets a CachedBlockSplit
// per stored block it reads. The data source reads the pages of those blocks
// straight, with no exchange: it fails with a FetchFailure when a block is
// gone, like an exchange does, and the driver recomputes the output.

#pragma once

#include <string>
#include <string_view>

#include "velox/connectors/Connector.h"

namespace ember {

inline constexpr std::string_view kCachedConnectorId = "ember_cached";

/// The cached table a scan reads. Its stored rows have the columns of
/// 'schema' by position, whatever names they were written under.
class CachedTableHandle : public facebook::velox::connector::ConnectorTableHandle {
 public:
  CachedTableHandle(std::string id, facebook::velox::RowTypePtr schema);

  /// The id of the CachedTable.
  const std::string& name() const override {
    return id_;
  }

  const facebook::velox::RowTypePtr& schema() const {
    return schema_;
  }

  folly::dynamic serialize() const override;

  static std::shared_ptr<CachedTableHandle> create(
      const folly::dynamic& obj,
      void* context);

 private:
  const std::string id_;
  const facebook::velox::RowTypePtr schema_;
};

class CachedColumnHandle : public facebook::velox::connector::ColumnHandle {
 public:
  explicit CachedColumnHandle(std::string name) : name_{std::move(name)} {}

  const std::string& name() const override {
    return name_;
  }

  folly::dynamic serialize() const override;

  static std::shared_ptr<CachedColumnHandle> create(
      const folly::dynamic& obj,
      void* context);

 private:
  const std::string name_;
};

/// One stored block, by its shuffle block URL. 'compressionKind' is the
/// query's shuffle compression, which a data source cannot see.
struct CachedBlockSplit : public facebook::velox::connector::ConnectorSplit {
  CachedBlockSplit(std::string url, std::string compressionKind)
      : ConnectorSplit(std::string(kCachedConnectorId)),
        url{std::move(url)},
        compressionKind{std::move(compressionKind)} {}

  const std::string url;
  const std::string compressionKind;

  folly::dynamic serialize() const override;

  std::string toString() const override {
    return url;
  }

  static std::shared_ptr<CachedBlockSplit> create(const folly::dynamic& obj);
};

/// Registers the connector and its serde. Safe to call once.
void registerCachedConnector();

} // namespace ember
