// Stored shuffle outputs in Kiln's catalog (kiln/catalog), so that later
// programs reuse them, not just later jobs of the same session.
//
// A memo maps a stage key (StageKey.h) to a manifest of the stage's map
// outputs: the chunk and partition lengths of each map output, which must
// all be in Kiln (ember.shuffle.store=kiln). Stage keys include the driver's
// executable, so a later run of the same program build finds the memo. A
// memo whose chunks Kiln has evicted is gone; the stage then runs again and
// stores a new one.

#pragma once

#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "ember/shuffle/ShuffleInterface.h"

namespace kiln::catalog {
class Catalog;
}

namespace ember::driver {

class KilnMemo {
 public:
  /// The memo of the session with 'config' if it keeps outputs in Kiln's
  /// catalog (DagScheduler::kReuseInKiln), else null.
  static std::unique_ptr<KilnMemo> fromConfig(
      const std::map<std::string, std::string>& config);

  explicit KilnMemo(std::string metadata);
  ~KilnMemo();

  /// The map outputs stored under 'key', by map id, if Kiln still holds
  /// them all.
  std::optional<std::vector<shuffle::MapStatus>> get(const std::string& key);

  /// Stores 'maps' under 'key', unless 'key' has a memo or a map output is
  /// not in this Kiln cluster. Returns whether it stored them.
  bool put(const std::string& key, const std::vector<shuffle::MapStatus>& maps);

 private:
  const std::string metadata_;
  const std::unique_ptr<kiln::catalog::Catalog> catalog_;
};

} // namespace ember::driver
