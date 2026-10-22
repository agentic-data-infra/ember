#include "ember/driver/StageKey.h"

#include <filesystem>
#include <map>
#include <set>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/SpookyHashV2.h>
#include <folly/json.h>

#include "velox/functions/FunctionRegistry.h"

namespace ember::driver {

namespace fs = std::filesystem;
namespace velox = facebook::velox;

namespace {

// Functions whose result is not a function of their arguments, beyond those
// Velox's registry marks nondeterministic. Matched without Ember's and
// Velox's sparksql prefixes; names starting "current_" match too.
const std::set<std::string> kNondeterministic = {
    "rand",
    "randn",
    "random",
    "secure_rand",
    "secure_random",
    "uuid",
    "now",
    "localtime",
    "localtimestamp",
    "shuffle",
    "monotonically_increasing_id",
    "spark_partition_id",
};

bool isDenied(std::string_view name) {
  return kNondeterministic.contains(std::string(name)) ||
      name.starts_with("current_");
}

bool isNondeterministic(const std::string& name) {
  if (velox::isDeterministic(name) == false || isDenied(name)) {
    return true;
  }
  for (std::string_view prefix : {"ember_", "spark_"}) {
    if (name.starts_with(prefix)) {
      return isDenied(std::string_view(name).substr(prefix.size()));
    }
  }
  return false;
}

std::string hashOf(const std::string& data) {
  uint64_t h1 = 0;
  uint64_t h2 = 0;
  folly::hash::SpookyHashV2::Hash128(data.data(), data.size(), &h1, &h2);
  return fmt::format("{:016x}{:016x}", h1, h2);
}

// The driver's executable, which statically links everything a plan runs.
const std::string& codeVersion() {
  static const std::string version = [] {
    std::error_code ec;
    const auto exe = fs::read_symlink("/proc/self/exe", ec);
    VELOX_CHECK(!ec, "Cannot find the executable: {}", ec.message());
    const auto size = fs::file_size(exe);
    const auto mtime = fs::last_write_time(exe).time_since_epoch().count();
    return hashOf(fmt::format("{}\n{}\n{}", exe.string(), size, mtime));
  }();
  return version;
}

bool isPlanNode(const folly::dynamic& obj) {
  const auto* name = obj.get_ptr("name");
  return name != nullptr && name->isString() &&
      name->getString().ends_with("Node");
}

// Keys whose values are never column names.
bool isOpaqueKey(const std::string& key) {
  return key == "name" || key == "type" || key == "functionName" ||
      key == "tableHandle" || key == "columnHandle";
}

bool isConstant(const folly::dynamic& obj) {
  const auto* name = obj.get_ptr("name");
  return name != nullptr && *name == "ConstantTypedExpr";
}

// Column names a stage defines or uses.
void collectNames(const folly::dynamic& json, std::set<std::string>& names) {
  if (json.isArray()) {
    for (const auto& item : json) {
      collectNames(item, names);
    }
    return;
  }
  if (!json.isObject() || isConstant(json)) {
    return;
  }
  for (const auto& [key, value] : json.items()) {
    const auto& k = key.getString();
    if (isOpaqueKey(k)) {
      continue;
    }
    if ((k == "fieldName" || k == "assign") && value.isString()) {
      names.insert(value.getString());
    } else if (
        (k == "names" || k == "aggregateNames" || k == "unnestNames") &&
        value.isArray()) {
      for (const auto& name : value) {
        if (name.isString()) {
          names.insert(name.getString());
        }
      }
    } else {
      collectNames(value, names);
    }
  }
}

class StageHasher {
 public:
  StageHasher(
      Stage& stage,
      const std::map<int32_t, const Stage*>& byShuffle,
      const std::string& environment)
      : stage_{stage}, byShuffle_{byShuffle}, environment_{environment} {
    collectNames(stage.fragment, names_);
  }

  // The key of 'node' and its subtree, or std::nullopt; sets 'reason_'.
  std::optional<std::string> hashNode(const folly::dynamic& node) {
    std::vector<std::optional<std::string>> children;
    if (const auto* sources = node.get_ptr("sources")) {
      for (const auto& source : *sources) {
        children.push_back(hashNode(source));
      }
    }

    // Parameters, renamed after the sources so that names first used
    // upstream keep their numbers whatever comes downstream.
    folly::dynamic params = folly::dynamic::object;
    for (const auto& [key, value] : node.items()) {
      if (key != "sources" && key != "shuffleId") {
        params[key] = value;
      }
    }
    canonicalize(params);

    std::string data = fmt::format(
        "{}\n{}\n{}\n",
        node["name"].asString(),
        folly::json::serialize(params, sortedKeys()),
        environment_);
    for (const auto& child : children) {
      if (!child.has_value()) {
        return std::nullopt;
      }
      data += *child + "\n";
    }

    if (const auto* call = findNondeterministicCall(params)) {
      reason_ = fmt::format("calls {}", *call);
      return std::nullopt;
    }
    const auto& name = node["name"].getString();
    if (name == "TableScanNode") {
      auto files = fileIdentity(node["tableHandle"]);
      if (!files.has_value()) {
        return std::nullopt;
      }
      data += *files;
    } else if (name == "ShuffleReadNode") {
      const auto shuffleId = node["shuffleId"].asInt();
      if (const auto* input = cachedInput(shuffleId)) {
        if (!input->cacheKey.has_value()) {
          reason_ = fmt::format("reads {}, which has no key", *input->cacheId);
          return std::nullopt;
        }
        data += "cached " + *input->cacheKey;
      } else {
        const auto& parent = *byShuffle_.at(shuffleId);
        if (!parent.key.has_value()) {
          reason_ = fmt::format("reads stage {}", parent.stageId);
          return std::nullopt;
        }
        data += *parent.key;
      }
    }
    auto key = hashOf(data);
    stage_.nodeKeys[node["id"].asString()] = key;
    return key;
  }

  const std::string& reason() const {
    return reason_;
  }

 private:
  static const folly::json::serialization_opts& sortedKeys() {
    static const auto opts = [] {
      folly::json::serialization_opts opts;
      opts.sort_keys = true;
      return opts;
    }();
    return opts;
  }

  const Stage::ShuffleInput* cachedInput(int32_t shuffleId) const {
    for (const auto& input : stage_.shuffleInputs) {
      if (input.shuffleId == shuffleId && input.cacheId.has_value()) {
        return &input;
      }
    }
    return nullptr;
  }

  // Drops plan node ids and renames column names by first use.
  void canonicalize(folly::dynamic& json) {
    if (json.isArray()) {
      for (auto& item : json) {
        canonicalize(item);
      }
      return;
    }
    if (json.isString()) {
      if (names_.contains(json.getString())) {
        auto [it, _] = renames_.try_emplace(
            json.getString(), "$" + std::to_string(renames_.size()));
        json = it->second;
      }
      return;
    }
    if (!json.isObject() || isConstant(json)) {
      return;
    }
    if (isPlanNode(json)) {
      json.erase("id");
    }
    // Visit keys in order, so names are numbered the same way every time.
    std::vector<std::string> keys;
    for (const auto& key : json.keys()) {
      keys.push_back(key.getString());
    }
    std::sort(keys.begin(), keys.end());
    for (const auto& key : keys) {
      if (!isOpaqueKey(key)) {
        canonicalize(json[key]);
      }
    }
  }

  const std::string* findNondeterministicCall(const folly::dynamic& json) {
    if (json.isArray()) {
      for (const auto& item : json) {
        if (const auto* call = findNondeterministicCall(item)) {
          return call;
        }
      }
    }
    if (!json.isObject()) {
      return nullptr;
    }
    if (const auto* name = json.get_ptr("functionName");
        name != nullptr && name->isString()) {
      // Registry lookups are slow; a stage calls few distinct functions.
      auto [it, inserted] = nondeterministic_.try_emplace(name->getString());
      if (inserted) {
        it->second = isNondeterministic(name->getString());
      }
      if (it->second) {
        return &name->getString();
      }
    }
    for (const auto& value : json.values()) {
      if (const auto* call = findNondeterministicCall(value)) {
        return call;
      }
    }
    return nullptr;
  }

  // Path, size and mtime of every file the scan reads. A directory stands for
  // its files, in sorted order.
  std::optional<std::string> fileIdentity(const folly::dynamic& handle) {
    std::vector<std::string> paths;
    if (const auto* files = handle.get_ptr("filePaths")) {
      for (const auto& path : *files) {
        paths.push_back(path.asString());
      }
    }
    if (paths.empty()) {
      paths.push_back(handle["filePath"].asString());
    }
    std::vector<fs::path> files;
    for (auto path : paths) {
      if (path.starts_with("file:")) {
        path = path.substr(5);
      }
      std::error_code ec;
      if (path.find("://") != std::string::npos || !fs::exists(path, ec)) {
        reason_ = fmt::format("reads {}, which is not a local file", path);
        return std::nullopt;
      }
      if (fs::is_directory(path)) {
        for (const auto& entry : fs::recursive_directory_iterator(path)) {
          if (entry.is_regular_file()) {
            files.push_back(entry.path());
          }
        }
      } else {
        files.push_back(path);
      }
    }
    std::sort(files.begin(), files.end());
    std::string identity;
    for (const auto& file : files) {
      identity += fmt::format(
          "{} {} {}\n",
          file.string(),
          fs::file_size(file),
          fs::last_write_time(file).time_since_epoch().count());
    }
    return identity;
  }

  Stage& stage_;
  const std::map<int32_t, const Stage*>& byShuffle_;
  // Code version and config.
  const std::string& environment_;
  std::set<std::string> names_;
  std::map<std::string, std::string> renames_;
  std::map<std::string, bool> nondeterministic_;
  std::string reason_;
};

} // namespace

void computeStageKeys(
    StagePlan& plan,
    const std::map<std::string, std::string>& config) {
  auto environment = codeVersion();
  for (const auto& [key, value] : config) {
    environment += fmt::format("\n{}={}", key, value);
  }
  std::map<int32_t, const Stage*> byShuffle;
  for (auto& stage : plan.stages) {
    StageHasher hasher(stage, byShuffle, environment);
    stage.key = hasher.hashNode(stage.fragment);
    if (stage.key.has_value() && !stage.shuffleId.has_value()) {
      // The result's column names are the user's.
      stage.key = hashOf(fmt::format(
          "{}\n{}",
          *stage.key,
          fmt::join(stage.plan->outputType()->names(), "\n")));
    }
    if (!stage.key.has_value()) {
      stage.noKeyReason = hasher.reason();
    }
    if (stage.shuffleId.has_value()) {
      byShuffle[*stage.shuffleId] = &stage;
    }
  }
}

std::string shortKey(const std::string& key) {
  return key.substr(0, 12);
}

} // namespace ember::driver
