#include "ember/DataframeReader.h"

#include <filesystem>

#include "ember/Dataframe.h"
#include "ember/GensortFileHandler.h"
#include "ember/KilnFiles.h"
#include "ember/Session.h"
#include "ember/TextFileHandler.h"

namespace ember {

namespace lp = facebook::axiom::logical_plan;

namespace {

// Local paths become absolute and kiln:// paths get the configured cluster;
// the file connector reads a directory when its path ends in '/'.
std::string tablePath(
    const std::string& path,
    const std::map<std::string, std::string>& config) {
  if (isKilnPath(path)) {
    // A dataset is a directory.
    const auto parsed = parseKilnPath(resolveKilnPath(path, config));
    return kilnPath(parsed.metadata, parsed.dataset, parsed.file);
  }
  if (path.find("://") != std::string::npos || path.starts_with("file:")) {
    return path;
  }
  std::error_code ec;
  auto result = std::filesystem::absolute(path, ec).string();
  if (!result.ends_with('/') && std::filesystem::is_directory(result, ec)) {
    result += "/";
  }
  return result;
}

Dataframe scan(
    const std::shared_ptr<SessionState>& session,
    const std::string& format,
    const std::string& path) {
  return Dataframe(
      session,
      std::make_shared<const PlanStep>(
          nullptr,
          [format, table = tablePath(path, session->config())](
              lp::PlanBuilder& builder, const auto& /*context*/) {
            builder.tableScan(
                SessionState::fileConnectorId(), format, table);
          }));
}

} // namespace

Dataframe DataframeReader::text(const std::string& path) const {
  return scan(session_, kTextSchema, path);
}

Dataframe DataframeReader::parquet(const std::string& path) const {
  return scan(session_, "parquet", path);
}

Dataframe DataframeReader::gensort(const std::string& path) const {
  return scan(session_, kGensortSchema, path);
}

Dataframe DataframeReader::load(const std::string& path) const {
  return scan(session_, format_, path);
}

} // namespace ember
