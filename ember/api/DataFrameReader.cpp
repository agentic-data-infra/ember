#include "ember/DataFrameReader.h"

#include <filesystem>

#include "ember/DataFrame.h"
#include "ember/Session.h"
#include "ember/TextFileHandler.h"

namespace ember {

namespace lp = facebook::axiom::logical_plan;

namespace {

// Local paths become absolute; the file connector reads a directory when its
// path ends in '/'.
std::string tablePath(const std::string& path) {
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

DataFrame scan(
    const std::shared_ptr<SessionState>& session,
    const std::string& format,
    const std::string& path) {
  return DataFrame(
      session,
      std::make_shared<const PlanStep>(PlanStep{
          nullptr,
          [format, table = tablePath(path)](
              lp::PlanBuilder& builder, const auto& /*context*/) {
            builder.tableScan(
                SessionState::fileConnectorId(), format, table);
          }}));
}

} // namespace

DataFrame DataFrameReader::text(const std::string& path) const {
  return scan(session_, kTextSchema, path);
}

DataFrame DataFrameReader::parquet(const std::string& path) const {
  return scan(session_, "parquet", path);
}

DataFrame DataFrameReader::load(const std::string& path) const {
  return scan(session_, format_, path);
}

} // namespace ember
