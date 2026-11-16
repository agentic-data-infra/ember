#include "ember/DataframeWriter.h"

#include <filesystem>

#include "ember/GensortFileHandler.h"
#include "ember/KilnFiles.h"
#include "ember/OutputConnector.h"
#include "ember/Session.h"
#include "ember/shuffle/ShuffleConfig.h"

namespace ember {

namespace fs = std::filesystem;
namespace lp = facebook::axiom::logical_plan;
namespace velox = facebook::velox;

namespace {

std::string outputDirectory(
    const std::string& directory,
    const std::map<std::string, std::string>& config) {
  if (isKilnPath(directory)) {
    // The driver checks that the dataset does not exist yet.
    const auto parsed = parseKilnPath(resolveKilnPath(directory, config));
    VELOX_USER_CHECK(
        !parsed.dataset.empty() && parsed.file.empty(),
        "Output to Kiln needs a dataset path, kiln://<metadata>/data/<dataset>: {}",
        directory);
    return kilnPath(parsed.metadata, parsed.dataset);
  }
  if (directory.find("://") != std::string::npos ||
      directory.starts_with("file:")) {
    return directory;
  }
  std::error_code ec;
  auto path = fs::absolute(directory, ec).lexically_normal().string();
  while (path.size() > 1 && path.ends_with('/')) {
    path.pop_back();
  }
  for (const auto& entry : fs::directory_iterator(path, ec)) {
    VELOX_USER_CHECK(
        !entry.path().filename().string().starts_with("part-"),
        "Output directory {} holds part files already",
        path);
  }
  return path;
}

} // namespace

int64_t DataframeWriter::parquet(const std::string& directory) const {
  return files(directory, "parquet");
}

int64_t DataframeWriter::gensort(const std::string& directory) const {
  return files(directory, kGensortSchema);
}

int64_t DataframeWriter::files(
    const std::string& directory,
    const std::string& format) const {
  const auto& config = df_.session()->config();
  const auto output = outputDirectory(directory, config);
  facebook::axiom::connector::Properties properties{
      {std::string(kOutputFormat), format}};
  for (auto key : {kKilnFilesTier, kKilnFilesWriteThrough}) {
    if (auto it = config.find(std::string(key)); it != config.end()) {
      properties[std::string(key)] = it->second;
    }
  }
  const auto schema = df_.schema();
  auto plan = df_.then([output, schema](
                           auto& builder, const auto& /*context*/) {
                   std::vector<lp::ExprApi> columns;
                   for (const auto& name : schema->names()) {
                     columns.push_back(lp::Col(name));
                   }
                   builder.tableWrite(
                       std::string(kOutputConnectorId),
                       "default",
                       output,
                       lp::WriteKind::kCreate,
                       schema->names(),
                       columns);
                 })
                  .logicalPlan();
  return df_.session()->writeTable(
      plan,
      {{std::string(kOutputConnectorId), std::move(properties)}},
      makeOutputTable(output, schema));
}

int64_t DataframeWriter::save(const std::string& name) const {
  VELOX_USER_CHECK(!connector_.empty(), "save() needs a format(connector)");
  facebook::axiom::connector::Properties properties{
      options_.begin(), options_.end()};
  const auto& config = df_.session()->config();
  auto metadata = config.find(std::string(kKilnFilesMetadata));
  if (metadata == config.end()) {
    metadata = config.find(std::string(shuffle::kKilnMetadata));
  }
  if (metadata != config.end()) {
    properties.emplace(std::string(kKilnMetadataProperty), metadata->second);
  }
  const auto schema = df_.schema();
  auto plan = df_.then([connector = connector_, name, schema](
                           auto& builder, const auto& /*context*/) {
                   std::vector<lp::ExprApi> columns;
                   for (const auto& column : schema->names()) {
                     columns.push_back(lp::Col(column));
                   }
                   builder.tableWrite(
                       connector,
                       "default",
                       name,
                       lp::WriteKind::kInsert,
                       schema->names(),
                       columns);
                 })
                  .logicalPlan();
  return df_.session()->writeTable(plan, {{connector_, std::move(properties)}});
}

} // namespace ember
