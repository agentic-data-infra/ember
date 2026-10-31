#include "ember/DataframeWriter.h"

#include <filesystem>

#include "ember/GensortFileHandler.h"
#include "ember/KilnFiles.h"
#include "ember/Session.h"
#include "ember/Stoke.h"
#include "ember/StokeConnector.h"
#include "ember/shuffle/ShuffleConfig.h"
#include "ember/worker/TaskDescription.h"

namespace ember {

namespace fs = std::filesystem;
namespace lp = facebook::axiom::logical_plan;

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

int64_t DataframeWriter::gensort(const std::string& directory) const {
  return df_.session()->write(
      df_.compile()->physicalPlan,
      OutputFiles{
          kGensortSchema,
          outputDirectory(directory, df_.session()->config())});
}

int64_t DataframeWriter::stream(const std::string& name, bool seal) const {
  VELOX_USER_CHECK(
      hasStoke(), "Writing to a stoke stream needs a build with stoke");
  const auto schema = df_.schema();
  VELOX_USER_CHECK(
      schema->size() == 1 && schema->childAt(0)->isVarbinary(),
      "A stoke stream takes one VARBINARY column of batches, not {}",
      schema->toString());
  const auto& config = df_.session()->config();
  auto metadata = config.find(std::string(kKilnFilesMetadata));
  if (metadata == config.end()) {
    metadata = config.find(std::string(shuffle::kKilnMetadata));
  }
  VELOX_USER_CHECK(
      metadata != config.end(),
      "Writing to a stoke stream needs a Kiln cluster: set {} or {}",
      kKilnFilesMetadata,
      shuffle::kKilnMetadata);

  facebook::axiom::connector::Properties properties{
      {std::string(kStokeKilnMetadata), metadata->second},
      {std::string(kStokeSeal), seal ? "true" : "false"}};
  if (auto chunkBytes = config.find(std::string(kStokeChunkBytesConfig));
      chunkBytes != config.end()) {
    properties[std::string(kStokeChunkBytes)] = chunkBytes->second;
  }
  auto plan = df_.then([name, column = schema->nameOf(0)](
                           auto& builder, const auto& /*context*/) {
                   builder.tableWrite(
                       std::string(kStokeConnectorId),
                       "default",
                       name,
                       lp::WriteKind::kInsert,
                       {"batch"},
                       std::vector<lp::ExprApi>{lp::Col(column)});
                 })
                  .logicalPlan();
  return df_.session()->writeTable(
      plan, {{std::string(kStokeConnectorId), std::move(properties)}});
}

} // namespace ember
