#include "ember/DataframeWriter.h"

#include <filesystem>

#include "ember/GensortFileHandler.h"
#include "ember/KilnFiles.h"
#include "ember/Session.h"
#include "ember/worker/TaskDescription.h"

namespace ember {

namespace fs = std::filesystem;

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

} // namespace ember
