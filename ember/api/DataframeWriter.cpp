#include "ember/DataframeWriter.h"

#include <filesystem>

#include "ember/GensortFileHandler.h"
#include "ember/Session.h"
#include "ember/worker/TaskDescription.h"

namespace ember {

namespace fs = std::filesystem;

namespace {

std::string outputDirectory(const std::string& directory) {
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
      OutputFiles{kGensortSchema, outputDirectory(directory)});
}

} // namespace ember
