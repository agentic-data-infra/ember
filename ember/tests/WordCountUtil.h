// WordCount pieces shared by tests: the example program, a reference count by
// a shell pipeline, and a generated corpus.

#pragma once

#include <cstdio>
#include <filesystem>
#include <fstream>
#include <map>
#include <random>
#include <string>
#include <vector>

#include "ember/Session.h"
#include "ember/functions.h"

namespace ember::test {

/// The example program from examples/WordCount.cpp.
inline Dataframe wordCount(const Session& session, const std::string& path) {
  using namespace ember::functions;
  Dataframe lines = session.read().text(path);
  Dataframe words =
      lines.select(explode(split(lower(col("value")), R"(\W+)")).as("word"))
          .filter(col("word") != "");
  return words.groupBy("word").count().orderBy(col("count").desc());
}

inline std::map<std::string, int64_t> toMap(const std::vector<Row>& rows) {
  std::map<std::string, int64_t> result;
  for (const auto& row : rows) {
    result[row.getString(0)] = row.getLong(1);
  }
  return result;
}

/// Counts words with tr, sort and uniq -c. \W+ splits on anything but
/// [A-Za-z0-9_], which is what tr -cs keeps.
inline std::map<std::string, int64_t> shellWordCount(
    const std::filesystem::path& path) {
  auto command = fmt::format(
      "find '{}' -type f ! -name '_*' ! -name '.*' -exec cat {{}} + | "
      "LC_ALL=C tr -cs 'A-Za-z0-9_' '\\n' | LC_ALL=C tr 'A-Z' 'a-z' | "
      "grep -v '^$' | LC_ALL=C sort | uniq -c",
      path.string());
  std::map<std::string, int64_t> result;
  auto* pipe = ::popen(command.c_str(), "r");
  VELOX_CHECK_NOT_NULL(pipe);
  char word[4096];
  long count;
  while (::fscanf(pipe, "%ld %4095s", &count, word) == 2) {
    result[word] = count;
  }
  VELOX_CHECK_EQ(::pclose(pipe), 0);
  return result;
}

/// Writes a corpus of random words in mixed case with punctuation, blank
/// lines and CRLF line ends, spread over 'numFiles' files in 'dir'.
inline void writeCorpus(
    const std::filesystem::path& dir,
    int32_t numFiles,
    int32_t linesPerFile) {
  const std::vector<std::string> vocabulary = {
      "the", "quick", "Brown", "fox", "JUMPS", "over", "lazy", "dog",
      "ember", "velox", "axiom", "flare", "x1", "snake_case", "a"};
  const std::vector<std::string> separators = {
      " ", "  ", ", ", ". ", "!", " -- ", "\t", "'", "(", ") "};
  std::mt19937 rng(42);
  for (int32_t f = 0; f < numFiles; ++f) {
    std::string content;
    for (int32_t l = 0; l < linesPerFile; ++l) {
      const auto numWords = rng() % 12;
      for (uint32_t w = 0; w < numWords; ++w) {
        content += vocabulary[rng() % vocabulary.size()];
        content += separators[rng() % separators.size()];
      }
      content += (l % 7 == 0) ? "\r\n" : "\n";
    }
    std::ofstream(dir / fmt::format("part-{:05d}.txt", f), std::ios::binary)
        << content;
  }
}

} // namespace ember::test
