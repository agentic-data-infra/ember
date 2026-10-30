// Datasets in Kiln (third-party/kiln) as files, for reading inputs and
// writing results there.
//
// A path kiln://<metadata>/data/<dataset>/<file> names file <file> of the
// current version of Kiln dataset <dataset> (kiln::catalog: a ref pointing at
// a manifest of named files), and kiln://<metadata>/data/<dataset>/ the
// dataset as a directory. <metadata> is the Kiln cluster's metadata server
// (as shuffle::kilnClient(), with '/' and '%' escaped as in kilnLocation());
// empty, the program's configured cluster (resolveKilnPath()).
//
// The "kiln" file system reads them: it lists datasets and their files and
// opens a file as a range of its chunk. Files are not written through it: a
// write() to a kiln:// directory writes each part as a chunk, and the driver
// publishes them as a new dataset at the end of the job, so the dataset
// appears with all its parts or not at all.

#pragma once

#include <map>
#include <string>
#include <string_view>

#include "kiln/common/Types.h"

namespace ember {

/// Config key: the Kiln cluster of kiln:// paths without one; default
/// ember.shuffle.kiln.metadata.
inline constexpr std::string_view kKilnFilesMetadata = "ember.kiln.metadata";
/// Config key: the Kiln tier written parts go to: "dram" (default) or
/// "localfs".
inline constexpr std::string_view kKilnFilesTier = "ember.kiln.files.tier";
/// Config key: the tier written parts are also written through to, e.g.
/// "localfs" so that they outlive DRAM eviction; none by default.
inline constexpr std::string_view kKilnFilesWriteThrough =
    "ember.kiln.files.write_through";

struct KilnPath {
  std::string metadata;
  std::string dataset;
  /// Empty for the dataset itself, or for the list of datasets if 'dataset'
  /// is empty too.
  std::string file;
};

/// Whether 'path' is a kiln:// dataset path (and not, e.g., a shuffle
/// location).
bool isKilnPath(std::string_view path);

/// Throws a user error for a path that is not one.
KilnPath parseKilnPath(std::string_view path);

/// The kiln:// path of 'file' of 'dataset', or of the dataset if 'file' is
/// empty (ending in '/').
std::string kilnPath(
    const std::string& metadata,
    const std::string& dataset,
    const std::string& file = "");

/// 'path' with the configured Kiln cluster if it has none (kKilnFilesMetadata
/// in 'config'); any other path as it is.
std::string resolveKilnPath(
    const std::string& path,
    const std::map<std::string, std::string>& config);

/// Where written parts go in Kiln, by kKilnFilesTier and
/// kKilnFilesWriteThrough in 'config'. Manifests go there too.
kiln::Placement kilnFilesPlacement(
    const std::map<std::string, std::string>& config);

/// Registers the "kiln" Velox file system. Called by initializeRuntime().
void registerKilnFileSystem();

} // namespace ember
