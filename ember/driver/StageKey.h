// Stage keys: a content hash of each stage, so the same work has the same key
// in any job and any session.
//
// Keys are Merkle hashes over a stage's rewritten plan. A node's key hashes
// its kind, its parameters (its serde JSON without plan node ids, shuffle ids
// and sources), the code version and its children's keys. A scan adds the
// identity of the files it reads: path, size and modification time. A
// exchange adds the key of the stage it reads, so a stage key covers the
// stage's whole upstream lineage.
//
// Column names are renamed by first use within a stage, so names the
// optimizer makes up (count4, __p3) do not change keys. File column names,
// literals and function names are kept, as are the result's column names.
//
// The code version is the identity of the file holding the driver's code, the
// executable or the Python extension, which links Velox, Axiom and the
// program's functions: any rebuild changes every key.
// Query config that can change results (e.g. the session time zone) goes into
// every key too.
//
// A node that calls a nondeterministic function (rand, uuid, now, ...) has no
// key, nor does anything that reads it; nor does a scan of files that are not
// local, or that are node-local (DagScheduler::kNodeLocalFiles in 'config'),
// since the driver does not see them.

#pragma once

#include <filesystem>

#include "ember/driver/StagePlan.h"

namespace ember::driver {

/// Sets the keys of the stages of 'plan', which are in dependency order.
/// 'config' is the query config that can change results.
void computeStageKeys(
    StagePlan& plan,
    const std::map<std::string, std::string>& config = {});

/// Short form of a key, as explain() shows it.
/// The file holding Ember's code: the executable, or the Python extension
/// loaded into the interpreter. Its identity is the code version.
std::filesystem::path codeFile();

std::string shortKey(const std::string& key);

} // namespace ember::driver
