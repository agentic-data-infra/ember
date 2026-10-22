// Stage keys: a content hash of each stage, so the same work has the same key
// in any job and any session.
//
// Keys are Merkle hashes over a stage's rewritten plan. A node's key hashes
// its kind, its parameters (its serde JSON without plan node ids, shuffle ids
// and sources), the code version and its children's keys. A scan adds the
// identity of the files it reads: path, size and modification time. A
// ShuffleRead adds the key of the stage it reads, so a stage key covers the
// stage's whole upstream lineage.
//
// Column names are renamed by first use within a stage, so names the
// optimizer makes up (count4, __p3) do not change keys. File column names,
// literals and function names are kept, as are the result's column names.
//
// The code version is the identity of the driver's executable, which links
// Velox, Axiom and the program's functions: any rebuild changes every key.
//
// A node that calls a nondeterministic function (rand, uuid, now, ...) has no
// key, nor does anything that reads it; nor does a scan of files that are not
// local.

#pragma once

#include "ember/driver/StagePlan.h"

namespace ember::driver {

/// Sets the keys of the stages of 'plan', which are in dependency order.
void computeStageKeys(StagePlan& plan);

/// Short form of a key, as explain() shows it.
std::string shortKey(const std::string& key);

} // namespace ember::driver
