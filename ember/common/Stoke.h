// stoke (third-party/stoke), the queue that feeds training data to PyTorch,
// in builds with -c ember.stoke=true: the "stoke" connector, which writes
// training batches into streams (StokeConnector.h).

#pragma once

#include <cstdint>

namespace ember {

/// How a write packs tokens into stoke batches: int32 batches of 'batch'
/// windows of 'sequence' + 1 tokens (inputs and shifted targets of
/// 'sequence' each), as stoke's Packer makes them.
struct StokePacking {
  uint64_t batch;
  uint64_t sequence;
};

/// Whether this build has stoke (-c ember.stoke=true).
bool hasStoke();

/// Registers the "stoke" connector with Velox and Axiom, in a build with
/// stoke. Called by initializeRuntime().
void registerStokeConnector();

} // namespace ember
