// stoke (third-party/stoke), the queue that feeds training data to PyTorch,
// in builds with -c ember.stoke=true: the "stoke" connector, which writes
// training batches into streams (StokeConnector.h).

#pragma once

namespace ember {

/// Registers the "stoke" connector with Velox and Axiom, in a build with
/// stoke. Called by initializeRuntime().
void registerStokeConnector();

} // namespace ember
