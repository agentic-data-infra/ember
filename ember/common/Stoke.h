// stoke (third-party/stoke), the queue that feeds training data to PyTorch,
// in builds with -c ember.stoke=true: the functions that turn text into
// training batches in stoke's format (StokeFunctions.cpp), and the "stoke"
// connector that writes them into streams (StokeConnector.h).

#pragma once

namespace ember {

/// Whether this build has stoke (-c ember.stoke=true).
bool hasStoke();

/// Registers stoke_tokenize and stoke_pack, in a build with stoke. Called by
/// registerFunctions().
void registerStokeFunctions();

/// Registers the "stoke" connector with Velox and Axiom, in a build with
/// stoke. Called by initializeRuntime().
void registerStokeConnector();

} // namespace ember
