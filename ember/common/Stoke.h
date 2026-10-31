// stoke (third-party/stoke), the queue that feeds training data to PyTorch,
// in builds with -c ember.stoke=true: the functions that turn text into
// training batches in stoke's format (StokeFunctions.cpp).

#pragma once

namespace ember {

/// Registers stoke_tokenize and stoke_pack, in a build with stoke. Called by
/// registerFunctions().
void registerStokeFunctions();

} // namespace ember
