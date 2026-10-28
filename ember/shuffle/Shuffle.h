// Registers Ember's shuffle with Velox.

#pragma once

namespace ember::shuffle {

/// Registers the ShuffleWrite and ShuffleRead plan nodes (serde and
/// operators), the range partition function's serde and the exchange source
/// for shuffle blocks. Safe to call more
/// than once.
void registerShuffle();

} // namespace ember::shuffle
