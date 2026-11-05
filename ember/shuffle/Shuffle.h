// Registers Ember's shuffle with Velox.

#pragma once

namespace ember::shuffle {

/// Registers the ShuffleWrite plan node (serde and operator), the range
/// partition function's serde and the exchange source for shuffle blocks. Safe
/// to call more than once.
void registerShuffle();

} // namespace ember::shuffle
