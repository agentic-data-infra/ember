// Stoke.h in builds without stoke; StokeConnector.cpp has it otherwise.

#include "ember/Stoke.h"

namespace ember {

#if !EMBER_STOKE

void registerStokeConnector() {}

#endif

} // namespace ember
