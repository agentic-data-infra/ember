// Stoke.h in builds without stoke; StokeFunctions.cpp and StokeConnector.cpp
// have it otherwise.

#include "ember/Stoke.h"

namespace ember {

#if !EMBER_STOKE

void registerStokeFunctions() {}

void registerStokeConnector() {}

#endif

} // namespace ember
