// Stoke.h in builds without stoke; StokeFunctions.cpp has it otherwise.

#include "ember/Stoke.h"

namespace ember {

#if !EMBER_STOKE

void registerStokeFunctions() {}

#endif

} // namespace ember
