// Process-wide setup shared by the driver and executors. Both sides must
// register the same functions, connectors and plan node types, so that a
// plan fragment serialized on the driver deserializes and runs on an executor.

#pragma once

#include <string>

namespace ember {

/// Connector that reads files (text, parquet).
inline constexpr const char* kFileConnectorId = "ember_file";

/// Initializes Velox (memory manager, file systems, vector serdes), registers
/// Ember's functions, the file connector with its text and parquet handlers,
/// plan serialization, and the shuffle operators. Safe to call more than
/// once.
void initializeRuntime();

} // namespace ember
