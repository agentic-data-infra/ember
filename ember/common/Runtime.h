// Process-wide setup shared by the driver and executors. Both sides must
// register the same functions, connectors and plan node types, so that a
// plan fragment serialized on the driver deserializes and runs on an executor.

#pragma once

#include <string>

namespace ember {

/// Connector that reads files (text, parquet).
inline constexpr const char* kFileConnectorId = "ember_file";

/// Session config: the bytes this process may use (see initializeRuntime()).
inline constexpr const char* kWorkerMemoryBytes = "ember.worker.memory_bytes";

/// Initializes Velox (memory manager, file systems, vector serdes), registers
/// Ember's functions, the file connector with its text and parquet handlers,
/// plan serialization, and the shuffle operators. Safe to call more than
/// once.
///
/// 'memoryBytes' is what the process may use, by default 80% of the memory of
/// the machine or cgroup. Kiln's buffer pools and 10% for spilling come off the
/// top, and the rest is the capacity of the memory arbitrator, which all tasks
/// share. The first call sets it for the process's lifetime;
/// a later call that asks for a different value fails.
void initializeRuntime(int64_t memoryBytes = 0);

} // namespace ember
