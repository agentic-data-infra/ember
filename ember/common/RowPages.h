// Rows as serialized pages (Presto serde), as returned by result tasks.

#pragma once

#include <string>

#include "velox/vector/ComplexVector.h"

namespace ember {

/// Serializes 'rows' into one page.
std::string serializeRows(
    const facebook::velox::RowVectorPtr& rows,
    facebook::velox::memory::MemoryPool* pool);

/// Reads back a page written by serializeRows.
facebook::velox::RowVectorPtr deserializeRows(
    const std::string& page,
    const facebook::velox::RowTypePtr& type,
    facebook::velox::memory::MemoryPool* pool);

} // namespace ember
