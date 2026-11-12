#include "ember/Runtime.h"

#include <gtest/gtest.h>
#include "velox/common/memory/Memory.h"

namespace ember {
namespace {

using namespace facebook::velox;

// A buffer of several MB, freed and allocated again, reuses the pages it had
// instead of mapping new ones.
TEST(RuntimeTest, reusesPagesOfLargeBuffers) {
  initializeRuntime();
  auto* manager = memory::memoryManager();
  auto pool = manager->addLeafPool("reusesPagesOfLargeBuffers");
  auto& allocator = *manager->allocator();
  constexpr int64_t kBytes = 6 << 20;

  void* data = pool->allocate(kBytes);
  const auto mapped = allocator.numMapped();
  pool->free(data, kBytes);
  EXPECT_EQ(allocator.numMapped(), mapped);

  data = pool->allocate(kBytes);
  EXPECT_EQ(allocator.numMapped(), mapped);
  pool->free(data, kBytes);
}

} // namespace
} // namespace ember
