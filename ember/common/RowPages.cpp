#include "ember/RowPages.h"

#include "velox/common/memory/ByteStream.h"
#include "velox/vector/VectorStream.h"

namespace ember {

namespace velox = facebook::velox;

namespace {
velox::VectorSerde* serde() {
  return velox::getNamedVectorSerde("Presto");
}
} // namespace

std::string serializeRows(
    const velox::RowVectorPtr& rows,
    velox::memory::MemoryPool* pool) {
  velox::VectorStreamGroup group(pool, serde());
  group.createStreamTree(velox::asRowType(rows->type()), rows->size());
  group.append(rows);
  velox::IOBufOutputStream stream(*pool);
  group.flush(&stream);
  auto buffer = stream.getIOBuf();
  std::string page;
  page.reserve(buffer->computeChainDataLength());
  for (auto range : *buffer) {
    page.append(reinterpret_cast<const char*>(range.data()), range.size());
  }
  return page;
}

velox::RowVectorPtr deserializeRows(
    const std::string& page,
    const velox::RowTypePtr& type,
    velox::memory::MemoryPool* pool) {
  std::vector<velox::ByteRange> ranges{velox::ByteRange{
      reinterpret_cast<uint8_t*>(const_cast<char*>(page.data())),
      static_cast<int32_t>(page.size()),
      0}};
  velox::BufferInputStream stream(std::move(ranges));
  velox::RowVectorPtr result;
  serde()->deserialize(&stream, pool, type, &result);
  return result;
}

} // namespace ember
