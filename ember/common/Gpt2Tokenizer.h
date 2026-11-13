// GPT-2's byte-level BPE tokenizer, with the ids of Hugging Face's
// `tokenizers` for "gpt2".

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "velox/buffer/Buffer.h"
#include "velox/common/memory/MemoryPool.h"

namespace ember {

class Gpt2Tokenizer {
 public:
  /// <|endoftext|>.
  static constexpr int32_t kEndOfText = 50256;

  /// Loads Hugging Face's GPT-2 files vocab.json and merges.txt from
  /// 'directory'; the merge table is allocated from 'pool'.
  static std::shared_ptr<const Gpt2Tokenizer> load(
      const std::string& directory,
      const std::shared_ptr<facebook::velox::memory::MemoryPool>& pool);

  /// Appends the ids of 'text' to 'ids'. As in Hugging Face's tokenizer,
  /// "<|endoftext|>" in the text is kEndOfText. Bytes that are not UTF-8
  /// are tokenized as they are, each on its own.
  void encode(std::string_view text, std::vector<int32_t>& ids) const;

 private:
  Gpt2Tokenizer() = default;

  // Splits the pre-token 'word' into ids, appended to 'ids'.
  void encodeWord(std::string_view word, std::vector<int32_t>& ids) const;

  // The rank and id of the merge of 'left' and 'right', if there is one.
  struct Merge {
    uint32_t rank;
    int32_t id;
  };
  std::optional<Merge> merge(int32_t left, int32_t right) const;

  // The id of each byte on its own.
  std::array<int32_t, 256> byteIds_{};
  // The pool of 'merges_', which it must outlive.
  std::shared_ptr<facebook::velox::memory::MemoryPool> pool_;
  // The merges, an open-addressing hash table of (left, right) -> (rank,
  // id), each slot two uint64_t, kEmpty keys in free ones.
  facebook::velox::BufferPtr merges_;
  uint64_t mask_{0};
};

} // namespace ember
