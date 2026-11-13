// stoke_tokenize and stoke_pack (Stoke.h), in builds with stoke: the
// two steps from text to training batches for a stoke stream.
//
//   stoke_tokenize(text VARCHAR) -> ARRAY<INTEGER>
//     Byte-level tokens: each UTF-8 byte (0-255), then the end-of-text token
//     256 (a vocabulary of 257).
//   stoke_tokenize(text VARCHAR, gpt2 VARCHAR) -> ARRAY<INTEGER>
//     GPT-2's tokens (Gpt2Tokenizer.h), the ids of Hugging Face's
//     tokenizer, then the end-of-text token 50256. 'gpt2', a constant, is
//     the directory of Hugging Face's GPT-2 files vocab.json and merges.txt;
//     a process loads them once for the functions that use them at a time.
//   stoke_pack(tokens ARRAY<INTEGER>, B, T) -> VARBINARY
//     One batch in stoke's format (stoke/format/Batch.h): an int32 tensor of
//     [b, T + 1], b = min(B, number of whole T + 1 windows of the tokens),
//     holding the tokens in order (inputs and shifted targets of T each);
//     tokens past b windows are dropped; NULL if there is no whole window.
//     Concatenate short documents first (e.g. flatten(array_agg(...)) over
//     groups) so that rows fill batches.

#include <algorithm>
#include <cstring>

#include <folly/Indestructible.h>
#include <folly/Synchronized.h>
#include <folly/container/F14Map.h>

#include "ember/Gpt2Tokenizer.h"
#include "ember/Stoke.h"
#include "stoke/format/Batch.h"
#include "velox/common/memory/Memory.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"

namespace ember {

namespace velox = facebook::velox;

namespace {

constexpr int32_t kEndOfText = 256;

template <typename T>
struct StokeTokenize {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void call(
      out_type<velox::Array<int32_t>>& out,
      const arg_type<velox::Varchar>& text) {
    out.reserve(text.size() + 1);
    for (const char c : text) {
      out.push_back(static_cast<unsigned char>(c));
    }
    out.push_back(kEndOfText);
  }
};

// The GPT-2 tokenizer of the files in 'directory', loaded once for the
// functions that use it at a time. The memory of one no function uses is
// freed, so that none is left when the memory manager goes.
std::shared_ptr<const Gpt2Tokenizer> gpt2Tokenizer(
    const std::string& directory) {
  static folly::Indestructible<folly::Synchronized<
      folly::F14FastMap<std::string, std::weak_ptr<const Gpt2Tokenizer>>>>
      tokenizers;
  auto locked = tokenizers->wlock();
  auto& cached = (*locked)[directory];
  auto tokenizer = cached.lock();
  if (tokenizer == nullptr) {
    tokenizer = Gpt2Tokenizer::load(
        directory,
        velox::memory::memoryManager()->addLeafPool(
            "stoke_tokenize_gpt2", /*threadSafe=*/true));
    cached = tokenizer;
  }
  return tokenizer;
}

template <typename T>
struct StokeTokenizeGpt2 {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  void initialize(
      const std::vector<velox::TypePtr>& /*inputTypes*/,
      const velox::core::QueryConfig& /*config*/,
      const arg_type<velox::Varchar>* /*text*/,
      const arg_type<velox::Varchar>* directory) {
    VELOX_USER_CHECK_NOT_NULL(
        directory, "stoke_tokenize's GPT-2 directory must be a constant");
    tokenizer_ = gpt2Tokenizer(std::string(*directory));
  }

  void call(
      out_type<velox::Array<int32_t>>& out,
      const arg_type<velox::Varchar>& text,
      const arg_type<velox::Varchar>& /*directory*/) {
    ids_.clear();
    tokenizer_->encode({text.data(), text.size()}, ids_);
    out.reserve(ids_.size() + 1);
    for (const auto id : ids_) {
      out.push_back(id);
    }
    out.push_back(Gpt2Tokenizer::kEndOfText);
  }

 private:
  std::shared_ptr<const Gpt2Tokenizer> tokenizer_;
  // The ids of the row, reused across rows.
  std::vector<int32_t> ids_;
};

template <typename T>
struct StokePack {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename Size>
  bool call(
      out_type<velox::Varbinary>& out,
      const arg_type<velox::Array<int32_t>>& tokens,
      const Size& batch,
      const Size& sequence) {
    VELOX_USER_CHECK(
        batch > 0 && sequence > 0,
        "stoke_pack needs a positive batch size and sequence length");
    const auto window = static_cast<uint64_t>(sequence) + 1;
    const auto rows = std::min<uint64_t>(
        static_cast<uint64_t>(batch), tokens.size() / window);
    if (rows == 0) {
      return false;
    }
    // The batch is built in 'out': its header in place, then the tokens,
    // copied at once from flat elements without nulls.
    const stoke::format::BatchHeader header{
        .dtype = stoke::format::DType::kInt32, .shape = {rows, window}};
    out.resize(stoke::format::encodedSize(header));
    auto payload = stoke::format::encodeHeader(
        header, {reinterpret_cast<uint8_t*>(out.data()), out.size()});
    const auto count = static_cast<velox::vector_size_t>(rows * window);
    if (!tokens.mayHaveNulls() && tokens.isFlatElements()) {
      const auto* elements =
          tokens.elementsVectorBase()->template asFlatVector<int32_t>();
      std::memcpy(
          payload.data(),
          elements->rawValues() + tokens.offset(),
          payload.size());
    } else {
      for (velox::vector_size_t i = 0; i < count; ++i) {
        const auto token = tokens[i];
        const int32_t value = token.has_value() ? token.value() : 0;
        std::memcpy(
            payload.data() + i * sizeof(int32_t), &value, sizeof(value));
      }
    }
    return true;
  }
};

} // namespace

bool hasStoke() {
  return true;
}

void registerStokeFunctions() {
  velox::registerFunction<
      StokeTokenize,
      velox::Array<int32_t>,
      velox::Varchar>({"stoke_tokenize"});
  velox::registerFunction<
      StokeTokenizeGpt2,
      velox::Array<int32_t>,
      velox::Varchar,
      velox::Varchar>({"stoke_tokenize"});
  velox::registerFunction<
      StokePack,
      velox::Varbinary,
      velox::Array<int32_t>,
      int64_t,
      int64_t>({"stoke_pack"});
  velox::registerFunction<
      StokePack,
      velox::Varbinary,
      velox::Array<int32_t>,
      int32_t,
      int32_t>({"stoke_pack"});
}

} // namespace ember
