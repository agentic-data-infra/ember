// stoke_tokenize and stoke_pack (Stoke.h), in builds with stoke: the
// two steps from text to training batches for a stoke stream.
//
//   stoke_tokenize(text VARCHAR) -> ARRAY<INTEGER>
//     Byte-level tokens: each UTF-8 byte (0-255), then the end-of-text token
//     256 (a vocabulary of 257).
//   stoke_pack(tokens ARRAY<INTEGER>, B, T) -> VARBINARY
//     One batch in stoke's format (stoke/format/Batch.h): an int32 tensor of
//     [b, T + 1], b = min(B, number of whole T + 1 windows of the tokens),
//     holding the tokens in order (inputs and shifted targets of T each);
//     tokens past b windows are dropped; NULL if there is no whole window.
//     Concatenate short documents first (e.g. flatten(array_agg(...)) over
//     groups) so that rows fill batches.

#include <algorithm>
#include <cstring>

#include "ember/Stoke.h"
#include "stoke/format/Batch.h"
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
    values_.resize(rows * window);
    for (size_t i = 0; i < values_.size(); ++i) {
      const auto token = tokens[i];
      values_[i] = token.has_value() ? token.value() : 0;
    }
    const auto encoded = stoke::format::encodeBatch(
        {.dtype = stoke::format::DType::kInt32, .shape = {rows, window}},
        {reinterpret_cast<const uint8_t*>(values_.data()),
         values_.size() * sizeof(int32_t)});
    out.resize(encoded.size());
    std::memcpy(out.data(), encoded.data(), encoded.size());
    return true;
  }

 private:
  std::vector<int32_t> values_;
};

} // namespace

void registerStokeFunctions() {
  velox::registerFunction<
      StokeTokenize,
      velox::Array<int32_t>,
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
