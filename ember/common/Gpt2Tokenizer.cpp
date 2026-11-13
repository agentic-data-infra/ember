#include "ember/Gpt2Tokenizer.h"

#include <folly/FileUtil.h>
#include <folly/String.h>
#include <folly/container/F14Map.h>
#include <folly/hash/Hash.h>
#include <folly/json.h>
#include <folly/lang/Bits.h>

#include "velox/common/base/Exceptions.h"
#include "velox/external/utf8proc/utf8procImpl.h"

namespace ember {

namespace velox = facebook::velox;

namespace {

constexpr uint64_t kEmpty = ~uint64_t{0};

// GPT-2's pre-tokenizer splits text into runs of letters, of numbers, of
// other characters, and of whitespace, the regex
//   's|'t|'re|'ve|'m|'ll|'d| ?\p{L}+| ?\p{N}+| ?[^\s\p{L}\p{N}]+|\s+(?!\S)|\s+
enum class Class : uint8_t { kLetter, kNumber, kSpace, kOther };

struct CodePoint {
  Class cls;
  uint32_t size;
};

constexpr std::array<Class, 128> asciiClasses() {
  std::array<Class, 128> classes{};
  for (int c = 0; c < 128; ++c) {
    if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
      classes[c] = Class::kLetter;
    } else if (c >= '0' && c <= '9') {
      classes[c] = Class::kNumber;
    } else if (c == ' ' || (c >= '\t' && c <= '\r')) {
      classes[c] = Class::kSpace;
    } else {
      classes[c] = Class::kOther;
    }
  }
  return classes;
}

constexpr auto kAsciiClasses = asciiClasses();

Class classOf(utf8proc_int32_t c) {
  if (c == 0x85) {
    return Class::kSpace;
  }
  switch (utf8proc_category(c)) {
    case UTF8PROC_CATEGORY_LU:
    case UTF8PROC_CATEGORY_LL:
    case UTF8PROC_CATEGORY_LT:
    case UTF8PROC_CATEGORY_LM:
    case UTF8PROC_CATEGORY_LO:
      return Class::kLetter;
    case UTF8PROC_CATEGORY_ND:
    case UTF8PROC_CATEGORY_NL:
    case UTF8PROC_CATEGORY_NO:
      return Class::kNumber;
    case UTF8PROC_CATEGORY_ZS:
    case UTF8PROC_CATEGORY_ZL:
    case UTF8PROC_CATEGORY_ZP:
      return Class::kSpace;
    default:
      return Class::kOther;
  }
}

// The class and size of the character at 'pos'; a byte that does not start
// a UTF-8 character is one of its own.
CodePoint at(std::string_view text, size_t pos) {
  const auto byte = static_cast<unsigned char>(text[pos]);
  if (byte < 0x80) {
    return {kAsciiClasses[byte], 1};
  }
  utf8proc_int32_t c;
  const auto size = utf8proc_iterate(
      reinterpret_cast<const utf8proc_uint8_t*>(text.data() + pos),
      text.size() - pos,
      &c);
  if (size <= 0) {
    return {Class::kOther, 1};
  }
  return {classOf(c), static_cast<uint32_t>(size)};
}

// The end of the run of 'cls' from 'pos'.
size_t runEnd(std::string_view text, size_t pos, Class cls) {
  while (pos < text.size()) {
    const auto c = at(text, pos);
    if (c.cls != cls) {
      break;
    }
    pos += c.size;
  }
  return pos;
}

// The end of the contraction ('s, 't, 're, 've, 'm, 'll or 'd) at 'pos',
// or 0.
size_t contractionEnd(std::string_view text, size_t pos) {
  const auto rest = text.substr(pos + 1);
  if (!rest.empty() &&
      (rest[0] == 's' || rest[0] == 't' || rest[0] == 'm' || rest[0] == 'd')) {
    return pos + 2;
  }
  const auto two = rest.substr(0, 2);
  if (two == "re" || two == "ve" || two == "ll") {
    return pos + 3;
  }
  return 0;
}

// The end of the pre-token at 'pos', as the regex above matches it.
size_t preTokenEnd(std::string_view text, size_t pos) {
  if (text[pos] == '\'') {
    if (const auto end = contractionEnd(text, pos); end != 0) {
      return end;
    }
  }
  const auto c = at(text, pos);
  if (c.cls != Class::kSpace) {
    return runEnd(text, pos + c.size, c.cls);
  }
  if (text[pos] == ' ' && pos + 1 < text.size()) {
    // A space before a run belongs to it.
    if (const auto next = at(text, pos + 1); next.cls != Class::kSpace) {
      return runEnd(text, pos + 1 + next.size, next.cls);
    }
  }
  // Whitespace, but for its last character if a run follows: that goes
  // with the run if it is a space, else on its own.
  size_t last = pos;
  size_t end = pos;
  while (end < text.size()) {
    const auto w = at(text, end);
    if (w.cls != Class::kSpace) {
      break;
    }
    last = end;
    end += w.size;
  }
  return end < text.size() && last > pos ? last : end;
}

// GPT-2 maps each byte to a printable character for its vocabulary: bytes
// that are printable to themselves, the others to 256 and up in order.
// Returns the byte of each such character.
std::array<int16_t, 512> bytesOfCharacters() {
  std::array<int16_t, 512> bytes;
  bytes.fill(-1);
  int next = 256;
  for (int b = 0; b < 256; ++b) {
    const bool printable = (b >= '!' && b <= '~') || (b >= 0xA1 && b <= 0xAC) ||
        (b >= 0xAE && b <= 0xFF);
    bytes[printable ? b : next++] = static_cast<int16_t>(b);
  }
  return bytes;
}

// The bytes of a token as the vocabulary writes it.
std::string bytesOf(std::string_view token) {
  static const auto kBytes = bytesOfCharacters();
  std::string bytes;
  for (size_t pos = 0; pos < token.size();) {
    utf8proc_int32_t c;
    const auto size = utf8proc_iterate(
        reinterpret_cast<const utf8proc_uint8_t*>(token.data() + pos),
        token.size() - pos,
        &c);
    VELOX_USER_CHECK(
        size > 0 && c < static_cast<int32_t>(kBytes.size()) && kBytes[c] >= 0,
        "Not a GPT-2 token: {}",
        token);
    bytes.push_back(static_cast<char>(kBytes[c]));
    pos += size;
  }
  return bytes;
}

std::string readFile(const std::string& path) {
  std::string contents;
  VELOX_USER_CHECK(
      folly::readFile(path.c_str(), contents), "Cannot read {}", path);
  return contents;
}

} // namespace

std::shared_ptr<const Gpt2Tokenizer> Gpt2Tokenizer::load(
    const std::string& directory,
    const std::shared_ptr<velox::memory::MemoryPool>& pool) {
  folly::F14FastMap<std::string, int32_t> ids;
  const auto vocab = folly::parseJson(readFile(directory + "/vocab.json"));
  for (const auto& [token, id] : vocab.items()) {
    ids.emplace(bytesOf(token.asString()), static_cast<int32_t>(id.asInt()));
  }
  auto idOf = [&](const std::string& bytes) {
    const auto it = ids.find(bytes);
    VELOX_USER_CHECK(
        it != ids.end(),
        "No id for {} in {}",
        folly::cEscape<std::string>(bytes),
        directory);
    return it->second;
  };

  std::shared_ptr<Gpt2Tokenizer> tokenizer{new Gpt2Tokenizer()};
  for (int b = 0; b < 256; ++b) {
    tokenizer->byteIds_[b] = idOf(std::string(1, static_cast<char>(b)));
  }

  std::vector<std::string_view> lines;
  const auto merges = readFile(directory + "/merges.txt");
  folly::split('\n', merges, lines);
  std::vector<std::array<int32_t, 3>> pairs;
  for (const auto line : lines) {
    if (line.empty() || line.starts_with("#version")) {
      continue;
    }
    const auto space = line.find(' ');
    VELOX_USER_CHECK(
        space != std::string_view::npos,
        "Not a merge in {}: {}",
        directory,
        line);
    const auto left = bytesOf(line.substr(0, space));
    const auto right = bytesOf(line.substr(space + 1));
    pairs.push_back({idOf(left), idOf(right), idOf(left + right)});
  }

  tokenizer->pool_ = pool;
  const auto capacity = folly::nextPowTwo(2 * pairs.size());
  tokenizer->mask_ = capacity - 1;
  tokenizer->merges_ = velox::AlignedBuffer::allocate<uint64_t>(
      2 * capacity, pool.get(), kEmpty);
  auto* slots = tokenizer->merges_->asMutable<uint64_t>();
  for (uint32_t rank = 0; rank < pairs.size(); ++rank) {
    const auto [left, right, id] = pairs[rank];
    const auto key = (uint64_t{static_cast<uint32_t>(left)} << 32) |
        static_cast<uint32_t>(right);
    auto slot = folly::hash::twang_mix64(key) & tokenizer->mask_;
    while (slots[2 * slot] != kEmpty && slots[2 * slot] != key) {
      slot = (slot + 1) & tokenizer->mask_;
    }
    // The first of duplicate merges counts.
    if (slots[2 * slot] == kEmpty) {
      slots[2 * slot] = key;
      slots[2 * slot + 1] =
          (uint64_t{rank} << 32) | static_cast<uint32_t>(id);
    }
  }
  return tokenizer;
}

std::optional<Gpt2Tokenizer::Merge> Gpt2Tokenizer::merge(
    int32_t left,
    int32_t right) const {
  const auto* slots = merges_->as<uint64_t>();
  const auto key = (uint64_t{static_cast<uint32_t>(left)} << 32) |
      static_cast<uint32_t>(right);
  for (auto slot = folly::hash::twang_mix64(key) & mask_;;
       slot = (slot + 1) & mask_) {
    if (slots[2 * slot] == key) {
      const auto value = slots[2 * slot + 1];
      return Merge{
          static_cast<uint32_t>(value >> 32), static_cast<int32_t>(value)};
    }
    if (slots[2 * slot] == kEmpty) {
      return std::nullopt;
    }
  }
}

void Gpt2Tokenizer::encode(std::string_view text, std::vector<int32_t>& ids)
    const {
  constexpr std::string_view kEndOfTextToken = "<|endoftext|>";
  for (;;) {
    // The text up to the token is tokenized on its own.
    const auto special = std::min(text.find(kEndOfTextToken), text.size());
    const auto part = text.substr(0, special);
    for (size_t pos = 0; pos < part.size();) {
      const auto end = preTokenEnd(part, pos);
      encodeWord(part.substr(pos, end - pos), ids);
      pos = end;
    }
    if (special == text.size()) {
      return;
    }
    ids.push_back(kEndOfText);
    text.remove_prefix(special + kEndOfTextToken.size());
  }
}

void Gpt2Tokenizer::encodeWord(std::string_view word, std::vector<int32_t>& ids)
    const {
  const auto start = ids.size();
  for (const char byte : word) {
    ids.push_back(byteIds_[static_cast<unsigned char>(byte)]);
  }
  // Merges the pair of the lowest rank, the first of equal ones, until no
  // pair merges.
  while (ids.size() - start > 1) {
    std::optional<Merge> best;
    size_t at = 0;
    for (auto i = start; i + 1 < ids.size(); ++i) {
      const auto merged = merge(ids[i], ids[i + 1]);
      if (merged.has_value() &&
          (!best.has_value() || merged->rank < best->rank)) {
        best = merged;
        at = i;
      }
    }
    if (!best.has_value()) {
      break;
    }
    ids[at] = best->id;
    ids.erase(ids.begin() + static_cast<ptrdiff_t>(at) + 1);
  }
}

} // namespace ember
