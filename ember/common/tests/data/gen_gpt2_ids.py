"""Generates gpt2_ids.json, the ids Hugging Face's `tokenizers` gives for
GPT-2 to edge cases and to the beginnings of FineWeb-Edu documents, for
Gpt2TokenizerTest. Its vocab.json and merges.txt are in gpt2/.

    python gen_gpt2_ids.py FINEWEB_EDU.parquet
"""

import json
import os
import sys

import pyarrow.parquet as pq
from tokenizers import Tokenizer

HERE = os.path.dirname(os.path.abspath(__file__))

EDGE_CASES = [
    "",
    "a",
    "Hello world",
    "  leading spaces",
    "trailing spaces   ",
    "tabs\tand\nnew\r\nlines\n\n\nend",
    "I'm you're we've they'll he'd it's can't DON'T 'S 're'",
    "x's rock'n'roll ''quotes'' '",
    "1234567890 3.14159 1,000,000 v2.0 -42",
    "Unicode: naïve café Ünïcödé Ελληνικά русский 中文 日本語 한국어 العربية",
    "Numbers: ٣٤ ½ Ⅻ ²",
    "Emoji: 😀👍🏽 🇨🇭 👨‍👩‍👧 ❤️",
    "Spaces:  　| |\u0085|",
    "Punctuation!!! ??? ... --- (([{}])) @#$%^&*",
    "mixed123abc ABC123 a1b2c3",
    "  \n  \t x",
    "word  \n",
    "combining: é ä ñ",
    "https://example.com/path?query=1&b=2#frag",
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "=================================================================",
    "<|endoftext|>",
    "one<|endoftext|>two <|endoftext|><|endoftext|> three<|endoftext",
]


def main():
    tokenizer = Tokenizer.from_pretrained("gpt2")

    texts = list(EDGE_CASES)
    rows = pq.ParquetFile(sys.argv[1]).read_row_group(0, columns=["text"])
    rows = rows["text"]
    texts += [text[:1500] for text in rows.to_pylist()[:40]]
    cases = []
    for text in texts:
        cases.append({"text": text, "ids": tokenizer.encode(text).ids})
    with open(os.path.join(HERE, "gpt2_ids.json"), "w") as f:
        json.dump(cases, f, ensure_ascii=False, indent=0)


if __name__ == "__main__":
    main()
