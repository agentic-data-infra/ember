// SortBenchmark: sorts the 100-byte records of gensort files by their 10-byte
// key (sortbenchmark.org) and writes them to one output file that valsort can
// check.
//
//   scripts/gensort.sh generate 1000000 /data/sort-input 4
//   buck2 run //examples:sort_benchmark -- /data/sort-input /data/sorted [master] [workers]
//   scripts/gensort.sh validate /data/sorted /data/sort-input
//
// master defaults to local[*]. With ember://host:port this program is the
// driver and waits for 'workers' (default 1) ember-worker processes started
// with --driver=host:port.
//
// For now the sort runs in a single task and the driver writes the output.

#include <chrono>
#include <fstream>
#include <iostream>

#include "EnvConfig.h"
#include "ember/Session.h"
#include "ember/functions.h"
#include "velox/vector/FlatVector.h"

using namespace ember;
using namespace ember::functions;

namespace velox = facebook::velox;

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage: sort_benchmark <input file or directory> <output file> "
                 "[master] [workers]"
              << std::endl;
    return 1;
  }

  auto builder = Session::builder();
  builder.appName("SortBenchmark")
      .master(argc > 3 ? argv[3] : "local[*]")
      .config("ember.cluster.workers", argc > 4 ? argv[4] : "1");
  auto ember = examples::withEnvConfig(builder).getOrCreate();

  const auto start = std::chrono::steady_clock::now();
  Dataframe sorted = ember.read().gensort(argv[1]).orderBy(col("key"));
  auto batches = sorted.collectBatches();
  const auto sortedAt = std::chrono::steady_clock::now();

  std::ofstream out(argv[2], std::ios::binary | std::ios::trunc);
  int64_t records = 0;
  for (const auto& batch : batches) {
    auto* keys = batch->childAt(0)->asFlatVector<velox::StringView>();
    auto* payloads = batch->childAt(1)->asFlatVector<velox::StringView>();
    for (velox::vector_size_t i = 0; i < batch->size(); ++i) {
      const auto key = keys->valueAt(i);
      const auto payload = payloads->valueAt(i);
      out.write(key.data(), key.size());
      out.write(payload.data(), payload.size());
    }
    records += batch->size();
  }
  out.close();
  if (!out) {
    std::cerr << "Failed to write " << argv[2] << std::endl;
    return 1;
  }
  const auto end = std::chrono::steady_clock::now();

  const auto ms = [](auto from, auto to) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(to - from)
        .count();
  };
  std::cout << "Sorted " << records << " records (" << records * 100
            << " bytes) in " << ms(start, end) << " ms (sort "
            << ms(start, sortedAt) << " ms, write " << ms(sortedAt, end)
            << " ms)" << std::endl;
  ember.stop();
}
