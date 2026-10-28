// SortBenchmark: sorts the 100-byte records of gensort files by their 10-byte
// key (sortbenchmark.org) and writes them as part files that valsort can
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
// The sort runs as one merge task writing part-00000, or, with
// EMBER_CONF=ember.sort.range_partitions=N, as N range-sorted tasks writing
// part-00000 to part-N-1. The rows do not pass through the driver.
//
// To sort more than fits in memory, add "spill_enabled=true
// query_max_memory_per_node=<bytes per task, e.g. 2GB>", and keep
// ember.shuffle.spill_threshold_bytes (64MB by default) well below that
// limit: a map task holds both its sort and its shuffle buffer.

#include <chrono>
#include <iostream>

#include "EnvConfig.h"
#include "ember/Session.h"
#include "ember/functions.h"

using namespace ember;
using namespace ember::functions;

int main(int argc, char** argv) {
  if (argc < 3) {
    std::cerr << "Usage: sort_benchmark <input file or directory> "
                 "<output directory> [master] [workers]"
              << std::endl;
    return 1;
  }

  auto builder = Session::builder();
  builder.appName("SortBenchmark")
      .master(argc > 3 ? argv[3] : "local[*]")
      .config("ember.cluster.workers", argc > 4 ? argv[4] : "1");
  auto ember = examples::withEnvConfig(builder).getOrCreate();

  const auto start = std::chrono::steady_clock::now();
  const auto records = ember.read()
                           .gensort(argv[1])
                           .orderBy(col("key"))
                           .write()
                           .gensort(argv[2]);
  const auto end = std::chrono::steady_clock::now();

  std::cout << "Sorted " << records << " records (" << records * 100
            << " bytes) in "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   end - start)
                   .count()
            << " ms" << std::endl;
  ember.stop();
}
