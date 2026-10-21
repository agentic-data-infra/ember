# ember/shuffle

File-based sort shuffle, written as Velox operators. The interfaces follow
Presto C++'s `ShuffleInterface`.

| File | Contents |
|------|----------|
| `ShuffleInterface` | `BlockId` (shuffle, map, reduce), `MapStatus` (location, bytes per partition), `ShuffleWriter` (`collect`, `commit`, `abort`) and `BlockTransport` (`fetchBlock(location, block)`). Block URLs: `ember-shuffle://<location>/<shuffle>/<map>/<reduce>`. |
| `FileShuffle` | `ShuffleBlockStore`, the per-executor store of shuffle files, and its writer and the local `BlockTransport`. |
| `ShuffleWrite` | `ShuffleWriteNode` and operator: partitions its input (hash, round robin or single), serializes it to Presto pages and hands them to a `ShuffleWriter`; commits at end of input. |
| `ShuffleRead` | `ShuffleReadNode`: Velox's `Exchange` or `MergeExchange` fed by an `ExchangeSource` that fetches blocks through the `BlockTransport`. Splits are block URLs. |
| `Shuffle` | `registerShuffle()`: plan node serde, operator translators and the exchange source factory. |

On disk, map task M of shuffle S writes `shuffle_S_M.data`, frames of
`[uint32 length][Presto page]` grouped by reduce partition, and
`shuffle_S_M.index`, numPartitions + 1 int64 offsets. A writer buffers pages
per partition and spills a run, grouped by partition, when it holds more than
`ember.shuffle.spill_threshold_bytes` (64MB); at commit it concatenates each
partition's runs. Attempts write temp files and rename them; the first commit
wins, so a retried or duplicate task never corrupts a block.

The worker never reads files directly in a reduce task: it fetches whole
blocks through `BlockTransport`, local today and over RPC once driver and
workers are separate processes.
