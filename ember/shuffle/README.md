# ember/shuffle

Sort shuffle, written as Velox operators, with map outputs in files on the
executor (the default) or in [Kiln](https://github.com/agentic-data-infra/kiln).
The interfaces follow Presto C++'s `ShuffleInterface`.

| File | Contents |
|------|----------|
| `ShuffleInterface` | `BlockId` (shuffle, map, reduce), `MapStatus` (location, bytes per partition of each segment), `ShuffleWriter` (`collect`, `commit`, `abort`) and `BlockTransport` (`fetchBlock(location, block)`). Block URLs: `ember-shuffle://<location>/<shuffle>/<map>/<reduce>`. `FetchFailure` names a block that could not be fetched; it travels in the task's error message as `[fetch-failed <block url>]`. |
| `FileShuffle` | `ShuffleBlockStore`, the per-executor store of shuffle files, `PartitionedPageWriter`, the buffering and spilling that all writers share, the file writer and the local `BlockTransport`. |
| `KilnShuffle` | The Kiln writer, which commits a map output as one Kiln chunk, and `co_fetchKilnBlock`, which reads a block's byte ranges of it. Both use a small pool of Kiln buffers per cluster. |
| `ShuffleWrite` | `ShuffleWriteNode` and operator: partitions its input (hash, round robin, range or single), serializes it to Presto pages and hands them to a `ShuffleWriter`; commits at end of input. |
| `ShuffleRead` | `ShuffleReadNode`: Velox's `Exchange` or `MergeExchange` fed by an `ExchangeSource` that fetches blocks through the `BlockTransport`. Splits are block URLs. A failed fetch fails the task with a `FetchFailure`. |
| `RangePartitionFunction` | Range partitioning for a global sort: split points the driver sampled on a VARCHAR or VARBINARY key or, with `ember.sort.sample=false`, fixed slices of its first 8 bytes, in the sort's direction, nulls first or last. |
| `Shuffle` | `registerShuffle()`: plan node and partition function serde, operator translators and the exchange source factory. |

On disk, map task M of shuffle S writes `shuffle_S_M.data`, frames of
`[uint32 length][Presto page]` grouped by reduce partition, and
`shuffle_S_M.index`, numPartitions + 1 int64 offsets. A writer buffers pages
per partition and spills a run, grouped by partition, when it holds more than
`ember.shuffle.spill_threshold_bytes` (64MB); at commit it concatenates each
partition's runs. Attempts write temp files and rename them; the first commit
wins, so a retried or duplicate task never corrupts a block.

With `ember.shuffle.store=kiln` and `ember.shuffle.kiln.metadata` set to the
Kiln metadata server (`host:port`, or Kiln's URL list
`tcp://host:port,rdma://host:port`), a map task's output goes to the Kiln
cluster instead: one chunk in Kiln's DRAM tier, laid out like the data file.
For shuffles bigger than Kiln's DRAM, set `ember.shuffle.kiln.tier`: in
DRAM alone (`dram`, the default), a tier full of chunks still being written
fails the map tasks, and one full of committed chunks evicts them, which
makes reduce tasks recompute them. `localfs` puts all chunks on Kiln's local
disks; `dram+localfs` puts a map output into DRAM if all of it fits into the
free DRAM blocks, else all of it on local disk, and evicts nothing in DRAM
for it (Kiln's overflow placement; the writer passes the output's size).
Kiln serves local-disk blocks over TCP only, not shared memory or RDMA.
The map task's `MapStatus` names the chunk, `kiln://<metadata>/<chunk>`,
rather than the executor, and the driver turns the partition lengths into
block URLs with byte ranges,
`kiln://<metadata>/<chunk>/<offset>/<length>/<shuffle>/<map>/<reduce>`.
`<metadata>` has its `/` and `%` percent-encoded, so a URL list stays one
part.
With `ember.shuffle.spill=false` (Kiln store only; the default is `true`),
a map task does not spill runs to the executor's directory: each time its
buffered pages pass the spill threshold, it copies them, partition by
partition, into one registered buffer and starts appending it to its chunk
without waiting, so Kiln moves the bytes (over RDMA, one-sided writes of
all the blocks at once) while the map task goes on. The next append, and the
commit, first wait for the one in flight. The chunk is then a sequence of
segments, each laid out partition by partition; the map's `MapStatus` lists
the partition lengths of each segment (an output written at commit is one
segment), and a block URL lists one byte range per segment,
`kiln://<metadata>/<chunk>/<offset>,.../<length>,.../<shuffle>/<map>/<reduce>`,
which the reduce task reads in parallel and chains in order. With
`dram+localfs`, the first segment's size picks the tier.
Reduce tasks read their ranges from Kiln directly; no worker serves these
blocks. So the outputs outlive the executor that wrote them: losing it loses
none of them. A chunk Kiln has evicted fails the read with a `FetchFailure`,
and the driver recomputes that map output as it would a lost one. Writers
spill to the executor's directory either way. Each shuffle's chunks go into
a Kiln bucket of its own (`ember.shuffle.kiln.bucket`, set by the driver),
which the driver deletes with the shuffle: after its job, or for kept
outputs, when they are replaced or uncached or the session ends. Outputs
stored in Kiln's catalog (`ember.reuse.kiln`) stay until Kiln evicts them, as
do those of a driver that died. A Kiln shuffle store is a session setting, so it covers
`cache()` and stored stage outputs too.

Kiln chooses how data moves: shared memory with storage servers on the same
host, else RDMA where the build, the device and the region allow it, else
TCP; metadata calls go over RDMA with an `rdma://` URL. Ember only hands it
memory it can move without copies: each process keeps up to 16 buffers of 4
MiB per cluster from `Client::allocateBuffer` (registered with the NIC under
RDMA). A writer that regroups its output at commit stages appends in two, filling one while Kiln appends the other; a block read that fits goes into one,
handed to Velox as an IOBuf that returns the buffer to the pool when freed.
Larger blocks, and reads while all buffers are in use, read into a fresh
IOBuf registered for that read.

The worker never reads files directly in a reduce task: it fetches whole
blocks through `BlockTransport`: read in process by the local backend, over
RPC (`fetchBlock`) between worker processes.
