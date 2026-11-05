# ember/common

Code shared by the api, driver and worker. Headers are included as `ember/<File>.h`.

| File | Contents |
|------|----------|
| `Runtime` | `initializeRuntime()`: memory manager, file systems, Presto serde, functions, plan node and split serde, shuffle operators, the `ember_file` connector with its text and gensort formats, the `ember_parquet` connector, and the `ember_output` write connector. Idempotent. |
| `FunctionRegistry` | Registers Presto functions (Axiom's base) plus Velox's `sparksql` function package under the `ember_` prefix, and a rewrite hook that resolves scalar calls to those versions, so `split` takes a regex. |
| `TextFileHandler` | Text format for Axiom's file connector: one `value` column per line. Reads a byte range of a file; a range owns the lines that start inside it. |
| `GensortFileHandler` | gensort format (sortbenchmark.org): 100-byte records as binary `key` (10 bytes) and `payload` (90 bytes) columns. A range owns the records that start inside it. |
| `ParquetConnector` | The `ember_parquet` connector behind `read().parquet(path)`: Velox's Hive connector under a small metadata that takes schemas from Parquet footers, so filters reach the reader (row group skipping, subfield pruning, dynamic join filters). Paths are files or directories on any registered file system, including `kiln://`. A table column read as two outputs is read once. |
| `FileSerde` | `FileRangeSplit`, a file split with a byte range (path, start, length), and the registration of the file connector's serde. |
| `CachedTable` | Registry of cached Dataframes (`cache_<n>` to schema and plan). |
| `CachedConnector` | The `ember_cached` connector: gives the optimizer the schema of cached tables, takes no filters, and reads the blocks of their stored output (`driver/README.md`). |
| `OutputConnector` | The `ember_output` connector behind `write().parquet(dir)` and `write().gensort(dir)`: an Axiom table write whose Velox `TableWriter` runs in the stage that makes the rows. Each task writes `part-NNNNN.parquet` (NNNNN = its partition) with Velox's Parquet writer, or `part-NNNNN` as gensort records, to a local or shared directory (temporary name, renamed when done) or to a Kiln chunk; the driver publishes the chunks as a Kiln dataset. |
| `KilnWriteFile` | A Velox `WriteFile` over a new Kiln chunk: appends go through a write-pool buffer, `close()` commits the chunk. |
| `TaskId` | Format and parse of Ember task ids (`ember://<job>/<stage>.<attempt>/<partition>.<attempt>`), so a data sink knows its partition. |
| `RowPages` | `serializeRows`/`deserializeRows`: result rows as Presto pages. |
