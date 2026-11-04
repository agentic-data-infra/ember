# ember/common

Code shared by the api, driver and worker. Headers are included as `ember/<File>.h`.

| File | Contents |
|------|----------|
| `Runtime` | `initializeRuntime()`: memory manager, file systems, Presto serde, functions, plan node and split serde, shuffle operators, the `ember_file` connector with its text, gensort and parquet formats, and the `ember_output` write connector. Idempotent. |
| `FunctionRegistry` | Registers Presto functions (Axiom's base) plus Velox's `sparksql` function package under the `ember_` prefix, and a rewrite hook that resolves scalar calls to those versions, so `split` takes a regex. |
| `TextFileHandler` | Text format for Axiom's file connector: one `value` column per line. Reads a byte range of a file; a range owns the lines that start inside it. |
| `GensortFileHandler` | gensort format (sortbenchmark.org): 100-byte records as binary `key` (10 bytes) and `payload` (90 bytes) columns. A range owns the records that start inside it. |
| `ParquetFileHandler` | Axiom's parquet format, but reads the file columns named by the column handles, so scans whose columns Axiom renamed (one file scanned twice in a plan) work. Registered in place of Axiom's handler. |
| `FileSerde` | Serializable versions of the file connector's table handle, column handle and split (`FileRangeSplit`: path, start, length), so tasks travel as bytes. |
| `CachedTable` | Registry of cached Dataframes (`cache_<n>` to schema and plan), and the `cached` file connector format that gives the optimizer their schema. The driver replaces their scans; they are never read as files. |
| `OutputConnector` | The `ember_output` connector behind `write().parquet(dir)`: an Axiom table write whose Velox `TableWriter` runs in the stage that makes the rows. Each task writes `part-NNNNN.parquet` (NNNNN = its partition) with Velox's Parquet writer, to a local or shared directory (temporary name, renamed when done) or to a Kiln chunk; the driver publishes the chunks as a Kiln dataset. |
| `KilnWriteFile` | A Velox `WriteFile` over a new Kiln chunk: appends go through a write-pool buffer, `close()` commits the chunk. |
| `TaskId` | Format and parse of Ember task ids (`ember://<job>/<stage>.<attempt>/<partition>.<attempt>`), so a data sink knows its partition. |
| `RowPages` | `serializeRows`/`deserializeRows`: result rows as Presto pages. |
