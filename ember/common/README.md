# ember/common

Code shared by the api, driver and worker. Headers are included as `ember/<File>.h`.

| File | Contents |
|------|----------|
| `Runtime` | `initializeRuntime()`: memory manager, file systems, Presto serde, functions, plan node and split serde, shuffle operators, and the `ember_file` connector with its text and parquet formats. Idempotent. |
| `FunctionRegistry` | Registers Presto functions (Axiom's base) plus Velox's `sparksql` function package under the `ember_` prefix, and a rewrite hook that resolves scalar calls to those versions, so `split` takes a regex. |
| `TextFileHandler` | Text format for Axiom's file connector: one `value` column per line. Reads a byte range of a file; a range owns the lines that start inside it. |
| `FileSerde` | Serializable versions of the file connector's table handle, column handle and split (`FileRangeSplit`: path, start, length), so tasks travel as bytes. |
| `RowPages` | `serializeRows`/`deserializeRows`: result rows as Presto pages. |
