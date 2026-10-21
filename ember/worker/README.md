# ember/worker

| File | Contents |
|------|----------|
| `TaskDescription` | The task message (job, stage, partition, attempt, plan fragment JSON, serialized splits per plan node, config) and the `TaskResult` reply (state, error, user-error flag, `MapStatus`, result pages). Both serialize to bytes. |
| `Executor` | Runs task bytes as a Velox task: deserializes the fragment, adds the splits, waits for completion, and reports the map output or result pages. Owns a `ShuffleBlockStore` under its shuffle directory, registered at its location. |
