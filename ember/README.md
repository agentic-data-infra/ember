# ember/

| Directory | Contents |
|-----------|----------|
| `api/`     | User-facing Dataframe API: `Session`, `Dataframe`, `Column`, `functions` (see [api/README.md](api/README.md)). |
| `driver/`  | Plans jobs (via Axiom), schedules stages and tasks onto workers. |
| `worker/`  | Runs tasks as Velox tasks; `ember-worker` binary. |
| `shuffle/` | Shuffle writer/reader interfaces and the file-based shuffle. |
| `rpc/`     | Small length-prefixed TCP RPC between driver and workers. |
| `tests/`   | Build-integration smoke tests. |
