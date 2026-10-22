# ember/

| Directory | Contents |
|-----------|----------|
| `api/`     | User-facing Dataframe API: `Session`, `Dataframe`, `Column`, `functions` (see [api/README.md](api/README.md)). |
| `common/`  | Runtime setup, function registry, text file format, file split and handle serde, row pages (see [common/README.md](common/README.md)). |
| `driver/`  | Cuts plans into stages and schedules stages and tasks onto a backend (see [driver/README.md](driver/README.md)). |
| `worker/`  | Runs serialized tasks as Velox tasks (see [worker/README.md](worker/README.md)). |
| `shuffle/` | Shuffle writer and block transport interfaces, the file-based shuffle and its Velox operators (see [shuffle/README.md](shuffle/README.md)). |
| `rpc/`     | fbthrift services between driver and workers (see [rpc/README.md](rpc/README.md)). |
| `tests/`   | Build-integration smoke tests. |

A job flows api → driver → worker → shuffle: the Session optimizes a
Dataframe into an Axiom `MultiFragmentPlan`, the driver turns its fragments
into stages and tasks, each task travels as bytes to an executor, and map
tasks write shuffle files that the next stage's tasks fetch as blocks.
