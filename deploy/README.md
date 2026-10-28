# Running Ember on Kubernetes

One container image (`build_image.sh`, TCP only) runs every process:

| Manifest | What runs |
| --- | --- |
| `kubernetes/kiln.yaml` | `kiln-meta` Deployment and Service; a `kiln-server` DaemonSet with one DRAM region per node |
| `kubernetes/ember-worker.yaml` | an `ember-worker` DaemonSet; each worker serves one driver after another |
| `kubernetes/sort-job.yaml` | the sort benchmark's driver as the `ember-sort` Job, behind the `ember-driver` Service the workers connect to |

Kiln servers and Ember workers use the node's network, host name and
`/dev/shm`, so a worker maps the DRAM regions of its own node's Kiln server
and reads other nodes' over TCP. Input is node-local
(`ember.files.node_local=true`): each node keeps its own files under
`/var/lib/ember/data`, mounted at `/data`, and the sorted part files land on
the nodes that ran each range.

## A local cluster with kind

`kind/cluster.yaml` is one control plane and three workers, as Docker
containers on this machine. Needs Docker, kind and kubectl.

```sh
deploy/build_image.sh                       # ember:dev
deploy/kind/up.sh                           # cluster, image, Kiln, workers
deploy/kubernetes/sort_demo.sh 10000000     # 10M records (1 GB) per node
```

`sort_demo.sh` writes each node's slice of one gensort run into
`/data/sort-input`, runs the driver Job and checks the part files of all nodes
with valsort: sorted across nodes, and the same record count and checksum as
the input. `kind delete cluster --name ember` removes the cluster.
