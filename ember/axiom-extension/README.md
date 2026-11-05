# ember/axiom-extension

Ember's implementations of Axiom's extension points. The Axiom side lives in
our fork (see "Updating Axiom and Velox" in the top-level README).

| File | Contents |
|------|----------|
| `RangePartitionType` | The `connector::PartitionType` of a global sort under a table write or at the root of a query, given to Axiom as `MultiFragmentPlan::Options::sortPartitioner`. Axiom plans a range shuffle and a sort per task instead of a merge onto one task, with `RangePartitionFunction` from `ember/shuffle`. A write adds a writer to each task; a result stays in the tasks (`remoteOutput`) for the driver to read in partition order. Keys that function cannot split keep the merge. |
