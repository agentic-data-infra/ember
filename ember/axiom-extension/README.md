# ember/axiom-extension

Ember's implementations of Axiom's extension points. The Axiom side lives in
our fork (see "Updating Axiom and Velox" in the top-level README).

| File | Contents |
|------|----------|
| `RangePartitionType` | The `connector::PartitionType` of a global sort under a table write, given to Axiom as `MultiFragmentPlan::Options::sortPartitioner`. Axiom plans a range shuffle and a sort and a writer per task instead of a merge onto one task, with `RangePartitionFunction` from `ember/shuffle`. Keys that function cannot split keep the merge. |
