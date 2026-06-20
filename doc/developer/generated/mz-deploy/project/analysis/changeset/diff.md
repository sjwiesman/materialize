---
source: src/mz-deploy/src/project/analysis/changeset/diff.rs
revision: a647094cc4
---

# mz_deploy::project::analysis::changeset::diff

Compares two [`DeploymentSnapshot`](crate::project::analysis::deployment_snapshot)s
and returns the set of objects whose content changed between them.

`find_changed_objects` is the single public function. It compares the `objects`
maps (`ObjectId` to content hash) of an old and a new snapshot and collects an
object into the result `BTreeSet<ObjectId>` when it is modified (present in both,
differing hashes), added (present only in the new snapshot), or deleted (present
only in the old snapshot). Because snapshot hashes are computed from the
normalized typed AST rather than raw file text, formatting-only edits —
whitespace, comments, identifier casing — produce identical hashes and do not
appear in the diff. The function emits colored `verbose!` lines for each
changed/new/deleted object and a closing count.
