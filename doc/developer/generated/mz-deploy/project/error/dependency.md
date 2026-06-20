---
source: src/mz-deploy/src/project/error/dependency.rs
revision: a647094cc4
---

# mz_deploy::project::error::dependency

Errors raised during dependency-graph analysis. `DependencyError` is a
`thiserror`-derived enum with a single variant, `CircularDependency`, carrying the
`ObjectId` of an object caught in a cycle in the object dependency graph. It is
returned by graph construction and cycle detection.
