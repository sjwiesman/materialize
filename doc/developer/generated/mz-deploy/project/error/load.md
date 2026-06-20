---
source: src/mz-deploy/src/project/error/load.rs
revision: a647094cc4
---

# mz_deploy::project::error::load

`LoadError`, the `thiserror`-derived enum for project file I/O and directory
traversal failures. Variants cover the project layout (`RootNotFound`,
`RootNotDirectory`, `ModelsNotFound`), filesystem reads and writes
(`DirectoryReadFailed`, `EntryReadFailed`, `FileReadFailed`, `CacheWriteFailed`,
`DirectoryCreationFailed`, each wrapping the underlying `std::io::Error` as a
`#[source]`), name/path extraction (`InvalidFileName`, `SchemaExtractionFailed`,
`DatabaseExtractionFailed`), and `DuplicateProfileObject` when two files resolve
to the same object name for the active profile (carrying both conflicting paths).
`BuildArtifactFailed` is a transparent wrapper converting from
`compiler::cache::CacheError` via `#[from]`.
