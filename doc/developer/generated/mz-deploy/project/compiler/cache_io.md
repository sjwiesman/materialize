---
source: src/mz-deploy/src/project/compiler/cache_io.rs
revision: a647094cc4
---

# mz_deploy::project::compiler::cache_io

Small IO helpers shared across the compiler cache layer. The single public
item is `hex_digest`, which renders a byte slice (such as a SHA-256 digest)
into its lowercase hexadecimal string form by formatting each byte as two
hex characters and concatenating. This produces the stable string
representation used for file content hashes and object fingerprints stored in
the SQLite build artifact.
