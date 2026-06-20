---
source: src/mz-deploy/src/fs.rs
revision: a647094cc4
---

# mz_deploy::fs

A read-through filesystem abstraction with an optional in-memory overlay. The
overlay intercepts only content reads; directory walks, existence checks, and
sibling metadata still come from disk. `FileSystem` wraps a
`BTreeMap<PathBuf, String>` overlay. `new` creates one with no overlay (all reads
hit disk); `with_overlay` seeds the map directly; `from_overlay_file` loads it
from a JSON file mapping absolute paths to contents (used by the `--overlay` flag
on `test` and `explain` so the VSCode extension can surface unsaved buffers
without writing them to disk). `read_to_string` returns overlay bytes when present
and otherwise reads from disk. `is_overlay` reports whether a path is
overlay-covered, so callers that key caches by disk path can bypass the cache for
overlaid paths.
