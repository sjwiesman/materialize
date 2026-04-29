// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Filesystem abstraction with optional in-memory overlays.
//!
//! The overlay only intercepts content reads. Directory walks, file
//! existence checks, and sibling metadata are still served from disk.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

/// Read-through filesystem with an optional in-memory overlay.
pub(crate) struct FileSystem {
    overlay: BTreeMap<PathBuf, String>,
}

impl FileSystem {
    /// Construct a filesystem with no overlay; reads always go to disk.
    pub(crate) fn new() -> Self {
        Self {
            overlay: BTreeMap::new(),
        }
    }

    /// Construct a filesystem with the given overlay; a read for a path
    /// present in `overlay` returns the overlay bytes, otherwise it falls
    /// back to disk.
    pub(crate) fn with_overlay(overlay: BTreeMap<PathBuf, String>) -> Self {
        Self { overlay }
    }

    /// Read the file at `path`, consulting the overlay first.
    pub(crate) fn read_to_string(&self, path: &Path) -> io::Result<String> {
        if let Some(text) = self.overlay.get(path) {
            return Ok(text.clone());
        }
        std::fs::read_to_string(path)
    }

    /// Whether `path` is covered by an overlay entry. Used by callers that
    /// maintain disk-keyed caches: when a path is overlay-covered, the
    /// disk-derived cache key is meaningless and the cache must be
    /// bypassed.
    pub(crate) fn is_overlay(&self, path: &Path) -> bool {
        self.overlay.contains_key(path)
    }
}

impl Default for FileSystem {
    fn default() -> Self {
        Self::new()
    }
}
