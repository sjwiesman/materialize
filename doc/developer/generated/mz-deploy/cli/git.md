---
source: src/mz-deploy/src/cli/git.rs
revision: a647094cc4
---

# mz_deploy::cli::git

Git metadata helpers used to tag deployments with source-control context, both
shelling out to the `git` binary in the project directory.

`get_git_commit` runs `git rev-parse HEAD` and returns `Some(hash)` when the
directory is a git repository with a valid HEAD, otherwise `None`. `is_dirty`
runs `git status --porcelain` and returns `true` when the output is non-empty
(any uncommitted or unstaged change); on command failure it returns `false`.
