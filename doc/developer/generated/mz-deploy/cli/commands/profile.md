---
source: src/mz-deploy/src/cli/commands/profile.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::profile

Implements the `profile {list,set,current}` subcommands for managing the
project's default profile, modeled on `kubectl config` contexts. The default is
recorded per-project and per-developer (in a `.mzprofile` file), so each team
member can choose their own. Resolution order is `--profile`, then
`MZ_DEPLOY_PROFILE`, then the recorded project default.

`list` loads `ProfilesConfig`, determines the active profile via
`resolve_active`, and emits a `ProfileListing` of `ProfileEntry` values marking
the active one. The listing is `Serialize` for JSON and has a `Display` impl that
prints each name, tagging the active entry "(active)", or a "no profiles found"
message.

`set` validates that the named profile exists in `profiles.toml` (so typos fail
immediately) and records it via `write_mzprofile`. `current` prints the resolved
profile and its source: a `--profile` flag vs. `MZ_DEPLOY_PROFILE` (distinguished
by checking the env var directly), the project default from `read_mzprofile`, or
a warning that no profile is selected. `resolve_active` returns the CLI profile
if present, otherwise the recorded default.
