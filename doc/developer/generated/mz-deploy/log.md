---
source: src/mz-deploy/src/log.rs
revision: a647094cc4
---

# mz_deploy::log

Output and logging utilities for mz-deploy. Three global atomic flags —
`VERBOSE`, `JSON_OUTPUT`, `QUIET` — are set once at startup from CLI flags
(`set_verbose`, `set_json_output`, `set_quiet`) and read back via the
corresponding `*_enabled` accessors. `color_enabled` reports stderr color support
(via the `supports-color` crate, the same source `owo-colors` consults).

The `verbose!` macro prints to stderr only when verbose mode is on. The `info!`
and `info_nonl!` macros print supplementary stderr messages (the latter without a
trailing newline) unless quiet mode is on.

The `Render` trait (blanket-implemented for any `Display + Serialize`) is the core
output pattern: a command defines one struct with both representations and hands
it to `output`, which emits JSON to stdout under `--output json` and human text to
stderr otherwise, both silenced by `--quiet`. `output_json` writes a JSON-only
value to stdout for paths with no human form (NDJSON streaming, machine-only
plans). `print_deploy_id` writes a bare deploy ID to stdout in human mode (the one
intentional human-mode stdout write, letting callers compose `stage` with
`wait`/`promote`), and is skipped in JSON mode where the structured result already
carries the id.
