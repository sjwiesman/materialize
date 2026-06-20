---
source: src/mz-deploy/src/cli/progress.rs
revision: a647094cc4
---

# mz_deploy::cli::progress

Helper functions for scannable, dbt-style progress output, all emitting via the
`info!` macro with `owo_colors` styling.

`stage_start` prints a yellow arrow and a "...". `stage_success` and `success`
print a green checkmark, with `stage_success` appending a dimmed duration.
`warn` and `error` print yellow and red status symbols. `action` prints a
cargo-style line: a 12-column right-aligned bold-green verb followed by a
message; `finished` builds on it to emit a "Finished <action> in <n>s" line. The
private `format_duration` formats seconds with two decimal places below one
second and one decimal place at or above one second.
