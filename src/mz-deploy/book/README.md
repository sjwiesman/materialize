# The mz-deploy Book

User-facing documentation for `mz-deploy`. Authored as an [mdbook][mdbook].

## Building

```bash
mdbook build src/mz-deploy/book
```

Output goes to `src/mz-deploy/book/build/`.

## Live preview

```bash
mdbook serve src/mz-deploy/book
```

Opens a local server with hot reload.

## Reference appendix

Appendix A is generated from `src/mz-deploy/src/cli/help/*.md`. Regenerate
when those files change:

```bash
src/mz-deploy/book/generate-reference.sh
```

This:

- Mirrors every `help/*.md` file into `book/src/reference/` (gitignored).
- Rewrites the marker-delimited block in `book/src/SUMMARY.md`.

CI runs `ci/test/lint-mz-deploy-book.sh`, which fails if `SUMMARY.md`'s
reference block is out of sync with the help files.

## Conventions

- Each chapter opens with a single-sentence "What you'll learn" italic line.
- Each chapter closes with a "You can now…" bullet list.
- Use H2/H3 only; chapter title is the implicit H1.
- Examples use the support-ticket SLA domain (see `examples/first-project/`).
- No screenshots in v1 — terminal output blocks only.

[mdbook]: https://rust-lang.github.io/mdBook/
