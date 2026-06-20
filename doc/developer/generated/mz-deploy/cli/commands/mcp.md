---
source: src/mz-deploy/src/cli/commands/mcp.rs
revision: a647094cc4
---

# mz_deploy::cli::commands::mcp

Implements the `mcp` command, a stdio-to-HTTP bridge for Materialize's developer
MCP server. MCP clients (Claude Desktop, Claude Code, Cursor) launch this binary
as a subprocess speaking newline-delimited JSON-RPC over stdio; the command POSTs
each message to the `POST /api/mcp/developer` endpoint using the active profile's
credentials and writes each response back to stdout.

`run` resolves the profile (without `Settings::load`, so `mcp` works outside a
project directory), builds the endpoint URL, and loops over stdin lines. For each
non-empty line it POSTs the body with `Content-Type: application/json`, attaching
HTTP Basic auth only when the profile carries a non-empty password. Responses are
written back with exactly one trailing newline (MCP stdio framing); transport
failures and empty/error responses are turned into a JSON-RPC error object by
`synthesize_error`, which preserves the request `id` so the client gets a
structured error rather than a hang.

`resolve_profile` reads `--profile` / `MZ_DEPLOY_PROFILE` (passed via
`cli_profile`), falling back to a `.mzprofile` in the directory, and resolves it
through `ProfilesConfig`. `developer_url` builds the endpoint from the profile's
`http_host`, inferring scheme and port: bare loopback hosts get `http://` and the
default `LOCAL_HTTP_PORT` (6876); other bare hosts get `https://`; explicit
`http://`/`https://` URLs are used verbatim; IPv6 hosts are bracketed. A unit-test
module exercises the URL inference across these cases.
