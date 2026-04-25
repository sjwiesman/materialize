# mz-deploy VS Code Extension

Thin LSP client for mz-deploy projects. The server (`mz-deploy lsp`) supplies
go-to-definition, hover, completion, parse diagnostics, and code lenses for
running tests and explaining objects. The extension wires the client up and
registers two terminal commands that the server's code lenses invoke.

The extension activates automatically when a workspace contains a
`project.toml` file.

## Prerequisites

- **Rust toolchain** — for building the `mz-deploy` binary
- **Node.js + npm** — for building the extension
- **VS Code** ^1.88.0

## Building

### 1. Build the mz-deploy binary

From the Materialize workspace root:

```sh
cargo build --release -p mz-deploy
```

The extension launches the binary configured by the `mz-deploy.path` setting,
which defaults to `mz-deploy` (resolved through your `$PATH` at spawn time).
For development builds, set `mz-deploy.path` to the absolute path of the
binary you want to run (e.g. via the VS Code settings UI or `settings.json`).

### 2. Install npm dependencies

```sh
cd misc/vscode-ext
npm install
```

### 3. Build the extension

```sh
npm run build
```

## Running in VS Code

### Option A: Debug (F5)

Open `misc/vscode-ext/` as a VS Code workspace and press **F5**.
The launch configuration runs `npm: build` automatically, then opens an
Extension Development Host window. Open any folder containing a
`project.toml` to activate the extension.

### Option B: Install locally

```sh
# From misc/vscode-ext/
npx @vscode/vsce package --no-dependencies
code --install-extension mz-deploy-lsp-0.1.0.vsix
```

## Development Workflow

- **`npm run watch`** — recompiles on save.
- After editing Rust LSP code, rebuild with `cargo build --release -p mz-deploy`
  and restart the extension.
