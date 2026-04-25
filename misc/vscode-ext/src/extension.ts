/**
 * VS Code extension entry point for mz-deploy.
 *
 * Hosts the mz-deploy LSP client and registers two commands invoked by code
 * lenses emitted from the server:
 *
 *   mz-deploy.runTest    — runs `mz-deploy test '<filter>'` in a terminal
 *   mz-deploy.runExplain — runs `mz-deploy explain '<target>'` in a terminal
 *
 * The binary is resolved via the `mz-deploy.path` setting, which defaults to
 * `"mz-deploy"` (looked up through `$PATH` at spawn time). On activation the
 * extension verifies that the configured binary is actually runnable and
 * surfaces an actionable error dialog if not.
 */

import { LanguageClient, ServerOptions, LanguageClientOptions } from "vscode-languageclient/node";
import * as vscode from "vscode";
import { execFile } from "child_process";

let client: LanguageClient | undefined;

/** Returns the filesystem path of the first open workspace folder, or undefined. */
function getWorkspacePath(): string | undefined {
  return vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
}

/**
 * Returns the mz-deploy binary path configured by the `mz-deploy.path` setting.
 * Defaults to the bare string `"mz-deploy"`, which resolves through the user's
 * `$PATH` at spawn time.
 */
function resolveBinaryPath(): string {
  return vscode.workspace.getConfiguration("mz-deploy").get<string>("path") || "mz-deploy";
}

/**
 * Returns true if the configured binary is present and runs successfully when
 * invoked with `--version`. Catches both "not found" (ENOENT) and "ran but
 * exited non-zero" so a stray binary at the path doesn't pass the check.
 */
function isBinaryAvailable(): Promise<boolean> {
  return new Promise((resolve) => {
    execFile(resolveBinaryPath(), ["--version"], { timeout: 5000 }, (error) => {
      resolve(!error);
    });
  });
}

/** Show an actionable error dialog when the binary cannot be found or run. */
async function notifyBinaryUnavailable(): Promise<void> {
  const binPath = resolveBinaryPath();
  const message =
    `Could not run \`mz-deploy\` from \`${binPath}\`. ` +
    `Install the binary and make sure it is on your PATH, or set the ` +
    `\`mz-deploy.path\` setting to its absolute location, then reload the window.`;
  const action = await vscode.window.showErrorMessage(message, "Open Settings");
  if (action === "Open Settings") {
    await vscode.commands.executeCommand("workbench.action.openSettings", "mz-deploy.path");
  }
}

/** Registers code-lens-invoked commands. */
function registerCommands(context: vscode.ExtensionContext): void {
  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.runTest", async (filter: string) => {
      const activeEditor = vscode.window.activeTextEditor;
      if (activeEditor) {
        await activeEditor.document.save();
      }
      const terminal = vscode.window.createTerminal("mz-deploy test");
      terminal.show();
      terminal.sendText(`${resolveBinaryPath()} test '${filter}'`);
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.runExplain", async (target: string) => {
      const activeEditor = vscode.window.activeTextEditor;
      if (activeEditor) {
        await activeEditor.document.save();
      }
      const terminal = vscode.window.createTerminal("mz-deploy explain");
      terminal.show();
      terminal.sendText(`${resolveBinaryPath()} explain '${target}'`);
    }),
  );
}

/** Start the LSP client against the configured binary. */
async function startLspClient(): Promise<void> {
  const command = resolveBinaryPath();
  const workspaceFolder = getWorkspacePath();

  const serverOptions: ServerOptions = {
    run: { command, args: ["lsp", "-d", workspaceFolder || "."] },
    debug: { command, args: ["lsp", "-d", workspaceFolder || "."] },
  };

  const clientOptions: LanguageClientOptions = {
    documentSelector: [{ scheme: "file", language: "sql" }],
    synchronize: {
      fileEvents: vscode.workspace.createFileSystemWatcher("**/{project,profiles}.toml"),
    },
  };

  client = new LanguageClient("mz-deploy-lsp", "mz-deploy LSP", serverOptions, clientOptions);
  await client.start();
}

export async function activate(context: vscode.ExtensionContext): Promise<void> {
  registerCommands(context);

  if (!(await isBinaryAvailable())) {
    await notifyBinaryUnavailable();
    return;
  }

  await startLspClient();
}

export function deactivate(): Thenable<void> | undefined {
  return client?.stop();
}
