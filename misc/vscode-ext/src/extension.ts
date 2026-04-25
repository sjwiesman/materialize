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
 * `"mz-deploy"` (looked up through `$PATH` at spawn time).
 */

import { LanguageClient, ServerOptions, LanguageClientOptions } from "vscode-languageclient/node";
import * as vscode from "vscode";

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
    })
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
    })
  );
}

export function activate(context: vscode.ExtensionContext): void {
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

  registerCommands(context);

  void client.start();
}

export function deactivate(): Thenable<void> | undefined {
  return client?.stop();
}
