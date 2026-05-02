// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/**
 * VS Code extension entry point for mz-deploy.
 *
 * Hosts the mz-deploy LSP client and registers commands. Two are invoked by
 * code lenses emitted from the server (`mz-deploy.runTest`,
 * `mz-deploy.runExplain`); a third (`mz-deploy.switchProfile`) is user-facing
 * and powers the status-bar profile switcher.
 *
 * The binary is resolved via the `mz-deploy.path` setting, which defaults to
 * `"mz-deploy"` (looked up through `$PATH` at spawn time). On activation the
 * extension verifies that the configured binary is actually runnable and
 * surfaces an actionable error dialog if not.
 */

import { LanguageClient, ServerOptions, LanguageClientOptions } from "vscode-languageclient/node";
import * as vscode from "vscode";
import { execFile } from "child_process";
import { promises as fs } from "fs";
import * as path from "path";

let client: LanguageClient | undefined;
let profileStatusBarItem: vscode.StatusBarItem | undefined;

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
    execFile(
      resolveBinaryPath(),
      ["--version"],
      { timeout: 5000, env: plainEnv() },
      (error) => {
        resolve(!error);
      },
    );
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

/**
 * Read the active profile from the workspace's `.mzprofile`. Returns `null` if
 * the file is absent or contains no usable line. Mirrors the parsing rules of
 * `read_mzprofile` in `src/mz-deploy/src/config.rs`: skip blank and `#`-comment
 * lines, take the first remaining line trimmed.
 */
async function readActiveProfile(workspace: string): Promise<string | null> {
  try {
    const contents = await fs.readFile(path.join(workspace, ".mzprofile"), "utf8");
    for (const raw of contents.split(/\r?\n/)) {
      const line = raw.trim();
      if (line.length === 0 || line.startsWith("#")) continue;
      return line;
    }
    return null;
  } catch (err: unknown) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw err;
  }
}

/** Spawn env that forces `mz-deploy` to emit plain (non-ANSI) output. */
function plainEnv(): NodeJS.ProcessEnv {
  return { ...process.env, NO_COLOR: "1" };
}

/**
 * Run `mz-deploy <args>` as a VSCode task. Output streams to a managed
 * terminal pane; the process is spawned directly so there's no shell-prompt
 * race that swallows the first character.
 */
async function runMzDeployTask(name: string, args: string[]): Promise<void> {
  const cwd = getWorkspacePath();
  const execution = new vscode.ProcessExecution(resolveBinaryPath(), args, cwd ? { cwd } : {});
  const task = new vscode.Task(
    { type: "process", task: name },
    vscode.TaskScope.Workspace,
    name,
    "mz-deploy",
    execution,
  );
  task.presentationOptions = {
    reveal: vscode.TaskRevealKind.Always,
    panel: vscode.TaskPanelKind.Dedicated,
    clear: true,
  };
  await vscode.tasks.executeTask(task);
}

interface ProfileListing {
  profiles: { name: string; active: boolean }[];
  source: string;
}

/**
 * List configured profiles by invoking `mz-deploy --output json profile list`.
 * Returns the parsed listing — name + active flag — straight from the CLI's
 * structured output, so the extension never has to scrape human-readable text.
 */
function listProfiles(workspace: string): Promise<ProfileListing> {
  return new Promise((resolve, reject) => {
    execFile(
      resolveBinaryPath(),
      ["--output", "json", "profile", "list"],
      { cwd: workspace, timeout: 5000, env: plainEnv() },
      (error, stdout, stderr) => {
        if (error) {
          reject(new Error((stderr || error.message).trim()));
          return;
        }
        try {
          resolve(JSON.parse(stdout) as ProfileListing);
        } catch (parseErr) {
          reject(
            new Error(`failed to parse profile list output: ${(parseErr as Error).message}`),
          );
        }
      },
    );
  });
}

/** Set the active profile via `mz-deploy profile set <name>`. */
function setActiveProfile(workspace: string, name: string): Promise<void> {
  return new Promise((resolve, reject) => {
    execFile(
      resolveBinaryPath(),
      ["profile", "set", name],
      { cwd: workspace, timeout: 5000, env: plainEnv() },
      (error, _stdout, stderr) => {
        if (error) {
          reject(new Error((stderr || error.message).trim()));
        } else {
          resolve();
        }
      },
    );
  });
}

/** Update the status-bar item to reflect the active profile in `.mzprofile`. */
async function refreshProfileStatusBar(item: vscode.StatusBarItem): Promise<void> {
  const workspace = getWorkspacePath();
  if (!workspace) {
    item.hide();
    return;
  }
  try {
    const active = await readActiveProfile(workspace);
    item.text = active ? `$(plug) ${active}` : "$(plug) (no profile)";
  } catch {
    item.text = "$(plug) ?";
  }
  item.show();
}

/** Registers code-lens-invoked commands. */
function registerCommands(context: vscode.ExtensionContext): void {
  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.runTest", async (filter: string) => {
      const activeEditor = vscode.window.activeTextEditor;
      if (activeEditor) {
        await activeEditor.document.save();
      }
      await runMzDeployTask("test", ["test", filter]);
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.runExplain", async (target: string) => {
      const activeEditor = vscode.window.activeTextEditor;
      if (activeEditor) {
        await activeEditor.document.save();
      }
      await runMzDeployTask("explain", ["explain", target]);
    }),
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.switchProfile", async () => {
      const workspace = getWorkspacePath();
      if (!workspace) {
        await vscode.window.showInformationMessage("Open an mz-deploy project to switch profiles.");
        return;
      }

      let listing: ProfileListing;
      try {
        listing = await listProfiles(workspace);
      } catch (err) {
        await vscode.window.showErrorMessage(
          `Failed to list mz-deploy profiles: ${(err as Error).message}`,
        );
        return;
      }

      if (listing.profiles.length === 0) {
        await vscode.window.showInformationMessage(
          `No mz-deploy profiles configured. Add one to ${listing.source}.`,
        );
        return;
      }

      const active = listing.profiles.find((p) => p.active)?.name ?? null;
      const items: vscode.QuickPickItem[] = listing.profiles.map((p) => ({
        label: p.name,
        description: p.active ? "(active)" : undefined,
      }));

      const picked = await vscode.window.showQuickPick(items, {
        placeHolder: "Select mz-deploy profile",
      });
      if (!picked || picked.label === active) return;

      try {
        await setActiveProfile(workspace, picked.label);
      } catch (err) {
        await vscode.window.showErrorMessage(
          `Failed to switch profile: ${(err as Error).message}`,
        );
      }

      if (profileStatusBarItem) {
        await refreshProfileStatusBar(profileStatusBarItem);
      }
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
      // `.mzprofile` is included so external edits trigger an LSP rebuild
      // against the new active profile.
      fileEvents: vscode.workspace.createFileSystemWatcher(
        "**/{project,profiles}.toml,**/.mzprofile",
      ),
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

  if (getWorkspacePath()) {
    profileStatusBarItem = vscode.window.createStatusBarItem(
      vscode.StatusBarAlignment.Left,
      100,
    );
    profileStatusBarItem.command = "mz-deploy.switchProfile";
    profileStatusBarItem.tooltip = "Active mz-deploy profile (click to switch)";
    context.subscriptions.push(profileStatusBarItem);
    await refreshProfileStatusBar(profileStatusBarItem);

    const watcher = vscode.workspace.createFileSystemWatcher("**/.mzprofile");
    const refresh = () => {
      if (profileStatusBarItem) void refreshProfileStatusBar(profileStatusBarItem);
    };
    watcher.onDidCreate(refresh);
    watcher.onDidChange(refresh);
    watcher.onDidDelete(refresh);
    context.subscriptions.push(watcher);
  }
}

export function deactivate(): Thenable<void> | undefined {
  return client?.stop();
}
