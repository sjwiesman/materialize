/**
 * VS Code extension entry point for mz-deploy.
 *
 * Orchestrates two subsystems on top of the mz-deploy LSP server:
 *
 * 1. **Data Catalog sidebar** — Tree-browsable object catalog with drill-down
 *    detail views, powered by `mz-deploy/catalog` LSP requests.
 * 2. **DAG panel** — Layered dependency graph visualization, powered by
 *    `mz-deploy/dag` LSP requests.
 *
 * ## Data Flow
 *
 *     LSP Server (mz-deploy lsp)
 *         │
 *         ▼
 *     LanguageClient ──► mz-deploy/catalog ──► CatalogProvider ──► catalog.js
 *                    ──► mz-deploy/dag     ──► DAGPanel        ──► dag.js
 *
 * ## Refresh Lifecycle
 *
 * On file save the LSP server rebuilds the project and emits a
 * `mz-deploy/projectRebuilt` notification. The extension responds by
 * re-requesting catalog and DAG data, keeping the UI in sync without polling.
 */

import { LanguageClient, ServerOptions, LanguageClientOptions } from "vscode-languageclient/node";
import * as vscode from "vscode";
import * as path from "path";
import * as os from "os";
import * as fs from "fs";
import { CatalogProvider } from "./sidebar/catalog-provider";
import { DAGPanel } from "./panels/dag-panel";
import { DagData, CatalogData, CatalogOutboundMessage, DagOutboundMessage } from "./types";

let client: LanguageClient;
let catalogProvider: CatalogProvider | null = null;
let dagPanel: DAGPanel | null = null;

/** Returns the filesystem path of the first open workspace folder, or undefined. */
function getWorkspacePath(): string | undefined {
  return vscode.workspace.workspaceFolders?.[0]?.uri.fsPath;
}

/** Fetches the dependency graph from the LSP server and pushes it to the DAG panel. */
async function requestDagData(): Promise<void> {
  if (!client || !client.isRunning()) return;
  try {
    const data = await client.sendRequest<DagData>("mz-deploy/dag");
    if (data && dagPanel) dagPanel.setDAGData(data);
  } catch (err) {
    console.error("[mz-deploy] dag request failed:", err);
  }
}

/** Fetches the catalog from the LSP server and pushes it to the sidebar provider. */
async function requestCatalogData(): Promise<void> {
  if (!client || !client.isRunning()) return;
  try {
    const data = await client.sendRequest<CatalogData>("mz-deploy/catalog");
    if (data && catalogProvider) {
      catalogProvider.setCatalogData(data);
    }
  } catch (err) {
    console.error("[mz-deploy] catalog request failed:", err);
  }
}

/**
 * Extension activation entry point. Called by VS Code when a workspace
 * containing `project.toml` is opened.
 *
 * Sets up:
 * 1. The LSP client pointing at `mz-deploy lsp`
 * 2. The catalog sidebar (`CatalogProvider`) and DAG panel (`DAGPanel`)
 * 3. Message routing between webviews and the extension host
 * 4. Commands: `mz-deploy.openDAG`, `mz-deploy.runTest`
 * 5. The `mz-deploy/projectRebuilt` notification handler for live refresh
 */
/** Registers all VS Code commands and pushes their disposables to the extension context. */
function registerCommands(context: vscode.ExtensionContext): void {
  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.openDAG", () => {
      dagPanel!.open(null);
      void requestDagData();
    })
  );

  context.subscriptions.push(
    vscode.commands.registerCommand("mz-deploy.runTest", async (filter: string) => {
      const activeEditor = vscode.window.activeTextEditor;
      if (activeEditor) {
        await activeEditor.document.save();
      }
      const terminal = vscode.window.createTerminal("mz-deploy test");
      terminal.show();
      terminal.sendText(
        `~/materialize/target/release/mz-deploy test '${filter}'`
      );
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
      terminal.sendText(
        `~/materialize/target/release/mz-deploy explain '${target}'`
      );
    })
  );
}

/** Registers LSP notification handlers for project rebuilds. */
function registerNotificationHandlers(): void {
  // Refresh catalog and DAG data when the LSP server finishes rebuilding
  // the project (triggered by file saves). Registered before start() so the
  // handler is in place when the first notification arrives.
  client.onNotification("mz-deploy/projectRebuilt", () => {
    void requestCatalogData();
    void requestDagData();
  });
}

function resolveBinaryPath(): string {
  const localPath = path.join(os.homedir(), "materialize", "target", "release", "mz-deploy");
  if (fs.existsSync(localPath)) {
    return localPath;
  }
  return "mz-deploy";
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

  client = new LanguageClient(
    "mz-deploy-lsp",
    "mz-deploy LSP",
    serverOptions,
    clientOptions
  );

  // --- Sidebar: Data Catalog ---
  catalogProvider = new CatalogProvider(context.extensionUri);
  context.subscriptions.push(
    vscode.window.registerWebviewViewProvider("mz-deploy-catalog", catalogProvider)
  );

  // --- Editor Panels ---
  dagPanel = new DAGPanel(context.extensionUri);
  // --- Sidebar message routing ---
  catalogProvider.onMessage((msg: CatalogOutboundMessage) => {
    switch (msg.type) {
      case "open-file": {
        const workspace = getWorkspacePath();
        if (workspace && msg.path) {
          const absPath = path.join(workspace, msg.path);
          void vscode.workspace.openTextDocument(absPath).then((doc) => {
            void vscode.window.showTextDocument(doc, { viewColumn: vscode.ViewColumn.One });
          });
        }
        break;
      }
      case "open-dag":
        dagPanel!.open(msg.focusTable || null);
        void requestDagData();
        break;
    }
  });

  // --- DAG panel message routing ---
  dagPanel.onMessage((msg: DagOutboundMessage) => {
    if (msg.type === "inspect-object" && msg.id) {
      catalogProvider!.inspectObject(msg.id);
    }
  });

  // --- Commands ---
  registerCommands(context);

  // --- Notification handlers ---
  registerNotificationHandlers();

  // --- LSP startup ---
  void client.start().then(() => {
    void requestCatalogData();
    void requestDagData();
  });
}

/** Extension deactivation. Stops the LSP client. */
export function deactivate(): Thenable<void> | undefined {
  return client?.stop();
}
