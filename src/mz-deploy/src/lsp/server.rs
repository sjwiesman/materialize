//! LSP backend and `LanguageServer` trait implementation.
//!
//! [`Backend`] holds per-session state: open documents, compiled project
//! metadata (for go-to-definition, hover, completion, and code lens),
//! and workspace configuration.
//!
//! ## State Management
//!
//! - **`documents`** — Open document contents, updated on every `didOpen` /
//!   `didChange`.
//! - **`project_cache`** — Compiled project metadata. Opened lazily on the
//!   first successful build; the same handle is reused across rebuilds.
//! - **`project_diagnostic_uris`** — Tracks which files currently have
//!   project-level validation diagnostics. On each rebuild, diagnostics are
//!   diffed via [`compute_diagnostic_actions`]: old URIs not in the new set
//!   are cleared, new diagnostics are published.
//! - **`root`** — The workspace root directory.
//! - **`settings`** / **`variables`** — Project and profile configuration,
//!   reloaded at startup and on every save.
//!
//! ## Typecheck on Save
//!
//! After every `rebuild_project()`, the server runs [`run_typecheck()`] which
//! performs incremental typechecking. If definitions are unchanged since the
//! last typecheck, validation is skipped entirely. If the typecheck backend
//! is unavailable, typechecking is silently skipped — the catalog retains its
//! last known column data.
//!
//! ## Custom Notifications
//!
//! - **`mz-deploy/projectRebuilt`** — Sent to the client after
//!   `rebuild_project()` and again after `run_typecheck()` if column data
//!   changed. The VS Code extension uses this to refresh the catalog sidebar
//!   and DAG panel with fresh data.

use crate::config::{ProjectSettings, default_docker_image, read_mzprofile};
use crate::lsp::{
    catalog, code_lens, completion, dag, diagnostics, document_symbol, goto_definition, hover,
    references, semantic_tokens, workspace_symbol,
};
use crate::project;
use crate::project::error::{ProjectError, ValidationErrors};
use crate::project::ir::graph;
use crate::project_cache::ProjectCache;
use crate::types;
use ropey::Rope;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tower_lsp::jsonrpc::Result;
use tower_lsp::lsp_types::*;
use tower_lsp::{Client, LanguageServer};

/// Custom notification sent to the client after a project rebuild completes.
///
/// The extension listens for this to refresh catalog and DAG data, replacing
/// the old timer-based approach that was prone to race conditions.
struct ProjectRebuilt;

impl notification::Notification for ProjectRebuilt {
    type Params = ();
    const METHOD: &'static str = "mz-deploy/projectRebuilt";
}

/// Actions to take after a project rebuild, expressed as pure data.
///
/// Separates the *decision* of which diagnostics to publish/clear from the
/// *execution* of those actions (which requires async I/O via the LSP client).
/// The [`compute_diagnostic_actions`] function produces this from validation
/// diagnostics and the set of previously tracked diagnostic URIs.
struct DiagnosticActions {
    /// New diagnostics to publish, keyed by file URI.
    diagnostics_to_publish: Vec<(Url, Vec<Diagnostic>)>,
    /// URIs that had diagnostics before but should now be cleared.
    uris_to_clear: Vec<Url>,
    /// The new set of URIs that have diagnostics (replaces `project_diagnostic_uris`).
    new_tracked_uris: Vec<Url>,
}

/// Compute which diagnostics to publish and which URIs to clear.
///
/// `new_diagnostics` is the set of validation diagnostics from the current
/// build (empty on success or non-validation errors). `old_diagnostic_uris`
/// is the set of URIs that had diagnostics before this build.
///
/// URIs in the old set that are not in the new set are scheduled for clearing.
/// Resolve the default profile name for LSP operations.
///
/// The language server has no CLI flags; it reads the project root's
/// `.mzprofile` and falls back to `"default"` so variable resolution, suffix
/// lookup, and cluster normalization have *something* to key off even if the
/// developer hasn't run `mz-deploy profile set` yet.
fn resolve_lsp_profile_name(project_root: &Path) -> String {
    read_mzprofile(project_root)
        .ok()
        .flatten()
        .unwrap_or_else(|| "default".to_string())
}

fn compute_diagnostic_actions(
    new_diagnostics: BTreeMap<PathBuf, Vec<Diagnostic>>,
    old_diagnostic_uris: &[Url],
) -> DiagnosticActions {
    let new_uris: Vec<Url> = new_diagnostics
        .keys()
        .filter_map(|path| Url::from_file_path(path).ok())
        .collect();

    let uris_to_clear: Vec<Url> = old_diagnostic_uris
        .iter()
        .filter(|uri| !new_uris.contains(uri))
        .cloned()
        .collect();

    let diagnostics_to_publish: Vec<(Url, Vec<Diagnostic>)> = new_diagnostics
        .into_iter()
        .filter_map(|(path, diags)| Url::from_file_path(path).ok().map(|uri| (uri, diags)))
        .collect();

    DiagnosticActions {
        diagnostics_to_publish,
        uris_to_clear,
        new_tracked_uris: new_uris,
    }
}

/// Try to open a long-lived [`ProjectCache`] (read-only SQLite connection).
///
/// Returns `None` if the database file doesn't exist yet or can't be opened.
fn try_open_project_cache(
    root: &Path,
    profile: &str,
    profile_suffix: Option<&str>,
    variables: &BTreeMap<String, String>,
) -> Option<ProjectCache> {
    ProjectCache::open(root, profile, profile_suffix, variables)
        .ok()
        .flatten()
}

/// LSP backend holding session state.
pub(super) struct Backend {
    /// Client handle for sending notifications (e.g., diagnostics).
    client: Client,
    /// Per-file text ropes, keyed by document URI.
    documents: Mutex<BTreeMap<Url, Rope>>,
    /// Compiled project metadata for go-to-definition, hover, completion, and code lens.
    project_cache: Mutex<Option<ProjectCache>>,
    /// File URIs that currently have project-level validation diagnostics.
    project_diagnostic_uris: Mutex<Vec<Url>>,
    /// Project root directory.
    root: RwLock<PathBuf>,
    /// Cached project settings loaded from `project.toml`.
    settings: RwLock<Option<ProjectSettings>>,
    /// Cached variables from the active profile config.
    variables: RwLock<BTreeMap<String, String>>,
    /// Name of the active profile (for hover display).
    profile_name: RwLock<String>,
    /// Last project build error, if the most recent build failed.
    /// Used by the catalog endpoint to report errors to the sidebar.
    last_build_error: RwLock<Option<String>>,
}

impl Backend {
    /// Create a new backend with the given LSP client handle and project root.
    pub(super) fn new_with_root(client: Client, root: PathBuf) -> Self {
        Self {
            client,
            documents: Mutex::new(BTreeMap::new()),
            project_cache: Mutex::new(None),
            project_diagnostic_uris: Mutex::new(Vec::new()),
            root: RwLock::new(root),
            settings: RwLock::new(None),
            variables: RwLock::new(BTreeMap::new()),
            profile_name: RwLock::new("default".to_string()),
            last_build_error: RwLock::new(None),
        }
    }

    /// Load project settings and variables from `project.toml`.
    ///
    /// Silently defaults when `project.toml` is missing (no config is valid).
    /// Called during `initialized` and at the start of each `rebuild_project`.
    async fn load_settings(&self) {
        let root = self.root.read().await.clone();
        match ProjectSettings::load(&root) {
            Ok(ps) => {
                let name = resolve_lsp_profile_name(&root);
                let config = ps.config_for_profile(&name);
                *self.variables.write().await = config.variables.clone();
                *self.profile_name.write().await = name;
                *self.settings.write().await = Some(ps);
            }
            Err(_) => {
                // No project.toml or parse error — use defaults.
                *self.settings.write().await = None;
                *self.variables.write().await = BTreeMap::new();
                *self.profile_name.write().await = "default".to_string();
            }
        }
    }

    /// Publish parse diagnostics for a single document.
    async fn publish_diagnostics(&self, uri: Url, text: &str) {
        let rope = Rope::from_str(text);
        let variables = self.variables.read().await.clone();
        let profile = self.profile_name.read().await.clone();
        let diags = diagnostics::diagnose(text, &rope, &variables, &profile);

        // Store the rope for later offset conversions (go-to-definition).
        let mut docs = self.documents.lock().await;
        docs.insert(uri.clone(), rope);
        drop(docs); // release before .await on client

        self.client.publish_diagnostics(uri, diags, None).await;
    }

    /// Snapshot document text and cursor context for a given position.
    ///
    /// Acquires the documents lock once and returns the full document text,
    /// char offset, and optional dot-qualified identifier parts at the cursor.
    /// Returns `None` if the document is not open.
    async fn snapshot_at_position(
        &self,
        uri: &Url,
        position: Position,
    ) -> Option<(String, usize, Option<Vec<String>>)> {
        let (byte_offset, text) = {
            let docs = self.documents.lock().await;
            let rope = docs.get(uri)?;
            let line_start = rope
                .try_line_to_char(usize::try_from(position.line).unwrap_or(0))
                .ok()?;
            let offset = line_start + usize::try_from(position.character).unwrap_or(0);
            (offset, rope.to_string())
        };

        let parts = goto_definition::find_reference_at_position(&text, byte_offset);
        Some((text, byte_offset, parts))
    }

    /// Handle the `mz-deploy/dag` custom request.
    ///
    /// Returns the project's dependency graph as JSON, or `null` if no project
    /// has been successfully built yet.
    #[allow(clippy::unused_async)] // async required by tower-lsp custom_method
    pub(super) async fn dag(&self) -> Result<serde_json::Value> {
        let root = self.root.read().await.clone();
        let cache_guard = self.project_cache.lock().await;
        match cache_guard.as_ref() {
            Some(cache) => Ok(serde_json::to_value(dag::build_dag_response(cache, &root))
                .unwrap_or(serde_json::Value::Null)),
            None => Ok(serde_json::Value::Null),
        }
    }

    /// Handle the `mz-deploy/catalog` custom request.
    ///
    /// Returns the project's data catalog as JSON, or `null` if no project
    /// has been successfully built yet.
    #[allow(clippy::unused_async)] // async required by tower-lsp custom_method
    pub(super) async fn catalog(&self) -> tower_lsp::jsonrpc::Result<serde_json::Value> {
        let root = self.root.read().await.clone();
        let cache_guard = self.project_cache.lock().await;
        match cache_guard.as_ref() {
            Some(cache) => {
                let types_lock = types::load_types_lock(&root).unwrap_or_default();
                Ok(
                    serde_json::to_value(catalog::build_catalog_response(
                        cache,
                        &types_lock,
                        &root,
                    ))
                    .unwrap_or(serde_json::Value::Null),
                )
            }
            None => {
                let error = self.last_build_error.read().await.clone();
                Ok(
                    serde_json::to_value(catalog::build_error_response(error.as_deref()))
                        .unwrap_or(serde_json::Value::Null),
                )
            }
        }
    }

    /// Rebuild the project model and types cache from disk.
    ///
    /// Delegates to [`compute_diagnostic_actions`] for the pure diagnostic
    /// diffing logic, then applies the resulting actions via the LSP client.
    /// Opens the [`ProjectCache`] lazily on the first successful build.
    ///
    /// Returns the compiled project on success for use by [`run_typecheck`].
    /// The project is not stored on `self` — it is only needed transiently
    /// for typecheck planning.
    async fn rebuild_project(&self) -> Option<Arc<graph::Project>> {
        self.load_settings().await;
        let root = self.root.read().await.clone();
        let (profile, profile_suffix, variables) = {
            let settings_guard = self.settings.read().await;
            match settings_guard.as_ref() {
                Some(ps) => {
                    let profile = resolve_lsp_profile_name(&root);
                    let config = ps.config_for_profile(&profile);
                    (
                        profile,
                        config.profile_suffix.clone(),
                        config.variables.clone(),
                    )
                }
                None => ("default".to_string(), None, BTreeMap::new()),
            }
        };

        let build_result =
            project::plan_sync(&root, &profile, profile_suffix.as_deref(), &variables);

        // Extract validation diagnostics from the build result (pure).
        let new_diagnostics = match &build_result {
            Err(ProjectError::Validation(ValidationErrors { errors })) => {
                diagnostics::validation_diagnostics(errors)
            }
            _ => BTreeMap::new(),
        };

        // Compute diagnostic actions (pure).
        let old_uris = self.project_diagnostic_uris.lock().await.clone();
        let actions = compute_diagnostic_actions(new_diagnostics, &old_uris);

        // Return the project on success, log and record error on failure.
        let project = match build_result {
            Ok(p) => {
                *self.last_build_error.write().await = None;
                Some(Arc::new(p))
            }
            Err(ref e) => {
                self.client
                    .log_message(MessageType::ERROR, format!("Project build failed: {e}"))
                    .await;
                *self.last_build_error.write().await = Some(format!("{e}"));
                None
            }
        };

        // Apply diagnostic actions (I/O).
        for uri in &actions.uris_to_clear {
            self.client
                .publish_diagnostics(uri.clone(), Vec::new(), None)
                .await;
        }
        for (uri, diags) in actions.diagnostics_to_publish {
            self.client.publish_diagnostics(uri, diags, None).await;
        }
        *self.project_diagnostic_uris.lock().await = actions.new_tracked_uris;

        // Open the long-lived ProjectCache SQLite connection on first successful build.
        if project.is_some() {
            let mut guard = self.project_cache.lock().await;
            if guard.is_none() {
                *guard =
                    try_open_project_cache(&root, &profile, profile_suffix.as_deref(), &variables);
            }
        }

        // Notify the client that the project has been rebuilt so it can
        // refresh catalog/DAG data.
        self.client.send_notification::<ProjectRebuilt>(()).await;

        project
    }

    /// Run compiler-owned incremental typechecking.
    ///
    /// The compiler compares the current compiled objects to persisted
    /// typecheck artifacts, validates only the dirty runtime frontier, and
    /// lazily stages both internal and external dependencies as temp tables.
    /// On success, it writes the refreshed internal types cache, then reloads
    /// those types so the extension can refresh column data in the catalog.
    ///
    /// Silently returns on any failure — the  catalog simply won't have updated
    /// column data until the next successful typecheck.
    async fn run_typecheck(&self, project: Arc<graph::Project>) {
        let root = self.root.read().await.clone();

        let (profile, profile_suffix, variables) = {
            let settings_guard = self.settings.read().await;
            match settings_guard.as_ref() {
                Some(ps) => {
                    let profile = resolve_lsp_profile_name(&root);
                    let config = ps.config_for_profile(&profile);
                    (
                        profile,
                        config.profile_suffix.clone(),
                        config.variables.clone(),
                    )
                }
                None => ("default".to_string(), None, BTreeMap::new()),
            }
        };

        let types_lock = types::load_types_lock(&root).unwrap_or_default();
        let plan = match project::compiler::typecheck::plan(
            &root,
            &profile,
            profile_suffix.as_deref(),
            &variables,
            &project,
            types_lock,
        ) {
            Ok(p) => p,
            Err(_) => return,
        };

        if plan.is_up_to_date() {
            return;
        }

        let docker_image = self
            .settings
            .read()
            .await
            .as_ref()
            .map(|s| s.docker_image())
            .unwrap_or_else(default_docker_image);

        let _ = project::compiler::typecheck::execute(
            &project,
            &root,
            &profile,
            profile_suffix.as_deref(),
            &variables,
            Some(&docker_image),
            plan,
        )
        .await;

        // Notify the client so it can refresh column data from the existing
        // ProjectCache connection (which sees new data on the next query).
        self.client.send_notification::<ProjectRebuilt>(()).await;
    }
}

#[tower_lsp::async_trait]
impl LanguageServer for Backend {
    async fn initialize(&self, params: InitializeParams) -> Result<InitializeResult> {
        if let Some(root_uri) = params.root_uri {
            if let Ok(path) = root_uri.to_file_path() {
                let mut root = self.root.write().await;
                *root = path;
            }
        }

        Ok(InitializeResult {
            capabilities: ServerCapabilities {
                text_document_sync: Some(TextDocumentSyncCapability::Options(
                    TextDocumentSyncOptions {
                        open_close: Some(true),
                        change: Some(TextDocumentSyncKind::FULL),
                        save: Some(TextDocumentSyncSaveOptions::SaveOptions(SaveOptions {
                            include_text: Some(false),
                        })),
                        ..Default::default()
                    },
                )),
                completion_provider: Some(CompletionOptions::default()),
                definition_provider: Some(OneOf::Left(true)),
                references_provider: Some(OneOf::Left(true)),
                document_symbol_provider: Some(OneOf::Left(true)),
                workspace_symbol_provider: Some(OneOf::Left(true)),
                hover_provider: Some(HoverProviderCapability::Simple(true)),
                code_lens_provider: Some(CodeLensOptions {
                    resolve_provider: Some(false),
                }),
                semantic_tokens_provider: Some(
                    SemanticTokensServerCapabilities::SemanticTokensOptions(
                        SemanticTokensOptions {
                            legend: SemanticTokensLegend {
                                token_types: semantic_tokens::legend_token_types(),
                                token_modifiers: vec![],
                            },
                            full: Some(SemanticTokensFullOptions::Bool(true)),
                            ..Default::default()
                        },
                    ),
                ),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    async fn initialized(&self, _: InitializedParams) {
        self.load_settings().await;
        let project = self.rebuild_project().await;
        if let Some(project) = project {
            self.run_typecheck(project).await;
        }
    }

    async fn shutdown(&self) -> Result<()> {
        Ok(())
    }

    async fn did_open(&self, params: DidOpenTextDocumentParams) {
        self.publish_diagnostics(params.text_document.uri, &params.text_document.text)
            .await;
    }

    async fn did_change(&self, params: DidChangeTextDocumentParams) {
        if let Some(change) = params.content_changes.into_iter().last() {
            self.publish_diagnostics(params.text_document.uri, &change.text)
                .await;
        }
    }

    async fn did_save(&self, _params: DidSaveTextDocumentParams) {
        let project = self.rebuild_project().await;
        if let Some(project) = project {
            self.run_typecheck(project).await;
        }
    }

    async fn did_change_watched_files(&self, _params: DidChangeWatchedFilesParams) {
        let project = self.rebuild_project().await;
        if let Some(project) = project {
            self.run_typecheck(project).await;
        }
    }

    async fn goto_definition(
        &self,
        params: GotoDefinitionParams,
    ) -> Result<Option<GotoDefinitionResponse>> {
        let uri = params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;

        let (_, _, parts) = match self.snapshot_at_position(&uri, position).await {
            Some(s) => s,
            None => return Ok(None),
        };
        let parts = match parts {
            Some(p) => p,
            None => return Ok(None),
        };

        let root = self.root.read().await.clone();
        let cache_guard = self.project_cache.lock().await;
        let cache = match cache_guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        let location = goto_definition::resolve_reference(&parts, &uri, &root, cache);
        Ok(location.map(GotoDefinitionResponse::Scalar))
    }

    async fn references(&self, params: ReferenceParams) -> Result<Option<Vec<Location>>> {
        let uri = params.text_document_position.text_document.uri;
        let position = params.text_document_position.position;

        let (_, _, parts) = match self.snapshot_at_position(&uri, position).await {
            Some(s) => s,
            None => return Ok(None),
        };
        let parts = match parts {
            Some(p) => p,
            None => return Ok(None),
        };

        let root = self.root.read().await.clone();
        let cache_guard = self.project_cache.lock().await;
        let cache = match cache_guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        let locations = references::find_references(
            &parts,
            &uri,
            &root,
            cache,
            params.context.include_declaration,
        );
        if locations.is_empty() {
            Ok(None)
        } else {
            Ok(Some(locations))
        }
    }

    async fn document_symbol(
        &self,
        params: DocumentSymbolParams,
    ) -> Result<Option<DocumentSymbolResponse>> {
        let file_uri = params.text_document.uri;
        let root = self.root.read().await.clone();

        let cache_guard = self.project_cache.lock().await;
        let cache = match cache_guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        let symbols = document_symbol::document_symbols(&file_uri, &root, cache);
        if symbols.is_empty() {
            Ok(None)
        } else {
            Ok(Some(DocumentSymbolResponse::Nested(symbols)))
        }
    }

    async fn symbol(
        &self,
        params: WorkspaceSymbolParams,
    ) -> Result<Option<Vec<SymbolInformation>>> {
        let root = self.root.read().await.clone();
        let cache_guard = self.project_cache.lock().await;
        let cache = match cache_guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        let symbols = workspace_symbol::workspace_symbols(&params.query, cache, &root);
        if symbols.is_empty() {
            Ok(None)
        } else {
            Ok(Some(symbols))
        }
    }

    async fn hover(&self, params: HoverParams) -> Result<Option<Hover>> {
        let uri = params.text_document_position_params.text_document.uri;
        let position = params.text_document_position_params.position;

        let (text, byte_offset, parts) = match self.snapshot_at_position(&uri, position).await {
            Some(s) => s,
            None => return Ok(None),
        };

        // Try variable hover first (pure).
        let variables = self.variables.read().await;
        let profile = self.profile_name.read().await;
        if let Some(h) = hover::resolve_variable_hover(&text, byte_offset, &variables, &profile) {
            return Ok(Some(h));
        }
        drop(variables);
        drop(profile);

        // Then object hover (pure).
        let parts = match parts {
            Some(p) => p,
            None => return Ok(None),
        };

        let root = self.root.read().await.clone();
        let cache_guard = self.project_cache.lock().await;
        let cache = match cache_guard.as_ref() {
            Some(c) => c,
            None => return Ok(None),
        };

        let types_lock = types::load_types_lock(&root).unwrap_or_default();

        Ok(hover::resolve_hover(
            &parts,
            &uri,
            &root,
            cache,
            &types_lock,
        ))
    }

    async fn completion(&self, params: CompletionParams) -> Result<Option<CompletionResponse>> {
        let file_uri = params.text_document_position.text_document.uri;
        let position = params.text_document_position.position;
        let root = self.root.read().await.clone();

        let doc_text = {
            let docs = self.documents.lock().await;
            docs.get(&file_uri).map(|rope| rope.to_string())
        };
        let text = doc_text.as_deref().unwrap_or("");
        let prefix = completion::prefix_context(text, position);

        let cache_guard = self.project_cache.lock().await;
        let types_lock = types::load_types_lock(&root).unwrap_or_default();
        let items =
            completion::complete(cache_guard.as_ref(), &types_lock, &file_uri, &root, &prefix);

        Ok(Some(CompletionResponse::Array(items)))
    }

    async fn code_lens(&self, params: CodeLensParams) -> Result<Option<Vec<CodeLens>>> {
        let file_uri = params.text_document.uri;
        let root = self.root.read().await.clone();

        let doc_text = {
            let docs = self.documents.lock().await;
            docs.get(&file_uri).map(|rope| rope.to_string())
        };
        let text = match doc_text.as_deref() {
            Some(t) => t,
            None => return Ok(None),
        };

        let cache_guard = self.project_cache.lock().await;
        let lenses = code_lens::code_lenses(&file_uri, text, &root, cache_guard.as_ref());
        Ok(Some(lenses))
    }

    async fn semantic_tokens_full(
        &self,
        params: SemanticTokensParams,
    ) -> Result<Option<SemanticTokensResult>> {
        let file_uri = params.text_document.uri;

        let doc_text = {
            let docs = self.documents.lock().await;
            docs.get(&file_uri).map(|rope| rope.to_string())
        };
        let text = match doc_text.as_deref() {
            Some(t) => t,
            None => return Ok(None),
        };

        let data = semantic_tokens::compute_semantic_tokens(text);
        Ok(Some(SemanticTokensResult::Tokens(SemanticTokens {
            result_id: None,
            data,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file_url(path: &str) -> Url {
        Url::from_file_path(path).unwrap()
    }

    fn make_diagnostic(msg: &str) -> Diagnostic {
        Diagnostic {
            message: msg.to_string(),
            severity: Some(DiagnosticSeverity::ERROR),
            source: Some("mz-deploy".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn diagnostic_actions_success_clears_all() {
        let old = vec![file_url("/a.sql"), file_url("/b.sql")];
        let actions = compute_diagnostic_actions(BTreeMap::new(), &old);

        assert!(actions.diagnostics_to_publish.is_empty());
        assert_eq!(actions.uris_to_clear.len(), 2);
        assert!(actions.new_tracked_uris.is_empty());
    }

    #[test]
    fn diagnostic_actions_validation_errors() {
        let old = vec![file_url("/a.sql"), file_url("/b.sql")];
        let mut new_diags = BTreeMap::new();
        new_diags.insert(PathBuf::from("/b.sql"), vec![make_diagnostic("error in b")]);
        new_diags.insert(PathBuf::from("/c.sql"), vec![make_diagnostic("error in c")]);

        let actions = compute_diagnostic_actions(new_diags, &old);

        // /a.sql should be cleared (was in old, not in new).
        assert_eq!(actions.uris_to_clear, vec![file_url("/a.sql")]);
        // /b.sql and /c.sql should be published.
        assert_eq!(actions.diagnostics_to_publish.len(), 2);
        // Tracked URIs should be the new set.
        assert_eq!(actions.new_tracked_uris.len(), 2);
    }

    #[test]
    fn diagnostic_actions_no_previous() {
        let mut new_diags = BTreeMap::new();
        new_diags.insert(PathBuf::from("/a.sql"), vec![make_diagnostic("error")]);

        let actions = compute_diagnostic_actions(new_diags, &[]);

        assert!(actions.uris_to_clear.is_empty());
        assert_eq!(actions.diagnostics_to_publish.len(), 1);
        assert_eq!(actions.new_tracked_uris.len(), 1);
    }

    #[test]
    fn try_open_project_cache_returns_none_for_missing_db() {
        let result = try_open_project_cache(
            Path::new("/nonexistent/path"),
            "default",
            None,
            &BTreeMap::new(),
        );
        assert!(result.is_none());
    }

    /// Regression test for the tokio::sync lock conversion.
    ///
    /// `publish_diagnostics` acquires `documents.lock()` and then holds the
    /// lock across an `.await` on `client.publish_diagnostics(...)`. With the
    /// previous `std::sync::Mutex` a second concurrent call from another task
    /// would deadlock, because the std guard is not `Send` and blocks the
    /// worker thread. With `tokio::sync::Mutex` the second task yields
    /// correctly and both calls complete.
    ///
    /// The multi-thread runtime with 2 workers is required so the two spawned
    /// tasks can actually make progress concurrently.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_publish_diagnostics_do_not_deadlock() {
        use std::sync::Mutex as StdMutex;

        // `LspService::new` hands the init closure a `Client`, but only
        // `inner()` (which returns `&Backend`) is publicly accessible on the
        // service. Capture a clone of the client out of the closure so we can
        // build an independent `Arc<Backend>` that outlives the service
        // reference.
        let captured_client: Arc<StdMutex<Option<Client>>> = Arc::new(StdMutex::new(None));
        let captured_client_clone = Arc::clone(&captured_client);
        let (_service, _socket) = tower_lsp::LspService::new(move |client| {
            *captured_client_clone.lock().unwrap() = Some(client.clone());
            Backend::new_with_root(client, std::env::temp_dir())
        });
        let client = captured_client
            .lock()
            .unwrap()
            .take()
            .expect("init closure ran and captured a Client");

        let backend = Arc::new(Backend::new_with_root(client, std::env::temp_dir()));

        let b1 = Arc::clone(&backend);
        let b2 = Arc::clone(&backend);
        let t1 = mz_ore::task::spawn(|| "lsp-test-publish-a", async move {
            b1.publish_diagnostics(
                Url::from_file_path(std::env::temp_dir().join("a.sql")).unwrap(),
                "SELECT 1;",
            )
            .await;
        });
        let t2 = mz_ore::task::spawn(|| "lsp-test-publish-b", async move {
            b2.publish_diagnostics(
                Url::from_file_path(std::env::temp_dir().join("b.sql")).unwrap(),
                "SELECT 2;",
            )
            .await;
        });
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            let _ = tokio::join!(t1, t2);
        })
        .await;
        assert!(result.is_ok(), "concurrent publish_diagnostics timed out");
    }
}
